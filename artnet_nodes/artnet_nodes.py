"""ArtNet DMX send node with worker-thread-based precise timing.

A dedicated worker thread transmits DMX data over Art-Net (UDP) at a
fixed rate with drift compensation, fully independent of the asyncio
event loop.  This eliminates jitter caused by HTTP/WebSocket processing
on the same loop.

Threading model:
    Event-loop thread          Worker thread
    ──────────────────         ──────────────────
    _on_dmx_in()             _worker_loop():
      snapshot → lock            drift-compensated sleep
    _on_blackout()               lock → snapshot
      event.set/clear            sendto(packet)
    _dispatch_ready(snap)        call_soon_threadsafe(→)
      output dispatch
"""

import asyncio
import ctypes
import logging
import socket
import sys
import threading
import time

import numpy as np

from nonoha_core.node import BaseNode, NodeDoc, replace_node_input_port
from nonoha_core.port import StaticTypeInputPort, StaticTypeOutputPort, PortValue
from nonoha_core.data_type import DataType, PortValueType, PropertyDataType

logger = logging.getLogger(__name__)

# Art-Net ArtDmx packet constants
_ARTDMX_HEADER_SIZE = 18
_HEADER_SEQ_OFFSET = 12


def _elevate_thread_priority(name: str, fps: float) -> tuple | None:
    """Raise the current thread's scheduling priority (platform-specific).

    Returns a handle for cleanup via _revert_thread_priority(), or None.
    Never raises — logs warnings on failure so the worker continues.

    macOS:   Mach THREAD_TIME_CONSTRAINT_POLICY (realtime scheduling).
    Windows: MMCSS "Games" task registration.
    Linux:   No-op.
    """
    if sys.platform == "darwin":
        try:
            libsystem = ctypes.CDLL("/usr/lib/libSystem.B.dylib")

            class _MachTimebaseInfo(ctypes.Structure):
                _fields_ = [
                    ("numer", ctypes.c_uint32),
                    ("denom", ctypes.c_uint32),
                ]

            tb = _MachTimebaseInfo()
            libsystem.mach_timebase_info(ctypes.byref(tb))

            def ns_to_abs(ns: int) -> int:
                return int(ns * tb.denom / tb.numer)

            class _ThreadTimeConstraintPolicy(ctypes.Structure):
                _fields_ = [
                    ("period", ctypes.c_uint32),
                    ("computation", ctypes.c_uint32),
                    ("constraint", ctypes.c_uint32),
                    ("preemptible", ctypes.c_uint32),
                ]

            _THREAD_TIME_CONSTRAINT_POLICY = 2
            _THREAD_TIME_CONSTRAINT_POLICY_COUNT = 4

            libsystem.pthread_mach_thread_np.argtypes = [ctypes.c_void_p]
            libsystem.pthread_mach_thread_np.restype = ctypes.c_uint32
            libsystem.pthread_self.restype = ctypes.c_void_p
            libsystem.thread_policy_set.argtypes = [
                ctypes.c_uint32,
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.c_uint32,
            ]
            libsystem.thread_policy_set.restype = ctypes.c_int

            period_ns = int(1_000_000_000 / fps)
            policy = _ThreadTimeConstraintPolicy()
            policy.period = ns_to_abs(period_ns)
            policy.computation = ns_to_abs(5_000_000)
            policy.constraint = ns_to_abs(10_000_000)
            policy.preemptible = 1

            mach_thread = libsystem.pthread_mach_thread_np(
                libsystem.pthread_self()
            )
            ret = libsystem.thread_policy_set(
                mach_thread,
                _THREAD_TIME_CONSTRAINT_POLICY,
                ctypes.byref(policy),
                _THREAD_TIME_CONSTRAINT_POLICY_COUNT,
            )
            if ret == 0:
                logger.info(
                    f"{name}: Mach realtime thread policy set "
                    f"(period={period_ns / 1e6:.1f}ms)"
                )
                return ("darwin", libsystem, mach_thread)
            else:
                logger.warning(
                    f"{name}: Mach realtime policy failed (ret={ret})"
                )
        except Exception as e:
            logger.warning(f"{name}: macOS realtime setup failed: {e}")
        return None

    if sys.platform == "win32":
        try:
            avrt = ctypes.windll.avrt  # type: ignore[attr-defined]
            task_index = ctypes.c_ulong(0)
            handle = avrt.AvSetMmThreadCharacteristicsW(
                "Games", ctypes.byref(task_index)
            )
            if handle:
                logger.info(f"{name}: MMCSS 'Games' registered")
                return ("win32", handle)
        except Exception:
            pass
        return None

    return None


def _revert_thread_priority(handle: tuple | None) -> None:
    """Undo _elevate_thread_priority. Safe to call with None."""
    if handle is None:
        return
    platform = handle[0]
    if platform == "darwin":
        try:
            _, libsystem, mach_thread = handle

            class _ThreadStandardPolicy(ctypes.Structure):
                _fields_ = [("no_data", ctypes.c_uint32)]

            std_policy = _ThreadStandardPolicy()
            libsystem.thread_policy_set(
                mach_thread, 1, ctypes.byref(std_policy), 0
            )
        except Exception:
            pass
    elif platform == "win32":
        try:
            ctypes.windll.avrt.AvRevertMmThreadCharacteristics(  # type: ignore[attr-defined]
                handle[1]
            )
        except Exception:
            pass


def _build_artdmx_header(universe: int, packet_size: int) -> bytearray:
    """Build an 18-byte ArtDmx packet header.

    Field layout per the Art-Net specification (ArtDmx packet); the universe
    is encoded as a 15-bit port-address split into low and high bytes.
    """
    header = bytearray(_ARTDMX_HEADER_SIZE)
    header[0:8] = b'Art-Net\x00'            # ID
    header[8:10] = b'\x00\x50'              # OpCode ArtDmx (little-endian)
    header[10:12] = b'\x00\x0e'             # Protocol version 14 (big-endian)
    # header[12] = 0                        # Sequence (set per-send)
    # header[13] = 0                        # Physical
    header[14] = universe & 0xFF            # Universe low byte
    header[15] = (universe >> 8) & 0xFF     # Universe high byte
    header[16] = (packet_size >> 8) & 0xFF  # Length MSB
    header[17] = packet_size & 0xFF         # Length LSB
    return header


class ArtNetDmxSendNode(BaseNode):
    """Send DMX data over Art-Net with precise worker-thread timing.

    A worker thread handles all UDP transmission at the configured rate,
    using drift-compensated ``time.sleep`` (high-resolution on Python 3.11+).
    The ``ready`` output fires after each send, triggering upstream pipeline
    computation for the next frame.

    Input ports:
        dmx_in   - DMX byte arrays per universe (u8[packet_size] × universe_count)
        blackout - Force all channels to 0 when True (bool)

    Output ports:
        ready    - Fires True after each transmission (bool)
        dmx_out  - Transmitted DMX data snapshot (u8[packet_size] × universe_count)
    """

    PROPERTIES = {
        "ip": {
            "description": "Target device IP address for Art-Net UDP packets",
            "data_type": "str",
            "default_value": "127.0.0.1",
        },
        "port": {
            "description": "UDP port number (Art-Net default: 6454)",
            "data_type": "i32",
            "default_value": 6454,
        },
        "universe_count": {
            "description": "Number of DMX512 universes to send",
            "data_type": "i32",
            "default_value": 2,
        },
        "packet_size": {
            "description": "DMX channel count per universe (2-512, must be even)",
            "data_type": "i32",
            "default_value": 512,
        },
        "fps": {
            "description": (
                "Transmission rate in Hz. "
                "Worker thread sends at this fixed interval with drift compensation"
            ),
            "data_type": "f64",
            "default_value": 30.0,
        },
        "src_port": {
            "description": (
                "Source UDP port to bind to. 0 = let OS pick an ephemeral port "
                "(default behavior, no bind). Set to a fixed value (e.g. 6454) "
                "if the receiver filters by source port or you need a stable "
                "source for firewall / NAT rules."
            ),
            "data_type": "i32",
            "default_value": 0,
            "min_value": 0,
            "max_value": 65535,
        },
    }

    @classmethod
    def describe(cls, properties=None):
        return NodeDoc(
            description=(
                "Send DMX data over Art-Net (UDP) with precise timing. "
                "A dedicated worker thread transmits at the configured fps rate "
                "with drift compensation. The 'ready' output fires after each "
                "transmission to trigger upstream pipeline computation for the "
                "next frame."
            ),
            category="ArtNet",
            inputs={
                "dmx_in": (
                    "DMX byte arrays per universe "
                    "(u8[packet_size] × universe_count). "
                    "Snapshot is taken on each dispatch; the worker thread "
                    "transmits the latest snapshot at the next scheduled interval."
                ),
                "blackout": (
                    "When True, all channels are forced to 0 regardless of "
                    "input data. When False, normal transmission resumes."
                ),
            },
            outputs={
                "ready": (
                    "Fires True after each successful transmission. "
                    "Connect to upstream pipeline to trigger next-frame "
                    "computation."
                ),
                "dmx_out": (
                    "DMX byte arrays actually transmitted "
                    "(u8[packet_size] × universe_count). "
                    "Reflects blackout state."
                ),
            },
        )

    def __init__(self, name: str):
        super().__init__(name, "ArtNetDmxSend")

        # Properties → instance vars
        self._ip = str(self.properties.get("ip", "127.0.0.1"))
        self._port = int(self.properties.get("port", 6454))
        self._universe_count = int(self.properties.get("universe_count", 2))
        self._packet_size = int(self.properties.get("packet_size", 512))
        self._fps = float(self.properties.get("fps", 30.0))
        self._src_port = int(self.properties.get("src_port", 0))

        # Input: dmx_in (u8[packet_size] × universe_count)
        self._input_dmx_in = StaticTypeInputPort(
            "dmx_in",
            [
                PortValueType(DataType.Uint8, (self._packet_size,))
                for _ in range(self._universe_count)
            ],
        )
        self._input_dmx_in.subscribe(id(self), self._on_dmx_in)
        self.inputs[self._input_dmx_in.name] = self._input_dmx_in

        # Input: blackout (bool)
        self._input_blackout = StaticTypeInputPort(
            "blackout", [PortValueType(DataType.Bool)]
        )
        self._input_blackout.subscribe(id(self), self._on_blackout)
        self.inputs[self._input_blackout.name] = self._input_blackout

        # Output: ready (bool)
        self._output_ready = StaticTypeOutputPort(
            "ready", [PortValueType(DataType.Bool)]
        )
        self.outputs[self._output_ready.name] = self._output_ready
        self._ready_value = np.array([True], dtype=np.bool_)

        # Output: dmx_out (u8[packet_size] × universe_count)
        self._output_dmx = StaticTypeOutputPort(
            "dmx_out",
            [
                PortValueType(DataType.Uint8, (self._packet_size,))
                for _ in range(self._universe_count)
            ],
        )
        self.outputs[self._output_dmx.name] = self._output_dmx

        # Shared state: snapshot handoff between event-loop and worker
        self._latest_snapshot: list[np.ndarray] | None = None
        self._buffer_lock = threading.Lock()
        self._blackout_event = threading.Event()
        self._shutdown_event = threading.Event()

        # Worker lifecycle
        self._worker_thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # ── Lifecycle ──

    def activate(self):
        self._loop = asyncio.get_running_loop()
        self._start_worker()

    def _start_worker(self):
        self._shutdown_event.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            args=(
                self._ip,
                self._port,
                self._fps,
                self._universe_count,
                self._packet_size,
                self._src_port,
            ),
            daemon=False,
            name=f"{self.name}-worker",
        )
        self._worker_thread.start()

    def _stop_worker(self):
        self._shutdown_event.set()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=2.0)

    def finalize(self):
        self._stop_worker()
        self._worker_thread = None
        self._loop = None

    # ── Input callbacks (event-loop thread) ──

    def _on_dmx_in(self, values: list[PortValue]):
        """Create a snapshot from incoming DMX data for the worker thread."""
        snapshot = [v[-1:].copy() for v in values]
        with self._buffer_lock:
            self._latest_snapshot = snapshot

    def _on_blackout(self, values: list[PortValue]):
        arr: np.ndarray = values[0]
        if arr[-1]:
            self._blackout_event.set()
        else:
            self._blackout_event.clear()

    def _dispatch_ready(self, dmx_out_arrays: list[np.ndarray]):
        """Called on event-loop thread via call_soon_threadsafe."""
        if not self.is_active:
            return
        self._output_dmx.dispatch(dmx_out_arrays)
        self._output_ready.dispatch([self._ready_value])
        if self.parent_graph is not None:
            self.parent_graph.flush_outputs()

    # ── Worker thread ──

    def _worker_loop(
        self,
        ip: str,
        port: int,
        fps: float,
        universe_count: int,
        packet_size: int,
        src_port: int,
    ):
        """Drift-compensated fixed-interval send loop.

        All mutable state is local or passed as arguments. The worker
        creates its own UDP socket, packet templates, and output arrays.
        This ensures that even if _stop_worker times out, the worker
        operates on its own consistent snapshot without races.

        If src_port > 0, the socket is bound to that source UDP port before
        the send loop starts. Bind failure logs an error and aborts the
        worker (no ArtNet transmission) so that configuration errors surface
        loudly instead of silently falling back to an ephemeral port.
        """
        interval = 1.0 / fps

        # UDP socket (worker-owned)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind to a fixed source port if requested. Done before any heavy
        # setup so we can bail out cleanly on failure.
        if src_port > 0:
            try:
                sock.bind(("", src_port))
                logger.info(f"{self.name}: bound to source port {src_port}")
            except OSError as e:
                logger.error(
                    f"{self.name}: failed to bind source port {src_port}: {e}. "
                    f"Worker will not start."
                )
                sock.close()
                return

        # Packet templates (one per universe)
        packets = []
        for i in range(universe_count):
            header = _build_artdmx_header(i, packet_size)
            packet = bytearray(_ARTDMX_HEADER_SIZE + packet_size)
            packet[:_ARTDMX_HEADER_SIZE] = header
            packets.append(packet)

        logger.info(
            f"{self.name}: ArtNet initialized "
            f"{ip}:{port} "
            f"universes={universe_count} "
            f"packet_size={packet_size} "
            f"fps={fps}"
        )

        # Zero snapshot for blackout / initial state (worker-local, immutable)
        zero_snapshot = [
            np.zeros((1, packet_size), dtype=np.uint8)
            for _ in range(universe_count)
        ]

        next_target = time.perf_counter() + interval
        sequences = [1] * universe_count
        perf_last_send = 0.0

        priority_handle = _elevate_thread_priority(self.name, fps)

        try:
            while not self._shutdown_event.is_set():
                # Sleep with drift compensation
                now = time.perf_counter()
                remaining = next_target - now
                if remaining > 0:
                    time.sleep(remaining)

                send_start = time.perf_counter()

                # Get latest snapshot (lock held only for reference read)
                with self._buffer_lock:
                    snapshot = self._latest_snapshot

                is_blackout = self._blackout_event.is_set()
                if snapshot is None or is_blackout:
                    snapshot = zero_snapshot

                # Build packets and send
                for i in range(universe_count):
                    packets[i][_ARTDMX_HEADER_SIZE:_ARTDMX_HEADER_SIZE + packet_size] = snapshot[i].data
                    packets[i][_HEADER_SEQ_OFFSET] = sequences[i]
                    sequences[i] = sequences[i] % 255 + 1  # 1→255→1 (skip 0)
                    try:
                        sock.sendto(packets[i], (ip, port))
                    except OSError:
                        pass

                # Delay detection
                if perf_last_send > 0:
                    tick_interval = send_start - perf_last_send
                    if tick_interval > interval * 1.2:
                        logger.warning(
                            f"{self.name}: tick delayed "
                            f"{tick_interval * 1000:.1f}ms "
                            f"(expected {interval * 1000:.1f}ms)"
                        )
                perf_last_send = send_start

                # Transfer snapshot ownership to event-loop / downstream nodes
                if self._loop is not None:
                    self._loop.call_soon_threadsafe(
                        self._dispatch_ready, snapshot
                    )

                # Drift compensation
                next_target += interval
                if time.perf_counter() - next_target > interval:
                    next_target = time.perf_counter() + interval
        finally:
            sock.close()
            _revert_thread_priority(priority_handle)

    # ── Properties ──

    def set_properties(self, properties: dict[str, PropertyDataType]):
        ip = str(properties.get("ip", self._ip))
        port = int(properties.get("port", self._port))
        universe_count = int(properties.get("universe_count", self._universe_count))
        packet_size = int(properties.get("packet_size", self._packet_size))
        fps = float(properties.get("fps", self._fps))
        src_port = int(properties.get("src_port", self._src_port))

        if universe_count < 1:
            raise ValueError("universe_count must be at least 1")
        if packet_size < 2 or packet_size > 512:
            raise ValueError("packet_size must be between 2 and 512")
        if fps <= 0:
            raise ValueError("fps must be greater than 0")
        if not (0 <= src_port <= 65535):
            raise ValueError(
                f"src_port must be in [0, 65535], got {src_port}"
            )

        changed = (
            ip != self._ip
            or port != self._port
            or universe_count != self._universe_count
            or packet_size != self._packet_size
            or fps != self._fps
            or src_port != self._src_port
        )

        if not changed:
            return

        need_port_rebuild = (
            universe_count != self._universe_count
            or packet_size != self._packet_size
        )

        was_active = self.is_active
        if was_active:
            self._stop_worker()

        if need_port_rebuild:
            # Reset snapshot (old dimensions are invalid for new worker)
            with self._buffer_lock:
                self._latest_snapshot = None

            new_input = StaticTypeInputPort(
                "dmx_in",
                [
                    PortValueType(DataType.Uint8, (packet_size,))
                    for _ in range(universe_count)
                ],
            )
            replace_node_input_port(
                self, self._input_dmx_in, new_input, self._on_dmx_in
            )
            self._input_dmx_in = new_input

            self._output_dmx = StaticTypeOutputPort(
                "dmx_out",
                [
                    PortValueType(DataType.Uint8, (packet_size,))
                    for _ in range(universe_count)
                ],
            )
            self.outputs["dmx_out"] = self._output_dmx

        self._ip = ip
        self._port = port
        self._universe_count = universe_count
        self._packet_size = packet_size
        self._fps = fps
        self._src_port = src_port

        self.properties["ip"] = self._ip
        self.properties["port"] = self._port
        self.properties["universe_count"] = self._universe_count
        self.properties["packet_size"] = self._packet_size
        self.properties["fps"] = self._fps
        self.properties["src_port"] = self._src_port

        if self.parent_graph is not None:
            self.parent_graph.notify_node_updated(self)

        if was_active:
            self._start_worker()
