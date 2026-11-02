import asyncio
import time
from unittest.mock import patch

import numpy as np

from nonoha_core.data_type import DataType, PortValueType
from nonoha_core.node import NodeDoc

from artnet_nodes.artnet_nodes import ArtNetDmxSendNode, _build_artdmx_header


def _activate_node(node, loop):
    """Activate node with a real event loop running in background."""
    async def _activate():
        node._do_activate()

    loop.run_until_complete(_activate())


class TestBuildArtdmxHeader:
    """_build_artdmx_header produces correct Art-Net ArtDmx headers."""

    def test_header_size(self):
        header = _build_artdmx_header(0, 512)
        assert len(header) == 18

    def test_art_net_id(self):
        header = _build_artdmx_header(0, 512)
        assert header[0:8] == b'Art-Net\x00'

    def test_opcode_artdmx(self):
        header = _build_artdmx_header(0, 512)
        assert header[8] == 0x00
        assert header[9] == 0x50

    def test_protocol_version(self):
        header = _build_artdmx_header(0, 512)
        assert header[10] == 0x00
        assert header[11] == 14

    def test_universe_low(self):
        header = _build_artdmx_header(3, 512)
        assert header[14] == 3
        assert header[15] == 0

    def test_universe_high(self):
        header = _build_artdmx_header(256, 512)
        assert header[14] == 0
        assert header[15] == 1

    def test_packet_size_encoding(self):
        header = _build_artdmx_header(0, 216)
        assert header[16] == 0   # 216 >> 8
        assert header[17] == 216  # 216 & 0xFF

    def test_packet_size_512(self):
        header = _build_artdmx_header(0, 512)
        assert header[16] == 2   # 512 >> 8
        assert header[17] == 0   # 512 & 0xFF


class TestLifecycle:
    """Node lifecycle: init -> activate -> finalize"""

    def test_init_no_side_effects(self):
        node = ArtNetDmxSendNode("test")
        assert node._worker_thread is None
        assert node._latest_snapshot is None

    @patch('artnet_nodes.artnet_nodes.socket')
    def test_activate_starts_worker(self, mock_socket, event_loop):
        node = ArtNetDmxSendNode("test")
        _activate_node(node, event_loop)
        assert node._worker_thread is not None
        assert node._worker_thread.is_alive()
        node._do_finalize()

    @patch('artnet_nodes.artnet_nodes.socket')
    def test_finalize_stops_worker(self, mock_socket, event_loop):
        node = ArtNetDmxSendNode("test")
        _activate_node(node, event_loop)
        worker = node._worker_thread
        node._do_finalize()
        assert not worker.is_alive()
        assert node._worker_thread is None

    def test_finalize_without_activate(self):
        node = ArtNetDmxSendNode("test")
        node._do_finalize()  # must not raise


class TestBlackout:
    """Blackout input port behavior"""

    def test_blackout_event_initially_clear(self):
        node = ArtNetDmxSendNode("test")
        assert not node._blackout_event.is_set()

    def test_blackout_set_on_true(self):
        node = ArtNetDmxSendNode("test")
        node._on_blackout([np.array([True], dtype=np.bool_)])
        assert node._blackout_event.is_set()

    def test_blackout_clear_on_false(self):
        node = ArtNetDmxSendNode("test")
        node._blackout_event.set()
        node._on_blackout([np.array([False], dtype=np.bool_)])
        assert not node._blackout_event.is_set()


class TestSnapshot:
    """Snapshot-based DMX data flow"""

    def test_snapshot_initially_none(self):
        node = ArtNetDmxSendNode("test")
        assert node._latest_snapshot is None

    def test_on_dmx_in_creates_snapshot(self):
        node = ArtNetDmxSendNode("test")
        values = [
            np.array([[1, 2, 3]], dtype=np.uint8),
            np.array([[4, 5, 6]], dtype=np.uint8),
        ]
        node._on_dmx_in(values)
        assert node._latest_snapshot is not None
        assert len(node._latest_snapshot) == 2
        np.testing.assert_array_equal(node._latest_snapshot[0], [[1, 2, 3]])
        np.testing.assert_array_equal(node._latest_snapshot[1], [[4, 5, 6]])

    def test_snapshot_keeps_batch_dimension(self):
        """Snapshot must preserve the batch dimension for port dispatch."""
        node = ArtNetDmxSendNode("test")
        values = [np.array([[10, 20, 30]], dtype=np.uint8)]
        node._on_dmx_in(values)
        assert node._latest_snapshot[0].ndim == 2
        assert node._latest_snapshot[0].shape == (1, 3)

    @patch('artnet_nodes.artnet_nodes.socket')
    def test_snapshot_dispatch_shape(self, mock_socket, event_loop):
        """Worker dispatch must pass output port shape validation.

        The worker picks up the snapshot, sends it, and dispatches via
        call_soon_threadsafe. The event loop processes _dispatch_ready,
        which triggers the nonoha-core _validate_dispatch_shapes fixture.
        """
        node = ArtNetDmxSendNode("test")
        _activate_node(node, event_loop)

        node._on_dmx_in([
            np.zeros((1, 512), dtype=np.uint8),
            np.zeros((1, 512), dtype=np.uint8),
        ])

        errors = []
        event_loop.set_exception_handler(
            lambda loop, ctx: errors.append(ctx.get("exception", ctx["message"]))
        )

        event_loop.run_until_complete(asyncio.sleep(0.1))

        node._do_finalize()
        assert not errors, f"Dispatch error: {errors}"

    def test_snapshot_is_copy(self):
        """Snapshot must be independent of the original port value."""
        node = ArtNetDmxSendNode("test")
        original = np.array([[10, 20]], dtype=np.uint8)
        node._on_dmx_in([original])
        original[0, 0] = 99
        assert node._latest_snapshot[0][0, 0] == 10  # unaffected


class TestSetProperties:
    """set_properties behavior"""

    def test_before_activate_no_worker(self):
        node = ArtNetDmxSendNode("test")
        node.set_properties({"packet_size": 216})
        assert node._worker_thread is None
        assert node._packet_size == 216

    @patch('artnet_nodes.artnet_nodes.socket')
    def test_same_values_no_change(self, mock_socket, event_loop):
        node = ArtNetDmxSendNode("test")
        _activate_node(node, event_loop)
        old_worker = node._worker_thread

        node.set_properties({
            "ip": node._ip,
            "port": node._port,
            "universe_count": node._universe_count,
            "packet_size": node._packet_size,
            "fps": node._fps,
        })
        assert node._worker_thread is old_worker
        node._do_finalize()

    @patch('artnet_nodes.artnet_nodes.socket')
    def test_changed_values_restarts(self, mock_socket, event_loop):
        node = ArtNetDmxSendNode("test")
        _activate_node(node, event_loop)
        old_worker = node._worker_thread

        node.set_properties({"fps": 60.0})
        assert node._fps == 60.0
        assert node._worker_thread is not old_worker
        assert node._worker_thread.is_alive()
        node._do_finalize()

    def test_dimension_change_resets_snapshot(self):
        node = ArtNetDmxSendNode("test")
        node._latest_snapshot = [np.zeros(512, dtype=np.uint8)]
        node.set_properties({"packet_size": 216})
        assert node._latest_snapshot is None

    def test_validation_universe_count(self):
        node = ArtNetDmxSendNode("test")
        try:
            node.set_properties({"universe_count": 0})
            assert False, "Should raise ValueError"
        except ValueError as e:
            assert "universe_count" in str(e)

    def test_validation_packet_size(self):
        node = ArtNetDmxSendNode("test")
        try:
            node.set_properties({"packet_size": 1})
            assert False, "Should raise ValueError"
        except ValueError as e:
            assert "packet_size" in str(e)

    def test_validation_fps(self):
        node = ArtNetDmxSendNode("test")
        try:
            node.set_properties({"fps": 0})
            assert False, "Should raise ValueError"
        except ValueError as e:
            assert "fps" in str(e)

    def test_src_port_default_is_zero(self):
        node = ArtNetDmxSendNode("test")
        assert node._src_port == 0
        assert node.properties["src_port"] == 0

    def test_set_src_port_updates_attribute_and_properties(self):
        node = ArtNetDmxSendNode("test")
        node.set_properties({"src_port": 6454})
        assert node._src_port == 6454
        assert node.properties["src_port"] == 6454

    def test_src_port_changed_restarts_worker(self, event_loop):
        with patch('artnet_nodes.artnet_nodes.socket'):
            node = ArtNetDmxSendNode("test")
            _activate_node(node, event_loop)
            old_worker = node._worker_thread

            node.set_properties({"src_port": 7000})
            assert node._src_port == 7000
            assert node._worker_thread is not old_worker
            assert node._worker_thread.is_alive()
            node._do_finalize()

    def test_src_port_same_value_no_restart(self, event_loop):
        with patch('artnet_nodes.artnet_nodes.socket'):
            node = ArtNetDmxSendNode("test")
            _activate_node(node, event_loop)
            old_worker = node._worker_thread

            node.set_properties({"src_port": node._src_port})
            assert node._worker_thread is old_worker
            node._do_finalize()

    def test_validation_src_port_negative(self):
        node = ArtNetDmxSendNode("test")
        try:
            node.set_properties({"src_port": -1})
            assert False, "Should raise ValueError"
        except ValueError as e:
            assert "src_port" in str(e)

    def test_validation_src_port_too_large(self):
        node = ArtNetDmxSendNode("test")
        try:
            node.set_properties({"src_port": 65536})
            assert False, "Should raise ValueError"
        except ValueError as e:
            assert "src_port" in str(e)

    def test_src_port_boundary_zero_ok(self):
        node = ArtNetDmxSendNode("test")
        node.set_properties({"src_port": 0})
        assert node._src_port == 0

    def test_src_port_boundary_max_ok(self):
        node = ArtNetDmxSendNode("test")
        node.set_properties({"src_port": 65535})
        assert node._src_port == 65535


class TestDescribe:
    """describe() returns correct NodeDoc"""

    def test_returns_node_doc(self):
        doc = ArtNetDmxSendNode.describe()
        assert isinstance(doc, NodeDoc)
        assert doc.category == "ArtNet"
        assert "dmx_in" in doc.inputs
        assert "blackout" in doc.inputs
        assert "ready" in doc.outputs
        assert "dmx_out" in doc.outputs


class TestPorts:
    """Port registration"""

    def test_has_input_ports(self):
        node = ArtNetDmxSendNode("test")
        assert "dmx_in" in node.inputs
        assert "blackout" in node.inputs

    def test_has_output_ports(self):
        node = ArtNetDmxSendNode("test")
        assert "ready" in node.outputs
        assert "dmx_out" in node.outputs

    def test_properties_defaults(self):
        node = ArtNetDmxSendNode("test")
        assert node.properties["ip"] == "127.0.0.1"
        assert node.properties["port"] == 6454
        assert node.properties["universe_count"] == 2
        assert node.properties["packet_size"] == 512
        assert node.properties["fps"] == 30.0
