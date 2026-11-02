# artnet-nodes

A nonoha-engine plugin nodes for Art-Net DMX512 lighting control protocol, providing real-time communication between nonoha-engine and professional lighting equipment via Ethernet networks.

## Overview

This plugin provides specialized nodes for sending and receiving Art-Net messages within the nonoha-engine visual programming environment. It enables integration with stage lighting systems, LED controllers, media servers, and other Art-Net compatible devices for professional lighting control applications.

## Features

### Nodes Provided

- **ArtNetDmxSend**: Sends DMX512 data over Art-Net protocol
  - Configurable IP address, port, subnet, and universe settings
  - Supports full 512-channel DMX universes
  - Real-time streaming with automatic packet sequencing
  - Efficient data transmission with frame rate limiting

## Installation

### From the Repository

```sh
pip install git+https://github.com/viestyle-dev/artnet-nodes.git
```

Requires Python 3.11+. `nonoha-core` is provided by the host runtime (the
nonoha engine), so it is not a runtime dependency of this package.

### As a Dependency in nonoha-engine Workspace

1. Add `artnet-nodes` to your workspace's `pyproject.toml`:

```toml
[project]
dependencies = [
    "artnet-nodes",
    # other dependencies...
]

[tool.uv.sources]
artnet-nodes = { path = "./plugins/artnet-nodes", editable = false }
```

2. Install dependencies using uv:

```bash
uv sync
```

### As a Built-in Plugin

To include artnet-nodes as a built-in plugin in your nonoha-engine instance:

1. Import the plugin in your `src/main.py`:

```python
import artnet_nodes
```

2. Add to RuntimeConfig:

```python
from nonoha_engine.runtime_config import RuntimeConfig

config = RuntimeConfig(
    builtin_plugins=[
        artnet_nodes,
        # other built-in plugins...
    ]
)
```

## Usage

Once installed, the Art-Net nodes will appear in the nonoha-engine node palette. You can:

1. Drag and drop Art-Net nodes into your visual programming workspace
2. Configure node properties (IP address, subnet, universe, DMX channels)
3. Connect data flows between Art-Net nodes and other processing nodes
4. Run the graph to enable real-time DMX lighting control

## Requirements

- Python >= 3.11
- nonoha-core (nonoha-engine core library)
- numpy (array processing)
- Socket library (standard library for network communication)

## Worker Thread Design

ArtNetDmxSendNode uses a dedicated worker thread to transmit DMX packets. The thread maintains a fixed interval (configured via the `fps` property) using drift-compensated `time.sleep()`.

Platform-specific scheduling optimizations are applied:

### Windows: MMCSS

- Registers the worker thread with the MMCSS (Multimedia Class Scheduler Service) "Games" task
- Provides kernel-level scheduling priority boost
- Effect: p99 jitter improved from 133ms to 33.8ms
- Falls back to normal priority if registration fails

#### MMCSS Side Effects

MMCSS has system-wide side effects:

| Side Effect | Details | Mitigation |
|-------------|---------|------------|
| Network receive throttling | ~15MB/s (120Mbps). Limits NDIS DPC processing | Set registry `NetworkThrottlingIndex` to `0xffffffff` (requires reboot) |
| CPU time allocation | Guarantees 20% CPU for non-MMCSS threads (SystemResponsiveness) | Adjustable via registry |
| Thread limit | 32 threads per process | This plugin uses only 1 thread |
| C-state suppression | Suppresses CPU power saving when LatencySensitive is set | "Games" task has this unset by default (no impact) |

In practice, browsers (Edge, Chrome) and the Windows audio engine (audiodg.exe) already use MMCSS, so registering this plugin's worker thread typically introduces no additional side effects.

#### Tips: Checking MMCSS Status

List processes currently using MMCSS:

```cmd
logman query providers "Microsoft-Windows-MMCSS"
```

Check registry settings:

```
HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Multimedia\SystemProfile
```

### Linux / macOS

No additional scheduling optimization is applied. `time.sleep()` on these platforms provides better precision than on Windows, and standard operation is sufficient.

If jitter becomes an issue on Linux, consider using `SCHED_FIFO` (requires root or CAP_SYS_NICE).

## Development

To contribute or modify the plugin:

```bash
uv sync        # also installs the dev dependency group (pytest, nonoha-core)
uv run pytest
```

## Art-Net Protocol Specification

Art-Net™ is a trademark of Artistic Licence Holdings Ltd.
Protocol documentation available at: <https://art-net.org.uk/>

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).
Pull requests require signing the [nonoha CLA](CLA.md) (a one-time
comment on your first PR, shared across the nonoha repositories).

## Security

See [SECURITY.md](SECURITY.md) for how to report vulnerabilities.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
