# Security Policy

## Reporting a Vulnerability

Please use GitHub's **Private Vulnerability Reporting**
(Security tab → "Report a vulnerability") on this repository.
Do not open public issues for security reports.

## Scope

artnet-nodes drives lighting equipment: `ArtNetDmxSend` transmits ArtDmx
packets to the IP address, port and universe configured in the graph.
Transmission starts as soon as the node is activated — all-zero frames are
sent at the configured frame rate until data arrives, which blacks out the
fixtures on that universe immediately. Packets are sent by unicast only.
A non-zero `src_port` binds a UDP socket on all interfaces. The worker
thread raises its own scheduling priority (MMCSS on Windows, a realtime
thread policy on macOS). The plugin does not execute code from the
network or from graph definitions. Load graphs only from sources you
trust, since a graph can control physical equipment.

The host runtime it plugs into (the nonoha engine) may include script
nodes with their own threat model — see the security policy of the host
project for loading untrusted graphs.

## Supported versions

Security fixes target the latest minor release.
