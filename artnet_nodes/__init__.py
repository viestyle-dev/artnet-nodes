import logging
from nonoha_core.node_registry import NodeRegistry
from .artnet_nodes import ArtNetDmxSendNode

logger = logging.getLogger(__name__)


def register_nodes(registry: NodeRegistry):
    """Register Art-Net nodes with the engine."""
    logger.info("Registering artnet-nodes plugin")
    registry.register_node("ArtNetDmxSend", ArtNetDmxSendNode)


async def finalize():
    """Optional cleanup function."""
    logger.info("Finalizing artnet-nodes plugin")
