import asyncio

import pytest


@pytest.fixture
def event_loop():
    """Provide a running event loop for tests that need activate()."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
