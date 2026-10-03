from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def run_keepalive() -> None:
    logger.info("Keepalive scaffold ready. Wire listen-key keepalive in Phase 6.")
