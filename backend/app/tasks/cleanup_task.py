from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def cleanup_old_files() -> None:
    logger.debug("Cleanup placeholder")
