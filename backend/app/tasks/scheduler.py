from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class TaskScheduler:
    def __init__(self) -> None:
        self.tasks: list[asyncio.Task] = []

    def every(self, interval_seconds: int, coro_factory: Callable[[], Awaitable[None]]) -> None:
        async def _loop() -> None:
            while True:
                await coro_factory()
                await asyncio.sleep(interval_seconds)

        self.tasks.append(asyncio.create_task(_loop()))

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
