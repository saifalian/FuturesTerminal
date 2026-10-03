from __future__ import annotations

from collections import defaultdict
from typing import Any, Awaitable, Callable

Listener = Callable[[dict[str, Any]], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Listener]] = defaultdict(list)

    def subscribe(self, topic: str, listener: Listener) -> None:
        self._listeners[topic].append(listener)

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        for listener in self._listeners.get(topic, []):
            await listener(message)
