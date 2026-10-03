from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable

import websockets

from app.core.time_utils import utc_now_iso
from app.services.binance.reconnect import ReconnectPolicy
from app.services.binance.streams import build_streams


logger = logging.getLogger(__name__)

EventHandler = Callable[[dict], Awaitable[None]]


class PublicWsClient:
    def __init__(self, ws_base: str, symbol: str, on_event: EventHandler) -> None:
        self.ws_base = ws_base.rstrip("/")
        self.symbol = symbol
        self.on_event = on_event
        self.reconnect = ReconnectPolicy()

    @property
    def url(self) -> str:
        joined = "/".join(build_streams(self.symbol))
        return f"{self.ws_base}/stream?streams={joined}"

    async def run(self, stop_event: asyncio.Event) -> None:
        attempt = 0
        while not stop_event.is_set():
            attempt += 1
            try:
                logger.info("Connecting public WS: %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=25,
                    ping_timeout=None,
                    max_size=5_000_000,
                    close_timeout=5,
                    open_timeout=20,
                ) as ws:
                    attempt = 0
                    async for raw in ws:
                        if stop_event.is_set():
                            return
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        payload = msg.get("data", msg)
                        event = {
                            "stream": stream,
                            "event_type": payload.get("e", stream),
                            "payload": payload,
                            "received_at": utc_now_iso(),
                        }
                        await self.on_event(event)
            except Exception as exc:
                delay = self.reconnect.delay(attempt)
                message = str(exc).lower()
                # Handshake timeouts are often transient network pressure.
                # Use a slightly calmer reconnect floor to avoid tight churn.
                if "timed out during handshake" in message:
                    delay = max(delay, 8.0)
                if isinstance(exc, OSError) and getattr(exc, "errno", None) == 11001:
                    logger.warning(
                        "Public WS DNS resolve failed for %s (errno 11001). Reconnect in %.1fs",
                        self.ws_base,
                        delay,
                    )
                else:
                    logger.warning("Public WS disconnected: %s. Reconnect in %.1fs", exc, delay)
                await asyncio.sleep(delay)
