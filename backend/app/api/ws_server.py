from __future__ import annotations

import asyncio
from fastapi import WebSocket


class BroadcastHub:
    def __init__(self) -> None:
        self._connections_by_tab: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, tab_id: str = "default") -> None:
        await websocket.accept()
        async with self._lock:
            bucket = self._connections_by_tab.setdefault(tab_id, set())
            bucket.add(websocket)

    async def disconnect(self, websocket: WebSocket, tab_id: str | None = None) -> None:
        async with self._lock:
            if tab_id is not None:
                bucket = self._connections_by_tab.get(tab_id)
                if bucket is not None:
                    bucket.discard(websocket)
                    if not bucket:
                        self._connections_by_tab.pop(tab_id, None)
                return
            stale_tabs: list[str] = []
            for key, bucket in self._connections_by_tab.items():
                bucket.discard(websocket)
                if not bucket:
                    stale_tabs.append(key)
            for key in stale_tabs:
                self._connections_by_tab.pop(key, None)

    async def broadcast(self, payload: dict, tab_id: str | None = None) -> None:
        async with self._lock:
            if tab_id is None:
                clients = [ws for bucket in self._connections_by_tab.values() for ws in bucket]
            else:
                clients = list(self._connections_by_tab.get(tab_id, set()))
        stale: list[WebSocket] = []
        for ws in clients:
            try:
                await asyncio.wait_for(ws.send_json(payload), timeout=1.0)
            except Exception:
                stale.append(ws)
        if stale:
            async with self._lock:
                stale_tabs: list[str] = []
                for key, bucket in self._connections_by_tab.items():
                    for ws in stale:
                        bucket.discard(ws)
                    if not bucket:
                        stale_tabs.append(key)
                for key in stale_tabs:
                    self._connections_by_tab.pop(key, None)
