from __future__ import annotations

from collections import deque

from app.services.market.orderbook_state import OrderBookState


class DepthSync:
    def __init__(self) -> None:
        self._buffer: deque[dict] = deque()
        self._synced = False
        self.book = OrderBookState()

    def reset(self) -> None:
        self._buffer.clear()
        self._synced = False
        self.book = OrderBookState()

    def buffer_event(self, event: dict) -> None:
        self._buffer.append(event)

    def sync_from_snapshot(self, snapshot: dict) -> bool:
        self.book.apply_snapshot(snapshot)
        last_id = self.book.last_update_id

        while self._buffer and int(self._buffer[0].get("u", 0)) < last_id:
            self._buffer.popleft()

        bridge_index: int | None = None
        buffered = list(self._buffer)
        for index, event in enumerate(buffered):
            first_id = int(event.get("U", 0))
            final_id = int(event.get("u", 0))
            if first_id <= self.book.last_update_id + 1 <= final_id:
                bridge_index = index
                break

        if bridge_index is None:
            self._synced = False
            return False

        applied_final = self.book.last_update_id
        for event in buffered[bridge_index:]:
            first_id = int(event.get("U", 0))
            final_id = int(event.get("u", 0))
            prev_final = int(event.get("pu", 0))

            if final_id <= applied_final:
                continue
            if first_id > applied_final + 1:
                self._synced = False
                return False
            if prev_final and prev_final != applied_final:
                # Some streams can surface a transient pu mismatch while still
                # carrying a valid bridging [U, u] range for our local id.
                if not (first_id <= applied_final + 1 <= final_id):
                    self._synced = False
                    return False

            self.book.apply_diff(event.get("b", []), event.get("a", []), final_id)
            applied_final = final_id

        self._buffer.clear()
        self._synced = True
        return True

    def force_sync_from_snapshot(self, snapshot: dict) -> None:
        self.book.apply_snapshot(snapshot)
        self._buffer.clear()
        self._synced = True

    def apply_live_event(self, event: dict) -> bool:
        if not self._synced:
            self.buffer_event(event)
            return False

        final_id = int(event.get("u", 0))
        if final_id <= self.book.last_update_id:
            return True

        # Stability-first live mode: once synced, apply forward diffs directly.
        # This avoids frequent resync loops on transient sequence irregularities.
        self.book.apply_diff(event.get("b", []), event.get("a", []), final_id)
        return True

    @property
    def synced(self) -> bool:
        return self._synced
