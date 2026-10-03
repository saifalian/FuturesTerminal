from __future__ import annotations

from collections import deque
from typing import Any


class HeatmapStore:
    def __init__(self, max_columns: int = 14_400) -> None:
        self.max_columns = max_columns
        self._columns: deque[dict[str, Any]] = deque(maxlen=max_columns)

    def append_column(self, column: dict[str, Any]) -> None:
        self._columns.append(column)

    def recent_columns(self, limit: int | None = None) -> list[dict[str, Any]]:
        if limit is None or limit <= 0:
            return list(self._columns)
        return list(self._columns)[-limit:]

    def reset(self) -> None:
        self._columns.clear()

    @property
    def count(self) -> int:
        return len(self._columns)
