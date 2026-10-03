from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CandleState:
    last: dict | None = None

    def update(self, kline: dict) -> None:
        self.last = kline
