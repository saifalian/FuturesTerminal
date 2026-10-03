from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class MarkState:
    mark_price: float = 0.0
    funding_rate: float = 0.0

    def update(self, payload: dict) -> None:
        if "p" in payload:
            self.mark_price = float(payload["p"])
        if "r" in payload:
            self.funding_rate = float(payload["r"])
