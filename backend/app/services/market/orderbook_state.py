from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class OrderBookState:
    last_update_id: int = 0
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    def apply_snapshot(self, snapshot: dict) -> None:
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.bids = {float(p): float(q) for p, q in snapshot.get("bids", []) if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snapshot.get("asks", []) if float(q) > 0}

    def apply_diff(self, bids: list[list[str]], asks: list[list[str]], final_update_id: int) -> None:
        for price_str, qty_str in bids:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price_str, qty_str in asks:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = int(final_update_id)

    def top_of_book(self) -> tuple[tuple[float, float] | None, tuple[float, float] | None]:
        best_bid = max(self.bids.items(), key=lambda x: x[0]) if self.bids else None
        best_ask = min(self.asks.items(), key=lambda x: x[0]) if self.asks else None
        return best_bid, best_ask
