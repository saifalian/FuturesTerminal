from __future__ import annotations

from collections import defaultdict

from app.services.market.price_bucketizer import PriceBucketizer


class LadderSnapshotBuilder:
    def __init__(self, bucketizer: PriceBucketizer, rows_count: int = 1200, use_tick_size: bool = False) -> None:
        self.bucketizer = bucketizer
        self.rows_count = rows_count
        self.use_tick_size = use_tick_size

    @property
    def effective_step_size(self) -> float:
        if self.use_tick_size and self.bucketizer.tick_size > 0:
            return self.bucketizer.tick_size
        if self.bucketizer.bucket_size > 0:
            return self.bucketizer.bucket_size
        return 0.0001

    def build(
        self,
        bids: dict[float, float],
        asks: dict[float, float],
        current_price: float,
        ts_ms: int,
    ) -> dict:
        step_size = self.effective_step_size
        center_bucket = int(current_price // step_size)
        half_rows = max(1, self.rows_count // 2)
        row_min = center_bucket - half_rows
        row_max = center_bucket + half_rows

        aggregated: dict[int, float] = defaultdict(float)
        for price, qty in bids.items():
            bucket_index = int(price // step_size)
            if row_min <= bucket_index <= row_max:
                aggregated[bucket_index] += float(qty)

        for price, qty in asks.items():
            bucket_index = int(price // step_size)
            if row_min <= bucket_index <= row_max:
                aggregated[bucket_index] += float(qty)

        rows: list[dict] = []
        half_step = step_size * 0.5
        for bucket_index in range(row_max, row_min - 1, -1):
            price = bucket_index * step_size
            liquidity = float(aggregated.get(bucket_index, 0.0))
            if price > current_price + half_step:
                side = "above"
            elif price < current_price - half_step:
                side = "below"
            else:
                side = "at"
            rows.append(
                {
                    "row": bucket_index,
                    "price": price,
                    "liquidity": liquidity,
                    "side": side,
                }
            )

        return {
            "type": "ladder_snapshot",
            "ts_ms": ts_ms,
            "current_price": current_price,
            "rows": rows,
        }
