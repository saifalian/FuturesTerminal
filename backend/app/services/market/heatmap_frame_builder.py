from __future__ import annotations

from collections import defaultdict

from app.services.market.price_bucketizer import PriceBucketizer


class HeatmapFrameBuilder:
    def __init__(self, bucketizer: PriceBucketizer, row_window: int = 180) -> None:
        self.bucketizer = bucketizer
        self.row_window = row_window

    def build_column(
        self,
        bids: dict[float, float],
        asks: dict[float, float],
        current_price: float,
        ts_ms: int,
    ) -> dict:
        center_bucket = self.bucketizer.price_to_bucket_index(current_price)
        row_min = center_bucket - self.row_window
        row_max = center_bucket + self.row_window

        aggregated: dict[int, float] = defaultdict(float)
        for price, qty in bids.items():
            bucket_index = self.bucketizer.price_to_bucket_index(price)
            if row_min <= bucket_index <= row_max:
                aggregated[bucket_index] += float(qty)

        for price, qty in asks.items():
            bucket_index = self.bucketizer.price_to_bucket_index(price)
            if row_min <= bucket_index <= row_max:
                aggregated[bucket_index] += float(qty)

        rows = [{"row": row, "value": value} for row, value in sorted(aggregated.items()) if value > 0]
        return {
            "ts_ms": ts_ms,
            "center_row": center_bucket,
            "row_min": row_min,
            "row_max": row_max,
            "rows": rows,
        }
