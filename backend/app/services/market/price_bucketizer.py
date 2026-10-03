from __future__ import annotations

import math


class PriceBucketizer:
    def __init__(self, tick_size: float, ticks_per_bucket: int = 2) -> None:
        safe_tick = tick_size if tick_size > 0 else 0.1
        safe_ticks_per_bucket = ticks_per_bucket if ticks_per_bucket > 0 else 1
        self.tick_size = safe_tick
        self.ticks_per_bucket = safe_ticks_per_bucket

    @property
    def bucket_size(self) -> float:
        return self.tick_size * self.ticks_per_bucket

    def update_tick_size(self, tick_size: float) -> None:
        self.tick_size = tick_size if tick_size > 0 else self.tick_size

    def price_to_bucket_index(self, price: float) -> int:
        if self.bucket_size <= 0:
            return 0
        return int(math.floor(price / self.bucket_size))

    def bucket_index_to_price(self, bucket_index: int) -> float:
        return bucket_index * self.bucket_size
