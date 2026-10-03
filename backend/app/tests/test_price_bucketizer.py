from app.services.market.price_bucketizer import PriceBucketizer


def test_bucketizer_uses_two_ticks() -> None:
    bucketizer = PriceBucketizer(tick_size=0.01, ticks_per_bucket=2)
    assert bucketizer.bucket_size == 0.02
    assert bucketizer.price_to_bucket_index(1.386) == 69
    assert round(bucketizer.bucket_index_to_price(69), 2) == 1.38
