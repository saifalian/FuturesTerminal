from app.services.market.ladder_snapshot_builder import LadderSnapshotBuilder
from app.services.market.price_bucketizer import PriceBucketizer


def test_ladder_builder_side_classification() -> None:
    bucketizer = PriceBucketizer(tick_size=0.01, ticks_per_bucket=2)
    builder = LadderSnapshotBuilder(bucketizer=bucketizer, rows_count=6)

    bids = {1.34: 200.0, 1.36: 300.0}
    asks = {1.38: 500.0, 1.4: 100.0}
    snapshot = builder.build(bids=bids, asks=asks, current_price=1.36, ts_ms=1000)

    assert snapshot["type"] == "ladder_snapshot"
    assert len(snapshot["rows"]) == 7
    sides = {round(float(row["price"]), 2): row["side"] for row in snapshot["rows"]}
    assert sides[1.34] == "below"
    assert sides[1.36] == "at"
    assert sides[1.38] == "above"
