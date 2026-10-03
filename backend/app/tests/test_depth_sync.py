from app.services.binance.depth_sync import DepthSync


def test_depth_sync_from_snapshot_then_live() -> None:
    ds = DepthSync()
    ds.buffer_event({"U": 101, "u": 102, "b": [["100", "2"]], "a": []})
    synced = ds.sync_from_snapshot({"lastUpdateId": 100, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    assert synced
    assert ds.synced
    assert ds.book.bids[100.0] == 2.0

    ok = ds.apply_live_event({"pu": 102, "u": 103, "b": [["100", "3"]], "a": []})
    assert ok
    assert ds.book.bids[100.0] == 3.0


def test_depth_sync_rejects_unbridgeable_buffer() -> None:
    ds = DepthSync()
    ds.buffer_event({"U": 200, "u": 201, "b": [["100", "2"]], "a": []})
    synced = ds.sync_from_snapshot({"lastUpdateId": 100, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    assert not synced
    assert not ds.synced
    assert ds.book.bids[100.0] == 1.0
