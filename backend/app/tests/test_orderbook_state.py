from app.services.market.orderbook_state import OrderBookState


def test_apply_snapshot_and_top_of_book() -> None:
    ob = OrderBookState()
    ob.apply_snapshot(
        {
            "lastUpdateId": 100,
            "bids": [["100", "1.0"], ["99", "2.0"]],
            "asks": [["101", "1.5"], ["102", "1.2"]],
        }
    )
    bid, ask = ob.top_of_book()
    assert bid == (100.0, 1.0)
    assert ask == (101.0, 1.5)
