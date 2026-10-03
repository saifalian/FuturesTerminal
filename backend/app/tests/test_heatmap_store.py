from app.services.market.heatmap_store import HeatmapStore


def test_heatmap_store_rolls_over_max_columns() -> None:
    store = HeatmapStore(max_columns=3)
    store.append_column({"ts_ms": 1, "rows": []})
    store.append_column({"ts_ms": 2, "rows": []})
    store.append_column({"ts_ms": 3, "rows": []})
    store.append_column({"ts_ms": 4, "rows": []})

    columns = store.recent_columns()
    assert len(columns) == 3
    assert columns[0]["ts_ms"] == 2
    assert columns[-1]["ts_ms"] == 4
