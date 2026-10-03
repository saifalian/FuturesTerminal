from app.services.aggregation.manager import SourceCoverageManager


def test_source_coverage_resolve_symbol_binance_supported() -> None:
    manager = SourceCoverageManager()
    resolution = manager.resolve_symbol("BTCUSDT", ["BTCUSDT", "ETHUSDT"])
    assert resolution["normalized_symbol"] == "BTCUSDT"
    assert "binance" in resolution["found_exchanges"]
    payload = manager.source_coverage_payload()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["supported_exchanges"] >= 1


def test_source_coverage_mark_event_updates_feature_counts() -> None:
    manager = SourceCoverageManager()
    manager.resolve_symbol("BTCUSDT", ["BTCUSDT"])
    manager.mark_event("binance", "btcusdt@depth@100ms")
    manager.mark_event("binance", "btcusdt@kline_1m")
    payload = manager.source_coverage_payload()
    assert payload["feature_contributors"]["book"] >= 1
    assert payload["feature_contributors"]["candles"] >= 1


def test_source_coverage_disabled_exchange_has_no_contribution() -> None:
    manager = SourceCoverageManager()
    manager.resolve_symbol("BTCUSDT", ["BTCUSDT"])
    manager.set_enabled_exchanges(["bybit"])
    manager.mark_event("binance", "btcusdt@depth@100ms")
    payload = manager.source_coverage_payload()
    binance = next(item for item in payload["exchanges"] if item["exchange_id"] == "binance")
    assert binance["enabled_by_user"] is False
    assert binance["reason"] == "disabled_by_user"
    assert payload["feature_contributors"]["book"] == 0


def test_source_coverage_selection_persists(tmp_path) -> None:
    path = tmp_path / "exchange-selection.json"
    manager = SourceCoverageManager(selection_path=path)
    manager.set_enabled_exchanges(["binance", "bybit"])
    reloaded = SourceCoverageManager(selection_path=path)
    assert set(reloaded.enabled_exchange_ids()) == {"binance", "bybit"}
