from __future__ import annotations


def score_liquidity_heatmap(orderbook: dict) -> float:
    return float(len(orderbook.get("bids", [])) + len(orderbook.get("asks", []))) / 100.0
