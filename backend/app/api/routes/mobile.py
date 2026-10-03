from __future__ import annotations

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

router = APIRouter(prefix="/mobile", tags=["mobile"])


class MobilePairSelectRequest(BaseModel):
    pair: str


@router.get("/pairs/search")
async def mobile_pairs_search(request: Request, q: str = Query(default="")) -> dict:
    all_symbols = [str(s).upper() for s in getattr(request.app.state, "all_symbols", [])]
    query = q.strip().upper()
    if query:
        matches = [s for s in all_symbols if query in s]
    else:
        matches = all_symbols
    return {"pairs": matches[:120]}


@router.get("/pairs/recent")
async def mobile_pairs_recent() -> dict:
    return {"pairs": []}


@router.post("/pairs/select")
async def mobile_pairs_select(payload: MobilePairSelectRequest) -> dict:
    return {"ok": True, "pair": payload.pair.upper(), "exchange": "binance"}


@router.get("/terminal/snapshot")
async def mobile_terminal_snapshot(request: Request, pair: str = Query(...)) -> dict:
    manager = request.app.state.tab_sessions
    snap = await manager.latest_pair_snapshot(pair.upper())
    return {"pair": pair.upper(), "snapshot": snap or {}}


@router.get("/terminal/sections")
async def mobile_terminal_sections(pair: str = Query(...)) -> dict:
    sections = [
        "Main Chart",
        "Live Ladder",
        "Live Ladder Depth",
        "Orderbook Dominance",
        "Book/Spread",
        "Candles",
        "Trades",
        "Mark/Funding",
        "Liquidations",
        "Replay Candle Core",
        "Data Quality Score",
        "Weighted Exchange Controls",
        "Liquidity Event Alerts",
        "Signal Confluence Meter",
        "Execution Simulator Panel",
        "Confluence Score (0-100)",
        "Long / Short Checklist",
        "Entry Quality Meter",
        "Regime Detector",
        "Absorption vs Breakout",
        "Spoof Confidence Score",
        "Multi-Timeframe Alignment",
        "Playbook Tags",
        "Model Inputs: Momentum",
        "Model Inputs: Volatility",
        "Model Inputs: Trend",
        "Model Inputs: Orderflow",
        "Model Inputs: Liquidity",
        "Model Inputs: Context",
        "CVD Panel",
        "Liquidity Area",
        "Market Snapshot",
        "Bias Meter",
        "Positioning Panel",
        "Spoofing Panel",
        "Absorption Panel",
        "Safety Panel",
    ]
    return {"pair": pair.upper(), "sections": sections}

