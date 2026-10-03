from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.services.execution.risk_checks import validate_order

router = APIRouter(prefix="/trading", tags=["trading"])


class TestOrderRequest(BaseModel):
    symbol: str
    side: str
    order_type: str
    quantity: float


class SubmitOrderRequest(BaseModel):
    symbol: str = Field(min_length=3, max_length=20)
    side: str = Field(min_length=3, max_length=5)
    order_type: str = Field(min_length=4, max_length=12)
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)
    reduce_only: bool = False


class CancelOrderRequest(BaseModel):
    order_id: str = Field(min_length=6, max_length=128)


def _store(request: Request) -> dict[str, dict]:
    if not hasattr(request.app.state, "paper_orders"):
        request.app.state.paper_orders = {}
    return request.app.state.paper_orders


def _normalize_side(value: str) -> str:
    side = value.upper().strip()
    if side not in {"BUY", "SELL"}:
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")
    return side


def _normalize_order_type(value: str) -> str:
    order_type = value.upper().strip()
    if order_type not in {"MARKET", "LIMIT"}:
        raise HTTPException(status_code=400, detail="order_type must be MARKET or LIMIT")
    return order_type


@router.post("/test-order")
async def test_order(req: TestOrderRequest) -> dict:
    return {
        "accepted": True,
        "message": "Execution module is scaffolded. Wire to Binance test endpoint in Phase 5.",
        "request": req.model_dump(),
    }


@router.post("/order")
async def submit_order(request: Request, payload: SubmitOrderRequest) -> dict:
    side = _normalize_side(payload.side)
    order_type = _normalize_order_type(payload.order_type)

    price = payload.price if payload.price is not None else 1.0
    notional_usdt = payload.quantity * price
    ok, reason = validate_order(quantity=payload.quantity, max_position_usdt=1_000_000, notional_usdt=notional_usdt)
    if not ok:
        raise HTTPException(status_code=400, detail=reason)

    order_id = f"ord_{uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    order = {
        "order_id": order_id,
        "symbol": payload.symbol.upper(),
        "side": side,
        "order_type": order_type,
        "quantity": payload.quantity,
        "price": payload.price,
        "reduce_only": payload.reduce_only,
        "status": "open" if order_type == "LIMIT" else "filled",
        "created_at": now,
        "updated_at": now,
    }

    store = _store(request)
    if order["status"] == "open":
        store[order_id] = order

    return {
        "accepted": True,
        "message": "order accepted",
        "order": order,
    }


@router.get("/open-orders")
async def open_orders(request: Request, symbol: str | None = None) -> dict:
    store = _store(request)
    values = list(store.values())
    if symbol:
        symbol_norm = symbol.upper()
        values = [order for order in values if order.get("symbol") == symbol_norm]
    values.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
    return {"orders": values}


@router.post("/cancel")
async def cancel_order(request: Request, payload: CancelOrderRequest) -> dict:
    store = _store(request)
    order = store.get(payload.order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")

    order["status"] = "canceled"
    order["updated_at"] = datetime.now(timezone.utc).isoformat()
    store.pop(payload.order_id, None)

    return {
        "ok": True,
        "message": "order canceled",
        "order_id": payload.order_id,
    }