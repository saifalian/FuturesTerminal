from __future__ import annotations

from dataclasses import dataclass

from app.services.market.candle_state import CandleState
from app.services.market.liquidation_state import LiquidationState
from app.services.market.mark_state import MarkState
from app.services.market.oi_state import OIState
from app.services.market.orderbook_state import OrderBookState
from app.services.market.trades_state import TradesState


@dataclass(slots=True)
class SessionState:
    symbol: str
    orderbook: OrderBookState
    trades: TradesState
    candle: CandleState
    mark: MarkState
    liquidations: LiquidationState
    oi: OIState
