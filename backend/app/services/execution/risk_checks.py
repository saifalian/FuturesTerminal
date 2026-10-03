from __future__ import annotations



def validate_order(quantity: float, max_position_usdt: float, notional_usdt: float) -> tuple[bool, str]:
    if quantity <= 0:
        return False, "Quantity must be positive"
    if notional_usdt > max_position_usdt:
        return False, "Order exceeds max position limit"
    return True, "ok"
