import pytest

from app.services.execution.order_router import OrderRouter


class DummyRest:
    async def test_order(self, order: dict) -> dict:
        return {"ok": True, "order": order}


@pytest.mark.asyncio
async def test_send_test_order() -> None:
    router = OrderRouter(DummyRest())
    out = await router.send_test_order({"symbol": "BTCUSDT"})
    assert out["ok"]
