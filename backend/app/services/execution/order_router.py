from __future__ import annotations

from app.services.binance.rest_client import BinanceRestClient


class OrderRouter:
    def __init__(self, rest: BinanceRestClient) -> None:
        self.rest = rest

    async def send_test_order(self, order: dict) -> dict:
        return await self.rest.test_order(order)

    async def send_order(self, order: dict) -> dict:
        return await self.rest.new_order(order)
