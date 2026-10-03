from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlencode

import httpx

from app.services.binance.signer import sign_query


class BinanceRestClient:
    def __init__(self, base_url: str, api_key: str = "", api_secret: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self._client = httpx.AsyncClient(timeout=10)

    async def close(self) -> None:
        await self._client.aclose()

    async def depth_snapshot(self, symbol: str, limit: int = 1000) -> dict[str, Any]:
        r = await self._client.get(f"{self.base_url}/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
        r.raise_for_status()
        return r.json()

    async def exchange_info(self) -> dict[str, Any]:
        r = await self._client.get(f"{self.base_url}/fapi/v1/exchangeInfo")
        r.raise_for_status()
        return r.json()

    async def klines(self, symbol: str, interval: str = "1m", limit: int = 120) -> list[list[Any]]:
        r = await self._client.get(
            f"{self.base_url}/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": max(1, min(1500, int(limit))),
            },
        )
        r.raise_for_status()
        payload = r.json()
        return payload if isinstance(payload, list) else []

    async def premium_index(self, symbol: str) -> dict[str, Any]:
        r = await self._client.get(
            f"{self.base_url}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        r.raise_for_status()
        payload = r.json()
        return payload if isinstance(payload, dict) else {}

    async def open_interest(self, symbol: str) -> dict[str, Any]:
        r = await self._client.get(
            f"{self.base_url}/fapi/v1/openInterest",
            params={"symbol": symbol},
        )
        r.raise_for_status()
        payload = r.json()
        return payload if isinstance(payload, dict) else {}

    async def test_order(self, params: dict[str, Any]) -> dict[str, Any]:
        return await self._signed_post("/fapi/v1/order/test", params)

    async def new_order(self, params: dict[str, Any]) -> dict[str, Any]:
        return await self._signed_post("/fapi/v1/order", params)

    async def _signed_post(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise RuntimeError("Missing Binance API credentials")
        payload = dict(params)
        payload["timestamp"] = int(time.time() * 1000)
        query = urlencode(payload, doseq=True)
        payload["signature"] = sign_query(self.api_secret, query)
        headers = {"X-MBX-APIKEY": self.api_key}
        r = await self._client.post(f"{self.base_url}{path}", data=payload, headers=headers)
        r.raise_for_status()
        if r.text:
            return r.json()
        return {"status": "ok"}
