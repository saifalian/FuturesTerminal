from __future__ import annotations

from app.constants import PUBLIC_STREAM_SUFFIXES



def build_streams(symbol: str) -> list[str]:
    symbol = symbol.lower()
    return [f"{symbol}@{suffix}" for suffix in PUBLIC_STREAM_SUFFIXES]
