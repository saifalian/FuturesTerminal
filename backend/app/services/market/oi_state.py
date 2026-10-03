from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class OIState:
    value: float = 0.0
    updated_at: str = ""
