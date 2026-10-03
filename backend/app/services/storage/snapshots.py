from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path



def write_snapshot(snapshot_dir: Path, name: str, payload: dict) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = snapshot_dir / f"{name}_{ts}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
