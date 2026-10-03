from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    app_env: str
    app_host: str
    app_port: int
    log_level: str
    binance_rest_base: str
    binance_ws_base: str
    binance_api_key: str
    binance_api_secret: str
    binance_testnet: bool
    binance_symbol: str
    sqlite_path: Path
    ml_sqlite_path: Path
    replay_dir: Path
    market_events_dir: Path
    snapshot_dir: Path
    ml_features_dir: Path
    models_root: Path
    ml_retention_days: int
    market_pair_soft_cap_gb: float
    project_root: Path


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    load_dotenv()
    project_root = Path(__file__).resolve().parents[2]
    sqlite_path = project_root / os.getenv("SQLITE_PATH", "backend/data/sqlite/terminal.db")
    ml_sqlite_path = project_root / os.getenv("ML_SQLITE_PATH", "backend/data/sqlite/ml.db")
    replay_dir = project_root / os.getenv("REPLAY_DIR", "backend/data/replays")
    market_events_dir = project_root / os.getenv("MARKET_EVENTS_DIR", "backend/data/market_events")
    snapshot_dir = project_root / os.getenv("SNAPSHOT_DIR", "backend/data/snapshots")
    ml_features_dir = project_root / os.getenv("ML_FEATURES_DIR", "backend/data/ml/features")
    models_root = project_root / os.getenv("MODELS_ROOT", "models")
    return Settings(
        app_env=os.getenv("APP_ENV", "dev"),
        app_host=os.getenv("APP_HOST", "127.0.0.1"),
        app_port=int(os.getenv("APP_PORT", "8000")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        binance_rest_base=os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com"),
        binance_ws_base=os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com"),
        binance_api_key=os.getenv("BINANCE_API_KEY", ""),
        binance_api_secret=os.getenv("BINANCE_API_SECRET", ""),
        binance_testnet=_to_bool(os.getenv("BINANCE_TESTNET", "false")),
        binance_symbol=os.getenv("BINANCE_SYMBOL", "BTCUSDT").upper(),
        sqlite_path=sqlite_path,
        ml_sqlite_path=ml_sqlite_path,
        replay_dir=replay_dir,
        market_events_dir=market_events_dir,
        snapshot_dir=snapshot_dir,
        ml_features_dir=ml_features_dir,
        models_root=models_root,
        ml_retention_days=max(1, int(os.getenv("ML_RETENTION_DAYS", "90"))),
        market_pair_soft_cap_gb=max(0.5, float(os.getenv("MARKET_PAIR_SOFT_CAP_GB", "25"))),
        project_root=project_root,
    )


def load_json_config(settings: Settings, name: str) -> dict:
    path = settings.project_root / "config" / name
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
