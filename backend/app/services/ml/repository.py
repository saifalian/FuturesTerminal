from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiosqlite

from app.core.time_utils import utc_now_iso
from app.services.storage.db import connect_sqlite, init_schema


@dataclass(slots=True)
class MlProfile:
    pair_symbol: str
    selected_exchanges: list[str]
    training_selected_exchanges: list[str]
    bot_data_selected_exchanges: list[str]
    bot_execution_selected_exchanges: list[str]
    bot_signal_mode: str
    execution_position_mode: str
    use_local_data: bool
    use_historic_data: bool
    historic_data_days: int
    horizons: list[int]
    label_threshold_pct: float
    min_data_hours: float
    normalizer_window: int
    replay_candle_limit: int
    training_hour_window_enabled: bool
    training_hour_start: int
    training_hour_end: int
    full_data_mode: bool
    strict_full_windows_mode: bool
    disabled_training_features: list[str]
    target_mode: str
    triple_barrier: dict[str, Any]
    barrier_debug: dict[str, Any]
    training: dict[str, Any]
    paper_bot: dict[str, Any]
    updated_at: str


@dataclass(slots=True)
class MlInstance:
    instance_id: str
    name: str
    pair_symbol: str
    created_at: str
    updated_at: str
    archived: bool


@dataclass(slots=True)
class MlBotTradeSession:
    session_id: str
    instance_id: str
    name: str
    is_default: bool
    created_at: str
    updated_at: str
    archived: bool
    trade_count: int = 0


DEFAULT_PROFILE = {
    "selected_exchanges": [],
    "training_selected_exchanges": [],
    "bot_data_selected_exchanges": [],
    "bot_execution_selected_exchanges": [],
    "bot_signal_mode": "combined",
    "execution_position_mode": "combined_position",
    "use_local_data": True,
    "use_historic_data": False,
    "historic_data_days": 90,
    "horizons": [5, 15, 30, 60],
    "label_threshold_pct": 0.05,
    "target_mode": "trade_outcome",
    "triple_barrier": {"tp_pct": 0.08, "sl_pct": 0.05, "timeout_steps": 180},
    "barrier_debug": {
        "enable_future_path_probe": True,
        "future_path_probe_samples": 5,
        "future_path_probe_depth": 20,
        "step_contiguity_tolerance_ms": 500,
        "timeout_unit_hint": "steps",
    },
    "min_data_hours": 48.0,
    "normalizer_window": 1000,
    "replay_candle_limit": 240,
    "training_hour_window_enabled": False,
    "training_hour_start": 0,
    "training_hour_end": 0,
    "full_data_mode": True,
    "strict_full_windows_mode": True,
    "disabled_training_features": [
        "liq_events_60s",
        "liq_count_60s",
        "liq_notional_60s",
        "liq_buy_sell_imbalance",
        "liq_momentum",
        "open_interest_change_pct",
        "oi_velocity",
        "hour_of_day_sin",
        "hour_of_day_cos",
        "session_asia_eu_us",
        "funding_rate",
    ],
    "label_guard_enabled": True,
    "training": {
        "epochs": 8,
        "batch_size": 64,
        "learning_rate": 0.001,
        "lookback_steps": 60,
        "hidden_size": 128,
        "num_layers": 2,
        "dropout": 0.2,
        "warm_start_from_current": False,
        "use_class_weighted_loss": False,
        "class_weight_mode": "auto",
        "class_weight_down": 1.0,
        "class_weight_flat": 1.0,
        "class_weight_up": 1.0,
        "use_focal_loss": False,
        "focal_gamma": 2.0,
        "focal_use_alpha_class_weights": True,
        "use_balanced_sampler": False,
        "use_class_quota_batches": False,
        "class_quota_per_batch": 4,
        "label_smoothing": 0.03,
        "epoch_trace_capture_list": "",
        "capture_trace_train": False,
        "capture_trace_val": False,
        "capture_trace_test": False,
        "early_stopping_monitor": "macro_f1",
        "early_stopping_enabled": False,
        "early_stopping_patience": 1,
        "early_stopping_min_delta": 0.0,
    },
    "paper_bot": {
        "enabled": False,
        "gate_mode": "confidence_only",
        "gate_profile": "balanced",
        "confidence_threshold": 0.72,
        "entry_confidence_threshold": 0.72,
        "exit_confidence_threshold": 0.55,
        "minimum_directional_edge": 0.03,
        "temperature_scaling_enabled": True,
        "minimum_action_rate": 0.03,
        "minimum_directional_samples": 30,
        "minimum_precision_directional": 0.0,
        "max_false_long_rate": 0.60,
        "max_false_short_rate": 0.60,
        "min_long_precision": 0.45,
        "min_short_precision": 0.45,
        "min_long_when_actual_up": 0.20,
        "min_short_when_actual_down": 0.20,
        "minimum_directional_trades_per_side": 30,
        "estimated_roundtrip_cost_bps": 6.0,
        "max_signal_age_ms": 2_000,
        "volatility_guard_enabled": False,
        "max_realized_volatility": 0.02,
        "volatility_guard_action": "hold_only",
        "max_spread_bps": 15.0,
        "min_orderbook_depth": 0.0,
        "live_calibration_guard_action": "monitor_only",
        "max_trades_per_hour": 30,
        "max_consecutive_entries": 5,
        "min_seconds_between_entries": 5,
        "enable_side_threshold_sweep": False,
        "long_confidence_thresholds": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
        "short_confidence_thresholds": [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
        "quality_threshold": 0.72,
        "min_trade_rate_floor": 0.01,
        "min_flat_rate_required": 0.12,
        "max_label_price_rejected_rate": 0.005,
        "fee_bps_round_trip": 4.0,
        "slippage_bps_round_trip": 2.0,
        "min_samples_per_bucket": 200,
        "poll_delay_ms": 2000,
        "max_position_qty": 1.0,
        "max_hold_seconds": 120,
        "leverage": 5.0,
        "commission_fee_pct": 0.04,
        "one_trade_at_time": True,
        "initial_balance": 100.0,
        "take_profit_pct": 0.25,
        "stop_loss_pct": 0.2,
        "use_trailing_stop": False,
        "trailing_stop_pct": 0.15,
        "regime_filter_enabled": True,
        "max_spread_pct": 0.15,
        "min_trade_rate_10s": 3.0,
        "min_confidence_gap": 0.06,
    },
}

MAX_HISTORIC_DATA_DAYS = max(1, int(os.getenv("ML_HISTORIC_MAX_DAYS", "3650")))


class MlRepository:
    def __init__(self, sqlite_path: Path, schema_path: Path) -> None:
        self._sqlite_path = sqlite_path
        self._schema_path = schema_path
        self._conn: aiosqlite.Connection | None = None

    async def start(self) -> None:
        self._conn = await connect_sqlite(self._sqlite_path)
        await init_schema(self._conn, self._schema_path)
        await self._migrate_instance_schema()

    async def stop(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("MlRepository is not started")
        return self._conn

    async def _migrate_instance_schema(self) -> None:
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_instances (
                instance_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                pair_symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_instance_profiles (
                instance_id TEXT PRIMARY KEY,
                pair_symbol TEXT NOT NULL,
                config_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_instance_bot_state (
                instance_id TEXT PRIMARY KEY,
                pair_symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                position_side TEXT NOT NULL,
                active_session_id TEXT NOT NULL DEFAULT '',
                entry_price REAL NOT NULL,
                qty REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                realized_pnl REAL NOT NULL,
                last_signal_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_bot_trades (
                trade_id TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL DEFAULT '',
                session_id TEXT NOT NULL DEFAULT '',
                pair_symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty REAL NOT NULL,
                leverage REAL NOT NULL,
                commission_fee_pct REAL NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                entry_notional REAL NOT NULL,
                exit_notional REAL NOT NULL,
                entry_fee REAL NOT NULL,
                exit_fee REAL NOT NULL,
                gross_pnl REAL NOT NULL,
                net_pnl REAL NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                close_reason TEXT NOT NULL,
                decision_context_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_bot_trade_sessions (
                session_id TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL,
                name TEXT NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_bot_inference_points (
                point_id TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL,
                pair_symbol TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                confidence REAL NOT NULL,
                quality REAL NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_run_epoch_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                epoch_index INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                metrics_json TEXT NOT NULL
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_run_prediction_trace (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                epoch_index INTEGER NOT NULL DEFAULT 0,
                split TEXT NOT NULL DEFAULT 'test',
                pair_symbol TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                frame_json TEXT NOT NULL
            )
            """
        )
        await self._ensure_column("ml_runs", "instance_id", "TEXT")
        await self._ensure_column("ml_models", "instance_id", "TEXT")
        await self._ensure_column("ml_bot_trades", "session_id", "TEXT DEFAULT ''")
        await self._ensure_column("ml_bot_trades", "decision_context_json", "TEXT DEFAULT '{}'")
        await self._ensure_column("ml_instance_bot_state", "active_session_id", "TEXT DEFAULT ''")
        await self._ensure_column("ml_run_prediction_trace", "epoch_index", "INTEGER NOT NULL DEFAULT 0")
        await self._ensure_column("ml_run_prediction_trace", "split", "TEXT NOT NULL DEFAULT 'test'")
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_runs_instance ON ml_runs(instance_id, created_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_models_instance ON ml_models(instance_id, approved_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_instance_profiles_pair ON ml_instance_profiles(pair_symbol, updated_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_instances_pair ON ml_instances(pair_symbol, archived, updated_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_bot_trades_instance ON ml_bot_trades(instance_id, created_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_bot_trades_pair ON ml_bot_trades(pair_symbol, created_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_bot_trades_instance_session ON ml_bot_trades(instance_id, session_id, created_at)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_bot_inference_instance_ts ON ml_bot_inference_points(instance_id, ts_ms)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_run_epoch_metrics_run_epoch ON ml_run_epoch_metrics(run_id, epoch_index)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_run_prediction_trace_run_step ON ml_run_prediction_trace(run_id, step_index)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_run_prediction_trace_run_epoch_step ON ml_run_prediction_trace(run_id, epoch_index, step_index)"
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_run_prediction_trace_run_split_epoch_step ON ml_run_prediction_trace(run_id, split, epoch_index, step_index)"
        )
        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_default
            ON ml_bot_trade_sessions(instance_id, is_default)
            WHERE is_default = 1
            """
        )
        await self.conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_name_ci
            ON ml_bot_trade_sessions(instance_id, lower(name))
            WHERE archived = 0
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ml_bot_trade_sessions_instance ON ml_bot_trade_sessions(instance_id, archived, updated_at)"
        )
        await self._ensure_default_sessions_for_existing_instances()
        await self.conn.commit()

    async def insert_bot_inference_point(self, payload: dict[str, Any], instance_id: str) -> None:
        point_id = str(payload.get("point_id", f"pt_{uuid4().hex[:16]}"))
        ts_ms = int(payload.get("ts_ms", 0) or 0)
        if not instance_id or ts_ms <= 0:
            return
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO ml_bot_inference_points (
                point_id, instance_id, pair_symbol, ts_ms, confidence, quality, action, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                point_id,
                str(instance_id),
                str(payload.get("pair_symbol", "")).upper(),
                ts_ms,
                float(payload.get("confidence", 0.0) or 0.0),
                float(payload.get("quality", 0.0) or 0.0),
                str(payload.get("action", "HOLD") or "HOLD"),
                str(payload.get("reason", "") or ""),
                str(payload.get("created_at", utc_now_iso())),
            ),
        )
        # keep table bounded per instance
        await self.conn.execute(
            """
            DELETE FROM ml_bot_inference_points
            WHERE instance_id = ?
              AND point_id NOT IN (
                SELECT point_id FROM ml_bot_inference_points
                WHERE instance_id = ?
                ORDER BY ts_ms DESC
                LIMIT 5000
              )
            """,
            (str(instance_id), str(instance_id)),
        )
        await self.conn.commit()

    async def list_bot_inference_points(
        self,
        *,
        instance_id: str,
        since_ts_ms: int = 0,
        limit: int = 600,
    ) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT ts_ms, confidence, quality, action, reason
            FROM ml_bot_inference_points
            WHERE instance_id = ?
              AND (? <= 0 OR ts_ms >= ?)
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(instance_id), int(since_ts_ms), int(since_ts_ms), max(1, int(limit))),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        rows = list(reversed(rows))
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "ts_ms": int(row[0] or 0),
                    "confidence": float(row[1] or 0.0),
                    "quality": float(row[2] or 0.0),
                    "action": str(row[3] or "HOLD"),
                    "reason": str(row[4] or ""),
                }
            )
        return out

    async def _ensure_column(self, table: str, column: str, dtype: str) -> None:
        cursor = await self.conn.execute(f"PRAGMA table_info({table})")
        rows = await cursor.fetchall()
        await cursor.close()
        cols = {str(row[1]).lower() for row in rows}
        if column.lower() in cols:
            return
        await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")

    async def create_instance(self, *, instance_id: str, name: str, pair_symbol: str) -> MlInstance:
        now = utc_now_iso()
        await self.conn.execute(
            """
            INSERT INTO ml_instances (instance_id, name, pair_symbol, created_at, updated_at, archived)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (instance_id, name, pair_symbol.upper(), now, now),
        )
        await self._ensure_default_session(instance_id)
        await self.conn.commit()
        return MlInstance(
            instance_id=instance_id,
            name=name,
            pair_symbol=pair_symbol.upper(),
            created_at=now,
            updated_at=now,
            archived=False,
        )

    async def list_instances(self, *, include_archived: bool = False) -> list[MlInstance]:
        if include_archived:
            cursor = await self.conn.execute(
                "SELECT instance_id, name, pair_symbol, created_at, updated_at, archived FROM ml_instances ORDER BY updated_at DESC"
            )
        else:
            cursor = await self.conn.execute(
                "SELECT instance_id, name, pair_symbol, created_at, updated_at, archived FROM ml_instances WHERE archived = 0 ORDER BY updated_at DESC"
            )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            MlInstance(
                instance_id=str(r[0]),
                name=str(r[1]),
                pair_symbol=str(r[2]).upper(),
                created_at=str(r[3]),
                updated_at=str(r[4]),
                archived=bool(int(r[5] or 0)),
            )
            for r in rows
        ]

    async def get_instance(self, instance_id: str) -> MlInstance | None:
        cursor = await self.conn.execute(
            "SELECT instance_id, name, pair_symbol, created_at, updated_at, archived FROM ml_instances WHERE instance_id = ?",
            (instance_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return MlInstance(
            instance_id=str(row[0]),
            name=str(row[1]),
            pair_symbol=str(row[2]).upper(),
            created_at=str(row[3]),
            updated_at=str(row[4]),
            archived=bool(int(row[5] or 0)),
        )

    async def update_instance(self, instance_id: str, *, name: str | None = None, archived: bool | None = None) -> MlInstance | None:
        current = await self.get_instance(instance_id)
        if current is None:
            return None
        merged_name = str(name) if name is not None else current.name
        merged_archived = bool(archived) if archived is not None else current.archived
        now = utc_now_iso()
        await self.conn.execute(
            "UPDATE ml_instances SET name = ?, archived = ?, updated_at = ? WHERE instance_id = ?",
            (merged_name, 1 if merged_archived else 0, now, instance_id),
        )
        await self.conn.commit()
        return MlInstance(
            instance_id=instance_id,
            name=merged_name,
            pair_symbol=current.pair_symbol,
            created_at=current.created_at,
            updated_at=now,
            archived=merged_archived,
        )

    async def ensure_default_instance_for_pair(self, pair_symbol: str) -> MlInstance:
        pair_symbol = pair_symbol.upper()
        default_instance_id = f"inst_{pair_symbol.lower()}_default"
        existing = await self.get_instance(default_instance_id)
        if existing is not None:
            await self._ensure_default_session(existing.instance_id)
            return existing
        return await self.create_instance(
            instance_id=default_instance_id,
            name=f"{pair_symbol} Default",
            pair_symbol=pair_symbol,
        )

    async def record_manifest(
        self,
        *,
        pair_symbol: str,
        tab_id: str,
        date_key: str,
        file_path: str,
        row_count: int,
        ts_start_ms: int,
        ts_end_ms: int,
        schema_version: str,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO ml_feature_manifests
            (pair_symbol, tab_id, date_key, file_path, row_count, ts_start_ms, ts_end_ms, schema_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pair_symbol,
                tab_id,
                date_key,
                file_path,
                row_count,
                ts_start_ms,
                ts_end_ms,
                schema_version,
                utc_now_iso(),
            ),
        )
        await self.conn.commit()

    async def purge_old_manifests(self, min_ts_ms: int) -> list[str]:
        cursor = await self.conn.execute(
            "SELECT file_path FROM ml_feature_manifests WHERE ts_end_ms < ?",
            (min_ts_ms,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        file_paths = [str(item[0]) for item in rows]
        await self.conn.execute("DELETE FROM ml_feature_manifests WHERE ts_end_ms < ?", (min_ts_ms,))
        await self.conn.commit()
        return file_paths

    async def data_status(self, pair_symbol: str) -> dict[str, Any]:
        cursor = await self.conn.execute(
            """
            SELECT
                COUNT(*) AS file_count,
                COALESCE(SUM(row_count), 0) AS rows_total,
                MIN(ts_start_ms) AS min_ts,
                MAX(ts_end_ms) AS max_ts
            FROM ml_feature_manifests
            WHERE pair_symbol = ?
            """,
            (pair_symbol,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        file_count = int(row[0] or 0)
        rows_total = int(row[1] or 0)
        min_ts = int(row[2]) if row[2] is not None else None
        max_ts = int(row[3]) if row[3] is not None else None
        data_hours = 0.0
        if min_ts is not None and max_ts is not None and max_ts > min_ts:
            data_hours = (max_ts - min_ts) / 3_600_000
        return {
            "pair_symbol": pair_symbol,
            "file_count": file_count,
            "rows_total": rows_total,
            "min_ts_ms": min_ts,
            "max_ts_ms": max_ts,
            "data_hours": data_hours,
        }

    async def manifests_for_pair(self, pair_symbol: str) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT file_path, ts_start_ms, ts_end_ms, row_count, schema_version, date_key, tab_id
            FROM ml_feature_manifests
            WHERE pair_symbol = ?
            ORDER BY ts_start_ms ASC
            """,
            (pair_symbol,),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                decision_context = json.loads(str(row[20] or "{}"))
                if not isinstance(decision_context, dict):
                    decision_context = {}
            except Exception:
                decision_context = {}
            out.append(
                {
                    "file_path": str(row[0]),
                    "ts_start_ms": int(row[1]),
                    "ts_end_ms": int(row[2]),
                    "row_count": int(row[3]),
                    "schema_version": str(row[4]),
                    "date_key": str(row[5]),
                    "tab_id": str(row[6]),
                }
            )
        return out

    async def upsert_profile(self, pair_symbol: str, config: dict[str, Any]) -> MlProfile:
        normalized = self._normalize_profile(pair_symbol, config)
        updated_at = utc_now_iso()
        await self.conn.execute(
            """
            INSERT INTO ml_pair_profiles (pair_symbol, config_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(pair_symbol) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at
            """,
            (pair_symbol, json.dumps(normalized), updated_at),
        )
        await self.conn.commit()
        return self._profile_from_row(pair_symbol, normalized, updated_at)

    async def get_profile(self, pair_symbol: str) -> MlProfile:
        cursor = await self.conn.execute(
            "SELECT config_json, updated_at FROM ml_pair_profiles WHERE pair_symbol = ?",
            (pair_symbol,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return self._profile_from_row(pair_symbol, self._normalize_profile(pair_symbol, {}), utc_now_iso())
        config = json.loads(str(row[0]))
        updated_at = str(row[1])
        return self._profile_from_row(pair_symbol, self._normalize_profile(pair_symbol, config), updated_at)

    async def list_profile_pairs(self) -> list[str]:
        cursor = await self.conn.execute("SELECT pair_symbol FROM ml_pair_profiles ORDER BY pair_symbol ASC")
        rows = await cursor.fetchall()
        await cursor.close()
        return [str(item[0]).upper() for item in rows]

    async def upsert_instance_profile(self, instance_id: str, pair_symbol: str, config: dict[str, Any]) -> MlProfile:
        normalized = self._normalize_profile(pair_symbol, config)
        updated_at = utc_now_iso()
        await self.conn.execute(
            """
            INSERT INTO ml_instance_profiles (instance_id, pair_symbol, config_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(instance_id) DO UPDATE SET
                pair_symbol=excluded.pair_symbol,
                config_json=excluded.config_json,
                updated_at=excluded.updated_at
            """,
            (instance_id, pair_symbol.upper(), json.dumps(normalized), updated_at),
        )
        await self.conn.commit()
        return self._profile_from_row(pair_symbol, normalized, updated_at)

    async def get_instance_profile(self, instance_id: str, pair_symbol_fallback: str) -> MlProfile:
        cursor = await self.conn.execute(
            "SELECT pair_symbol, config_json, updated_at FROM ml_instance_profiles WHERE instance_id = ?",
            (instance_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            pair_symbol = pair_symbol_fallback.upper()
            return self._profile_from_row(pair_symbol, self._normalize_profile(pair_symbol, {}), utc_now_iso())
        pair_symbol = str(row[0]).upper()
        config = json.loads(str(row[1]))
        updated_at = str(row[2])
        return self._profile_from_row(pair_symbol, self._normalize_profile(pair_symbol, config), updated_at)

    async def create_run(self, run: dict[str, Any]) -> None:
        await self.conn.execute(
            """
            INSERT INTO ml_runs (
                run_id, pair_symbol, instance_id, status, stage, stage_progress, device, created_at,
                updated_at, started_at, ended_at, config_json, metrics_json, error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run["run_id"],
                run["pair_symbol"],
                str(run.get("instance_id", "")),
                run["status"],
                run["stage"],
                float(run.get("stage_progress", 0.0)),
                run.get("device", "pending"),
                run["created_at"],
                run["updated_at"],
                run.get("started_at"),
                run.get("ended_at"),
                json.dumps(run.get("config", {})),
                json.dumps(run.get("metrics", {})),
                str(run.get("error_text", "")),
            ),
        )
        await self.conn.commit()

    async def update_run(self, run_id: str, updates: dict[str, Any]) -> None:
        current = await self.get_run(run_id)
        if current is None:
            return
        merged = {**current, **updates}
        await self.conn.execute(
            """
            UPDATE ml_runs
            SET status=?, stage=?, stage_progress=?, device=?, updated_at=?, started_at=?, ended_at=?, config_json=?, metrics_json=?, error_text=?
            WHERE run_id=?
            """,
            (
                merged.get("status"),
                merged.get("stage"),
                float(merged.get("stage_progress", 0.0)),
                merged.get("device", "pending"),
                merged.get("updated_at", utc_now_iso()),
                merged.get("started_at"),
                merged.get("ended_at"),
                json.dumps(merged.get("config", {})),
                json.dumps(merged.get("metrics", {})),
                str(merged.get("error_text", "")),
                run_id,
            ),
        )
        await self.conn.commit()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            """
            SELECT
                run_id, pair_symbol, instance_id, status, stage, stage_progress, device, created_at, updated_at,
                started_at, ended_at, config_json, metrics_json, error_text
            FROM ml_runs
            WHERE run_id = ?
            """,
            (run_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return self._run_from_row(row)

    async def list_runs(self, pair_symbol: str | None = None, limit: int = 100, instance_id: str | None = None) -> list[dict[str, Any]]:
        if instance_id:
            cursor = await self.conn.execute(
                """
                SELECT
                    run_id, pair_symbol, instance_id, status, stage, stage_progress, device, created_at, updated_at,
                    started_at, ended_at, config_json, metrics_json, error_text
                FROM ml_runs
                WHERE instance_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (instance_id, limit),
            )
        elif pair_symbol:
            cursor = await self.conn.execute(
                """
                SELECT
                    run_id, pair_symbol, instance_id, status, stage, stage_progress, device, created_at, updated_at,
                    started_at, ended_at, config_json, metrics_json, error_text
                FROM ml_runs
                WHERE pair_symbol = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (pair_symbol, limit),
            )
        else:
            cursor = await self.conn.execute(
                """
                SELECT
                    run_id, pair_symbol, instance_id, status, stage, stage_progress, device, created_at, updated_at,
                    started_at, ended_at, config_json, metrics_json, error_text
                FROM ml_runs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cursor.fetchall()
        await cursor.close()
        return [self._run_from_row(row) for row in rows]

    async def append_run_log(self, run_id: str, level: str, message: str) -> None:
        await self.conn.execute(
            "INSERT INTO ml_run_logs (run_id, ts, level, message) VALUES (?, ?, ?, ?)",
            (run_id, utc_now_iso(), level.upper(), message),
        )
        await self.conn.commit()

    async def append_run_epoch_metrics(self, run_id: str, epoch_index: int, metrics: dict[str, Any]) -> None:
        await self.conn.execute(
            """
            INSERT INTO ml_run_epoch_metrics (run_id, epoch_index, created_at, metrics_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                run_id,
                int(epoch_index),
                utc_now_iso(),
                json.dumps(metrics),
            ),
        )
        await self.conn.commit()

    async def list_run_epoch_metrics(self, run_id: str, limit: int = 1000) -> list[dict[str, Any]]:
        cursor = await self.conn.execute(
            """
            SELECT epoch_index, created_at, metrics_json
            FROM ml_run_epoch_metrics
            WHERE run_id = ?
            ORDER BY epoch_index ASC
            LIMIT ?
            """,
            (run_id, int(limit)),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        payload: list[dict[str, Any]] = []
        for row in rows:
            decoded = json.loads(str(row[2] or "{}"))
            if not isinstance(decoded, dict):
                decoded = {}
            payload.append(
                {
                    "epoch_index": int(row[0] or 0),
                    "created_at": str(row[1]),
                    **decoded,
                }
            )
        return payload

    async def replace_run_prediction_trace(
        self,
        run_id: str,
        pair_symbol: str,
        frames: list[dict[str, Any]],
        *,
        epoch_index: int = 0,
        split: str = "test",
    ) -> None:
        epoch_value = int(epoch_index)
        split_value = str(split or "test").strip().lower() or "test"
        await self.conn.execute(
            "DELETE FROM ml_run_prediction_trace WHERE run_id = ? AND epoch_index = ? AND split = ?",
            (run_id, epoch_value, split_value),
        )
        now_iso = utc_now_iso()
        if frames:
            await self.conn.executemany(
                """
                INSERT INTO ml_run_prediction_trace (run_id, epoch_index, split, pair_symbol, step_index, created_at, frame_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        epoch_value,
                        split_value,
                        pair_symbol.upper(),
                        int(idx),
                        now_iso,
                        json.dumps(frame),
                    )
                    for idx, frame in enumerate(frames)
                ],
            )
        await self.conn.commit()

    async def list_run_prediction_trace(
        self,
        run_id: str,
        limit: int = 5000,
        *,
        epoch_index: int = 0,
        split: str = "test",
    ) -> list[dict[str, Any]]:
        split_value = str(split or "test").strip().lower() or "test"
        cursor = await self.conn.execute(
            """
            SELECT step_index, frame_json
            FROM ml_run_prediction_trace
            WHERE run_id = ? AND epoch_index = ? AND split = ?
            ORDER BY step_index ASC
            LIMIT ?
            """,
            (run_id, int(epoch_index), split_value, int(limit)),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        payload: list[dict[str, Any]] = []
        for row in rows:
            decoded = json.loads(str(row[1] or "{}"))
            if not isinstance(decoded, dict):
                decoded = {}
            payload.append({"step_index": int(row[0] or 0), **decoded})
        return payload

    async def list_run_prediction_trace_epochs(self, run_id: str, *, split: str = "test") -> list[int]:
        split_value = str(split or "test").strip().lower() or "test"
        cursor = await self.conn.execute(
            """
            SELECT DISTINCT epoch_index
            FROM ml_run_prediction_trace
            WHERE run_id = ? AND split = ?
            ORDER BY epoch_index ASC
            """,
            (run_id, split_value),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [int(row[0] or 0) for row in rows]

    async def run_logs(self, run_id: str, limit: int = 500) -> list[dict[str, str]]:
        cursor = await self.conn.execute(
            """
            SELECT ts, level, message
            FROM ml_run_logs
            WHERE run_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (run_id, limit),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [{"ts": str(row[0]), "level": str(row[1]), "message": str(row[2])} for row in reversed(rows)]

    async def delete_run(self, run_id: str) -> dict[str, Any] | None:
        run = await self.get_run(run_id)
        if run is None:
            return None

        cursor = await self.conn.execute(
            "SELECT pair_symbol, run_id, model_path, normalizer_path, is_current FROM ml_models WHERE run_id = ?",
            (run_id,),
        )
        model_rows = await cursor.fetchall()
        await cursor.close()
        if any(int(row[4] or 0) == 1 for row in model_rows):
            raise ValueError("Cannot delete currently approved run. Approve another run first.")

        await self.conn.execute("DELETE FROM ml_run_logs WHERE run_id = ?", (run_id,))
        await self.conn.execute("DELETE FROM ml_run_epoch_metrics WHERE run_id = ?", (run_id,))
        await self.conn.execute("DELETE FROM ml_run_prediction_trace WHERE run_id = ?", (run_id,))
        await self.conn.execute("DELETE FROM ml_models WHERE run_id = ?", (run_id,))
        await self.conn.execute("DELETE FROM ml_runs WHERE run_id = ?", (run_id,))
        await self.conn.commit()

        return {
            "pair_symbol": str(run["pair_symbol"]).upper(),
            "run_id": run_id,
            "deleted_model_rows": len(model_rows),
        }

    async def set_current_model(
        self,
        *,
        pair_symbol: str,
        instance_id: str,
        run_id: str,
        model_path: str,
        normalizer_path: str,
        metrics: dict[str, Any],
    ) -> None:
        await self.conn.execute("UPDATE ml_models SET is_current = 0 WHERE pair_symbol = ? AND instance_id = ?", (pair_symbol, instance_id))
        await self.conn.execute(
            """
            INSERT INTO ml_models (pair_symbol, instance_id, run_id, model_path, normalizer_path, metrics_json, approved_at, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                pair_symbol,
                instance_id,
                run_id,
                model_path,
                normalizer_path,
                json.dumps(metrics),
                utc_now_iso(),
            ),
        )
        await self.conn.commit()

    async def current_model(self, pair_symbol: str, instance_id: str = "") -> dict[str, Any] | None:
        cursor = await self.conn.execute(
            """
            SELECT run_id, model_path, normalizer_path, metrics_json, approved_at, instance_id
            FROM ml_models
            WHERE pair_symbol = ? AND instance_id = ? AND is_current = 1
            ORDER BY id DESC
            LIMIT 1
            """,
            (pair_symbol, instance_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return {
            "pair_symbol": pair_symbol,
            "run_id": str(row[0]),
            "model_path": str(row[1]),
            "normalizer_path": str(row[2]),
            "metrics": json.loads(str(row[3] or "{}")),
            "approved_at": str(row[4]),
            "instance_id": str(row[5] or ""),
        }

    async def upsert_bot_state(self, pair_symbol: str, payload: dict[str, Any], instance_id: str = "") -> None:
        active_session_id_payload = payload.get("active_session_id")
        active_session_id = (
            str(active_session_id_payload).strip()
            if active_session_id_payload is not None
            else ""
        )
        if instance_id and not active_session_id:
            active_session_id = await self.active_session_id(instance_id)
        data = {
            "status": str(payload.get("status", "IDLE")),
            "position_side": str(payload.get("position_side", "")),
            "active_session_id": active_session_id,
            "entry_price": float(payload.get("entry_price", 0.0) or 0.0),
            "qty": float(payload.get("qty", 0.0) or 0.0),
            "unrealized_pnl": float(payload.get("unrealized_pnl", 0.0) or 0.0),
            "realized_pnl": float(payload.get("realized_pnl", 0.0) or 0.0),
            "last_signal_json": json.dumps(payload.get("last_signal", {})),
            "updated_at": utc_now_iso(),
        }
        if instance_id:
            await self.conn.execute(
                """
                INSERT INTO ml_instance_bot_state
                (instance_id, pair_symbol, status, position_side, active_session_id, entry_price, qty, unrealized_pnl, realized_pnl, last_signal_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    pair_symbol=excluded.pair_symbol,
                    status=excluded.status,
                    position_side=excluded.position_side,
                    active_session_id=excluded.active_session_id,
                    entry_price=excluded.entry_price,
                    qty=excluded.qty,
                    unrealized_pnl=excluded.unrealized_pnl,
                    realized_pnl=excluded.realized_pnl,
                    last_signal_json=excluded.last_signal_json,
                    updated_at=excluded.updated_at
                """,
                (
                    instance_id,
                    pair_symbol,
                    data["status"],
                    data["position_side"],
                    data["active_session_id"],
                    data["entry_price"],
                    data["qty"],
                    data["unrealized_pnl"],
                    data["realized_pnl"],
                    data["last_signal_json"],
                    data["updated_at"],
                ),
            )
        else:
            await self.conn.execute(
                """
                INSERT INTO ml_bot_state
                (pair_symbol, status, position_side, entry_price, qty, unrealized_pnl, realized_pnl, last_signal_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pair_symbol) DO UPDATE SET
                    status=excluded.status,
                    position_side=excluded.position_side,
                    entry_price=excluded.entry_price,
                    qty=excluded.qty,
                    unrealized_pnl=excluded.unrealized_pnl,
                    realized_pnl=excluded.realized_pnl,
                    last_signal_json=excluded.last_signal_json,
                    updated_at=excluded.updated_at
                """,
                (
                    pair_symbol,
                    data["status"],
                    data["position_side"],
                    data["entry_price"],
                    data["qty"],
                    data["unrealized_pnl"],
                    data["realized_pnl"],
                    data["last_signal_json"],
                    data["updated_at"],
                ),
            )
        await self.conn.commit()

    async def bot_state(self, pair_symbol: str, instance_id: str = "") -> dict[str, Any] | None:
        if instance_id:
            cursor = await self.conn.execute(
                """
                SELECT status, position_side, active_session_id, entry_price, qty, unrealized_pnl, realized_pnl, last_signal_json, updated_at
                FROM ml_instance_bot_state
                WHERE instance_id = ?
                """,
                (instance_id,),
            )
        else:
            cursor = await self.conn.execute(
                """
                SELECT status, position_side, entry_price, qty, unrealized_pnl, realized_pnl, last_signal_json, updated_at
                FROM ml_bot_state
                WHERE pair_symbol = ?
                """,
                (pair_symbol,),
            )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        if instance_id:
            return {
                "pair_symbol": pair_symbol,
                "status": str(row[0]),
                "position_side": str(row[1]),
                "active_session_id": str(row[2] or ""),
                "entry_price": float(row[3] or 0.0),
                "qty": float(row[4] or 0.0),
                "unrealized_pnl": float(row[5] or 0.0),
                "realized_pnl": float(row[6] or 0.0),
                "last_signal": json.loads(str(row[7] or "{}")),
                "updated_at": str(row[8]),
            }
        return {
            "pair_symbol": pair_symbol,
            "status": str(row[0]),
            "position_side": str(row[1]),
            "entry_price": float(row[2] or 0.0),
            "qty": float(row[3] or 0.0),
            "unrealized_pnl": float(row[4] or 0.0),
            "realized_pnl": float(row[5] or 0.0),
            "last_signal": json.loads(str(row[6] or "{}")),
            "updated_at": str(row[7]),
        }

    async def insert_bot_trade(self, payload: dict[str, Any], instance_id: str = "", session_id: str = "") -> None:
        base_values = (
            str(payload.get("trade_id", "")),
            str(instance_id or ""),
            str(session_id or payload.get("session_id", "") or ""),
            str(payload.get("pair_symbol", "")).upper(),
            str(payload.get("side", "")),
            float(payload.get("qty", 0.0) or 0.0),
            float(payload.get("leverage", 1.0) or 1.0),
            float(payload.get("commission_fee_pct", 0.0) or 0.0),
            float(payload.get("entry_price", 0.0) or 0.0),
            float(payload.get("exit_price", 0.0) or 0.0),
            float(payload.get("entry_notional", 0.0) or 0.0),
            float(payload.get("exit_notional", 0.0) or 0.0),
            float(payload.get("entry_fee", 0.0) or 0.0),
            float(payload.get("exit_fee", 0.0) or 0.0),
            float(payload.get("gross_pnl", 0.0) or 0.0),
            float(payload.get("net_pnl", 0.0) or 0.0),
            str(payload.get("opened_at", "")),
            str(payload.get("closed_at", "")),
            int(payload.get("duration_seconds", 0) or 0),
            str(payload.get("close_reason", "")),
        )
        created_at = str(payload.get("created_at", utc_now_iso()))
        try:
            await self.conn.execute(
                """
                INSERT INTO ml_bot_trades (
                    trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                    entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                    gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, decision_context_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    *base_values,
                    json.dumps(payload.get("decision_context", {}), ensure_ascii=False),
                    created_at,
                ),
            )
        except aiosqlite.OperationalError:
            await self.conn.execute(
                """
                INSERT INTO ml_bot_trades (
                    trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                    entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                    gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (*base_values, created_at),
            )
        await self.conn.commit()

    async def list_bot_trades(
        self,
        pair_symbol: str,
        instance_id: str = "",
        limit: int = 200,
        session_id: str = "",
    ) -> list[dict[str, Any]]:
        has_context = True
        try:
            if instance_id:
                if session_id:
                    cursor = await self.conn.execute(
                        """
                        SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                               entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                               gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, decision_context_json, created_at
                        FROM ml_bot_trades
                        WHERE instance_id = ? AND session_id = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                        (instance_id, session_id, max(1, int(limit))),
                    )
                else:
                    cursor = await self.conn.execute(
                        """
                        SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                               entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                               gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, decision_context_json, created_at
                        FROM ml_bot_trades
                        WHERE instance_id = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                        (instance_id, max(1, int(limit))),
                    )
            else:
                cursor = await self.conn.execute(
                    """
                    SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                           entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                           gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, decision_context_json, created_at
                    FROM ml_bot_trades
                    WHERE pair_symbol = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (pair_symbol.upper(), max(1, int(limit))),
                )
        except aiosqlite.OperationalError:
            has_context = False
            if instance_id:
                if session_id:
                    cursor = await self.conn.execute(
                        """
                        SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                               entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                               gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, created_at
                        FROM ml_bot_trades
                        WHERE instance_id = ? AND session_id = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                        (instance_id, session_id, max(1, int(limit))),
                    )
                else:
                    cursor = await self.conn.execute(
                        """
                        SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                               entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                               gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, created_at
                        FROM ml_bot_trades
                        WHERE instance_id = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                        (instance_id, max(1, int(limit))),
                    )
            else:
                cursor = await self.conn.execute(
                    """
                    SELECT trade_id, instance_id, session_id, pair_symbol, side, qty, leverage, commission_fee_pct,
                           entry_price, exit_price, entry_notional, exit_notional, entry_fee, exit_fee,
                           gross_pnl, net_pnl, opened_at, closed_at, duration_seconds, close_reason, created_at
                    FROM ml_bot_trades
                    WHERE pair_symbol = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (pair_symbol.upper(), max(1, int(limit))),
                )

        rows = await cursor.fetchall()
        await cursor.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            decision_context: dict[str, Any] = {}
            if has_context:
                try:
                    raw = json.loads(str(row[20] or "{}"))
                    decision_context = raw if isinstance(raw, dict) else {}
                except Exception:
                    decision_context = {}
            out.append(
                {
                    "trade_id": str(row[0]),
                    "instance_id": str(row[1] or ""),
                    "session_id": str(row[2] or ""),
                    "pair_symbol": str(row[3]).upper(),
                    "side": str(row[4]),
                    "qty": float(row[5] or 0.0),
                    "leverage": float(row[6] or 1.0),
                    "commission_fee_pct": float(row[7] or 0.0),
                    "entry_price": float(row[8] or 0.0),
                    "exit_price": float(row[9] or 0.0),
                    "entry_notional": float(row[10] or 0.0),
                    "exit_notional": float(row[11] or 0.0),
                    "entry_fee": float(row[12] or 0.0),
                    "exit_fee": float(row[13] or 0.0),
                    "gross_pnl": float(row[14] or 0.0),
                    "net_pnl": float(row[15] or 0.0),
                    "opened_at": str(row[16]),
                    "closed_at": str(row[17]),
                    "duration_seconds": int(row[18] or 0),
                    "close_reason": str(row[19] or ""),
                    "decision_context": decision_context,
                    "created_at": str(row[21] if has_context else row[20]),
                }
            )
        return out

    async def list_bot_trade_sessions(self, instance_id: str, include_archived: bool = False) -> list[MlBotTradeSession]:
        if include_archived:
            cursor = await self.conn.execute(
                """
                SELECT s.session_id, s.instance_id, s.name, s.is_default, s.created_at, s.updated_at, s.archived,
                       COALESCE(t.cnt, 0) AS trade_count
                FROM ml_bot_trade_sessions s
                LEFT JOIN (
                    SELECT session_id, COUNT(*) AS cnt
                    FROM ml_bot_trades
                    WHERE instance_id = ?
                    GROUP BY session_id
                ) t ON t.session_id = s.session_id
                WHERE s.instance_id = ?
                ORDER BY s.is_default DESC, s.updated_at DESC
                """,
                (instance_id, instance_id),
            )
        else:
            cursor = await self.conn.execute(
                """
                SELECT s.session_id, s.instance_id, s.name, s.is_default, s.created_at, s.updated_at, s.archived,
                       COALESCE(t.cnt, 0) AS trade_count
                FROM ml_bot_trade_sessions s
                LEFT JOIN (
                    SELECT session_id, COUNT(*) AS cnt
                    FROM ml_bot_trades
                    WHERE instance_id = ?
                    GROUP BY session_id
                ) t ON t.session_id = s.session_id
                WHERE s.instance_id = ? AND s.archived = 0
                ORDER BY s.is_default DESC, s.updated_at DESC
                """,
                (instance_id, instance_id),
            )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            MlBotTradeSession(
                session_id=str(r[0]),
                instance_id=str(r[1]),
                name=str(r[2]),
                is_default=bool(int(r[3] or 0)),
                created_at=str(r[4]),
                updated_at=str(r[5]),
                archived=bool(int(r[6] or 0)),
                trade_count=int(r[7] or 0),
            )
            for r in rows
        ]

    async def get_bot_trade_session(self, instance_id: str, session_id: str) -> MlBotTradeSession | None:
        cursor = await self.conn.execute(
            """
            SELECT session_id, instance_id, name, is_default, created_at, updated_at, archived
            FROM ml_bot_trade_sessions
            WHERE instance_id = ? AND session_id = ?
            """,
            (instance_id, session_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return MlBotTradeSession(
            session_id=str(row[0]),
            instance_id=str(row[1]),
            name=str(row[2]),
            is_default=bool(int(row[3] or 0)),
            created_at=str(row[4]),
            updated_at=str(row[5]),
            archived=bool(int(row[6] or 0)),
            trade_count=0,
        )

    async def create_bot_trade_session(self, instance_id: str, name: str, is_default: bool = False) -> MlBotTradeSession:
        now = utc_now_iso()
        clean_name = " ".join(str(name or "").strip().split())
        if not clean_name:
            raise ValueError("Session name is required.")
        dup = await self.conn.execute(
            """
            SELECT 1 FROM ml_bot_trade_sessions
            WHERE instance_id = ? AND archived = 0 AND lower(name) = lower(?)
            LIMIT 1
            """,
            (instance_id, clean_name),
        )
        dup_row = await dup.fetchone()
        await dup.close()
        if dup_row is not None:
            raise ValueError("Session name already exists for this bot.")
        session_id = f"sess_{int(time.time())}_{os.urandom(3).hex()}"
        if is_default:
            await self.conn.execute(
                "UPDATE ml_bot_trade_sessions SET is_default = 0, updated_at = ? WHERE instance_id = ?",
                (now, instance_id),
            )
        await self.conn.execute(
            """
            INSERT INTO ml_bot_trade_sessions (session_id, instance_id, name, is_default, created_at, updated_at, archived)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (session_id, instance_id, clean_name, 1 if is_default else 0, now, now),
        )
        await self.conn.commit()
        return MlBotTradeSession(
            session_id=session_id,
            instance_id=instance_id,
            name=clean_name,
            is_default=is_default,
            created_at=now,
            updated_at=now,
            archived=False,
            trade_count=0,
        )

    async def rename_bot_trade_session(self, instance_id: str, session_id: str, name: str) -> MlBotTradeSession:
        current = await self.get_bot_trade_session(instance_id, session_id)
        if current is None or current.archived:
            raise ValueError("Session not found.")
        clean_name = " ".join(str(name or "").strip().split())
        if not clean_name:
            raise ValueError("Session name is required.")
        dup = await self.conn.execute(
            """
            SELECT 1 FROM ml_bot_trade_sessions
            WHERE instance_id = ? AND archived = 0 AND lower(name) = lower(?) AND session_id <> ?
            LIMIT 1
            """,
            (instance_id, clean_name, session_id),
        )
        dup_row = await dup.fetchone()
        await dup.close()
        if dup_row is not None:
            raise ValueError("Session name already exists for this bot.")
        now = utc_now_iso()
        await self.conn.execute(
            "UPDATE ml_bot_trade_sessions SET name = ?, updated_at = ? WHERE session_id = ?",
            (clean_name, now, session_id),
        )
        await self.conn.commit()
        updated = await self.get_bot_trade_session(instance_id, session_id)
        if updated is None:
            raise ValueError("Session not found.")
        return updated

    async def delete_bot_trade_session(self, instance_id: str, session_id: str, move_to_session_id: str) -> None:
        target = await self.get_bot_trade_session(instance_id, session_id)
        if target is None or target.archived:
            raise ValueError("Session not found.")
        if target.is_default:
            raise ValueError("Default session cannot be deleted.")
        if move_to_session_id == session_id:
            raise ValueError("Invalid destination session.")
        move_target = await self.get_bot_trade_session(instance_id, move_to_session_id)
        if move_target is None or move_target.archived:
            raise ValueError("Destination session not found.")
        now = utc_now_iso()
        await self.conn.execute(
            "UPDATE ml_bot_trades SET session_id = ? WHERE instance_id = ? AND session_id = ?",
            (move_to_session_id, instance_id, session_id),
        )
        await self.conn.execute(
            "UPDATE ml_bot_trade_sessions SET archived = 1, updated_at = ? WHERE session_id = ?",
            (now, session_id),
        )
        await self.conn.execute(
            "UPDATE ml_instance_bot_state SET active_session_id = ?, updated_at = ? WHERE instance_id = ? AND active_session_id = ?",
            (move_to_session_id, now, instance_id, session_id),
        )
        await self.conn.commit()

    async def clear_bot_trade_session_history(self, instance_id: str, session_id: str) -> int:
        current = await self.get_bot_trade_session(instance_id, session_id)
        if current is None or current.archived:
            raise ValueError("Session not found.")
        cursor = await self.conn.execute(
            "SELECT COUNT(*) FROM ml_bot_trades WHERE instance_id = ? AND session_id = ?",
            (instance_id, session_id),
        )
        row = await cursor.fetchone()
        await cursor.close()
        deleted = int((row or [0])[0] or 0)
        await self.conn.execute(
            "DELETE FROM ml_bot_trades WHERE instance_id = ? AND session_id = ?",
            (instance_id, session_id),
        )
        await self.conn.commit()
        return deleted

    async def set_active_session(self, instance_id: str, session_id: str) -> None:
        current = await self.get_bot_trade_session(instance_id, session_id)
        if current is None or current.archived:
            raise ValueError("Session not found.")
        now = utc_now_iso()
        cursor = await self.conn.execute(
            "SELECT pair_symbol FROM ml_instance_bot_state WHERE instance_id = ?",
            (instance_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        pair_symbol = str((row or [""])[0] or "")
        if not pair_symbol:
            inst_cursor = await self.conn.execute(
                "SELECT pair_symbol FROM ml_instances WHERE instance_id = ?",
                (instance_id,),
            )
            inst_row = await inst_cursor.fetchone()
            await inst_cursor.close()
            pair_symbol = str((inst_row or [""])[0] or "")
        await self.conn.execute(
            """
            INSERT INTO ml_instance_bot_state
            (instance_id, pair_symbol, status, position_side, active_session_id, entry_price, qty, unrealized_pnl, realized_pnl, last_signal_json, updated_at)
            VALUES (?, ?, 'IDLE', '', ?, 0, 0, 0, 0, '{}', ?)
            ON CONFLICT(instance_id) DO UPDATE SET
                active_session_id=excluded.active_session_id,
                updated_at=excluded.updated_at
            """,
            (instance_id, pair_symbol, session_id, now),
        )
        await self.conn.commit()

    async def active_session_id(self, instance_id: str) -> str:
        cursor = await self.conn.execute(
            "SELECT active_session_id FROM ml_instance_bot_state WHERE instance_id = ?",
            (instance_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return str((row or [""])[0] or "")

    async def default_session(self, instance_id: str) -> MlBotTradeSession:
        return await self._ensure_default_session(instance_id)

    async def _ensure_default_session(self, instance_id: str) -> MlBotTradeSession:
        cursor = await self.conn.execute(
            """
            SELECT session_id, instance_id, name, is_default, created_at, updated_at, archived
            FROM ml_bot_trade_sessions
            WHERE instance_id = ? AND is_default = 1 AND archived = 0
            LIMIT 1
            """,
            (instance_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return await self.create_bot_trade_session(instance_id, "Default", is_default=True)
        return MlBotTradeSession(
            session_id=str(row[0]),
            instance_id=str(row[1]),
            name=str(row[2]),
            is_default=bool(int(row[3] or 0)),
            created_at=str(row[4]),
            updated_at=str(row[5]),
            archived=bool(int(row[6] or 0)),
            trade_count=0,
        )

    async def _ensure_default_sessions_for_existing_instances(self) -> None:
        cursor = await self.conn.execute("SELECT instance_id FROM ml_instances")
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            instance_id = str(row[0] or "")
            if not instance_id:
                continue
            default_session = await self._ensure_default_session(instance_id)
            await self.conn.execute(
                """
                UPDATE ml_bot_trades
                SET session_id = ?
                WHERE instance_id = ? AND (session_id IS NULL OR session_id = '')
                """,
                (default_session.session_id, instance_id),
            )
        await self.conn.commit()

    def _normalize_profile(self, pair_symbol: str, config: dict[str, Any]) -> dict[str, Any]:
        def _as_bool(value: Any, default: bool = False) -> bool:
            if isinstance(value, bool):
                return value
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "on"}
            return bool(value)

        def _normalize_epoch_trace_capture_list(raw: Any, max_epoch: int) -> str:
            values: list[int] = []
            if isinstance(raw, str):
                parts = [item.strip() for item in raw.split(",")]
                for part in parts:
                    if not part:
                        continue
                    try:
                        values.append(int(part))
                    except ValueError:
                        continue
            elif isinstance(raw, (list, tuple)):
                for item in raw:
                    try:
                        values.append(int(item))
                    except Exception:
                        continue
            cleaned = sorted({int(v) for v in values if 1 <= int(v) <= max_epoch})
            return ",".join(str(v) for v in cleaned)

        merged = json.loads(json.dumps(DEFAULT_PROFILE))
        allowed_top_keys = {
            "selected_exchanges",
            "training_selected_exchanges",
            "bot_data_selected_exchanges",
            "bot_execution_selected_exchanges",
            "bot_signal_mode",
            "execution_position_mode",
            "use_local_data",
            "use_historic_data",
            "historic_data_days",
            "horizons",
            "label_threshold_pct",
            "target_mode",
            "triple_barrier",
            "barrier_debug",
            "min_data_hours",
            "normalizer_window",
            "replay_candle_limit",
            "training_hour_window_enabled",
            "training_hour_start",
            "training_hour_end",
            "full_data_mode",
            "strict_full_windows_mode",
            "label_guard_enabled",
            "disabled_training_features",
        }
        merged.update({k: v for k, v in config.items() if k in allowed_top_keys})
        if isinstance(config.get("training"), dict):
            merged["training"].update(config["training"])
        if isinstance(config.get("paper_bot"), dict):
            merged["paper_bot"].update(config["paper_bot"])
        merged["selected_exchanges"] = [str(item).lower() for item in merged.get("selected_exchanges", []) if str(item).strip()]
        merged["training_selected_exchanges"] = [
            str(item).lower() for item in merged.get("training_selected_exchanges", []) if str(item).strip()
        ]
        merged["bot_data_selected_exchanges"] = [
            str(item).lower() for item in merged.get("bot_data_selected_exchanges", []) if str(item).strip()
        ]
        merged["bot_execution_selected_exchanges"] = [
            str(item).lower() for item in merged.get("bot_execution_selected_exchanges", []) if str(item).strip()
        ]
        legacy_selected = merged["selected_exchanges"]
        if not merged["training_selected_exchanges"]:
            merged["training_selected_exchanges"] = list(legacy_selected)
        if not merged["bot_data_selected_exchanges"]:
            merged["bot_data_selected_exchanges"] = list(legacy_selected)
        if not merged["bot_execution_selected_exchanges"]:
            merged["bot_execution_selected_exchanges"] = list(legacy_selected)
        # Compatibility alias for older callers.
        merged["selected_exchanges"] = list(merged["training_selected_exchanges"])
        signal_mode = str(merged.get("bot_signal_mode", "combined") or "combined").strip().lower()
        merged["bot_signal_mode"] = signal_mode if signal_mode in {"combined", "majority", "best"} else "combined"
        exec_mode = str(merged.get("execution_position_mode", "combined_position") or "combined_position").strip().lower()
        merged["execution_position_mode"] = (
            exec_mode if exec_mode in {"combined_position", "separate_positions"} else "combined_position"
        )
        merged["use_local_data"] = bool(merged.get("use_local_data", True))
        merged["use_historic_data"] = bool(merged.get("use_historic_data", False))
        merged["historic_data_days"] = min(
            MAX_HISTORIC_DATA_DAYS,
            max(1, int(merged.get("historic_data_days", 90))),
        )
        merged["horizons"] = [max(1, int(item)) for item in merged.get("horizons", [5, 15, 30, 60])]
        merged["label_threshold_pct"] = float(merged.get("label_threshold_pct", 0.05))
        target_mode = str(merged.get("target_mode", "trade_outcome") or "trade_outcome").strip().lower()
        merged["target_mode"] = target_mode if target_mode in {"triple_barrier", "fixed_return", "trade_outcome"} else "trade_outcome"
        tb_raw = merged.get("triple_barrier", {})
        tb = tb_raw if isinstance(tb_raw, dict) else {}
        merged["triple_barrier"] = {
            "tp_pct": max(0.001, float(tb.get("tp_pct", 0.08))),
            "sl_pct": max(0.001, float(tb.get("sl_pct", 0.05))),
            "timeout_steps": max(1, int(tb.get("timeout_steps", 180))),
        }
        dbg_raw = merged.get("barrier_debug", {})
        dbg = dbg_raw if isinstance(dbg_raw, dict) else {}
        timeout_hint = str(dbg.get("timeout_unit_hint", "steps") or "steps").strip().lower()
        merged["barrier_debug"] = {
            "enable_future_path_probe": _as_bool(dbg.get("enable_future_path_probe", True), True),
            "future_path_probe_samples": max(1, min(20, int(dbg.get("future_path_probe_samples", 5) or 5))),
            "future_path_probe_depth": max(5, min(200, int(dbg.get("future_path_probe_depth", 20) or 20))),
            "step_contiguity_tolerance_ms": max(1, min(60_000, int(dbg.get("step_contiguity_tolerance_ms", 500) or 500))),
            "timeout_unit_hint": timeout_hint if timeout_hint in {"steps", "seconds_hint"} else "steps",
        }
        merged["min_data_hours"] = max(0.1, float(merged.get("min_data_hours", 48.0)))
        merged["normalizer_window"] = max(30, int(merged.get("normalizer_window", 1000)))
        merged["replay_candle_limit"] = max(30, min(5000, int(merged.get("replay_candle_limit", 240))))
        merged["training_hour_window_enabled"] = _as_bool(merged.get("training_hour_window_enabled", False), False)
        merged["training_hour_start"] = max(0, int(merged.get("training_hour_start", 0)))
        merged["training_hour_end"] = max(0, int(merged.get("training_hour_end", 0)))
        if merged["training_hour_end"] != 0 and merged["training_hour_end"] < merged["training_hour_start"]:
            merged["training_hour_end"] = int(merged["training_hour_start"])
        merged["full_data_mode"] = _as_bool(merged.get("full_data_mode", True), True)
        merged["strict_full_windows_mode"] = _as_bool(merged.get("strict_full_windows_mode", True), True)
        merged["label_guard_enabled"] = _as_bool(merged.get("label_guard_enabled", True), True)
        dead_feature_defaults = {
            "liq_events_60s",
            "liq_count_60s",
            "liq_notional_60s",
            "liq_buy_sell_imbalance",
            "liq_momentum",
            "open_interest_change_pct",
            "oi_velocity",
            "hour_of_day_sin",
            "hour_of_day_cos",
            "session_asia_eu_us",
            "funding_rate",
        }
        raw_disabled_features = merged.get("disabled_training_features", [])
        disabled_features = set()
        if isinstance(raw_disabled_features, (list, tuple)):
            disabled_features = {str(item).strip() for item in raw_disabled_features if str(item).strip()}
        merged["disabled_training_features"] = sorted(disabled_features.union(dead_feature_defaults))
        training = merged["training"]
        training["epochs"] = max(1, int(training.get("epochs", 8)))
        training["batch_size"] = max(1, int(training.get("batch_size", 64)))
        training["learning_rate"] = float(training.get("learning_rate", 0.001))
        training["lookback_steps"] = max(10, int(training.get("lookback_steps", 60)))
        training["hidden_size"] = max(16, int(training.get("hidden_size", 128)))
        training["num_layers"] = max(1, int(training.get("num_layers", 2)))
        training["dropout"] = float(training.get("dropout", 0.2))
        model_type = str(training.get("model_type", "lstm_attention") or "lstm_attention").strip().lower()
        training["model_type"] = model_type if model_type in {"lstm_attention", "tcn", "transformer_encoder"} else "lstm_attention"
        training["warm_start_from_current"] = _as_bool(training.get("warm_start_from_current", False), False)
        training["use_class_weighted_loss"] = _as_bool(training.get("use_class_weighted_loss", False), False)
        mode = str(training.get("class_weight_mode", "auto") or "auto").strip().lower()
        training["class_weight_mode"] = "manual" if mode == "manual" else "auto"
        training["class_weight_down"] = max(0.0, float(training.get("class_weight_down", 1.0)))
        training["class_weight_flat"] = max(0.0, float(training.get("class_weight_flat", 1.0)))
        training["class_weight_up"] = max(0.0, float(training.get("class_weight_up", 1.0)))
        training["use_focal_loss"] = _as_bool(training.get("use_focal_loss", False), False)
        training["focal_gamma"] = max(0.0, float(training.get("focal_gamma", 2.0)))
        training["focal_use_alpha_class_weights"] = _as_bool(
            training.get("focal_use_alpha_class_weights", True),
            True,
        )
        training["use_balanced_sampler"] = _as_bool(training.get("use_balanced_sampler", False), False)
        training["use_class_quota_batches"] = _as_bool(training.get("use_class_quota_batches", False), False)
        training["class_quota_per_batch"] = max(0, int(training.get("class_quota_per_batch", 4)))
        training["label_smoothing"] = max(0.0, min(0.2, float(training.get("label_smoothing", 0.03))))
        training["epoch_trace_capture_list"] = _normalize_epoch_trace_capture_list(
            training.get("epoch_trace_capture_list", ""),
            training["epochs"],
        )
        training["capture_trace_train"] = _as_bool(training.get("capture_trace_train", False), False)
        training["capture_trace_val"] = _as_bool(training.get("capture_trace_val", False), False)
        training["capture_trace_test"] = _as_bool(training.get("capture_trace_test", False), False)
        monitor = str(training.get("early_stopping_monitor", "macro_f1") or "macro_f1").strip().lower()
        training["early_stopping_monitor"] = "macro_f1" if monitor == "macro_f1" else "val_loss"
        training["early_stopping_enabled"] = _as_bool(training.get("early_stopping_enabled", False), False)
        training["early_stopping_patience"] = max(1, int(training.get("early_stopping_patience", 1)))
        training["early_stopping_min_delta"] = max(0.0, float(training.get("early_stopping_min_delta", 0.0)))
        paper = merged["paper_bot"]
        paper["enabled"] = bool(paper.get("enabled", False))
        gate_mode = str(paper.get("gate_mode", "confidence_only") or "confidence_only").strip().lower()
        paper["gate_mode"] = gate_mode if gate_mode in {"confidence_only", "combined"} else "confidence_only"
        gate_profile = str(paper.get("gate_profile", "balanced") or "balanced").strip().lower()
        paper["gate_profile"] = gate_profile if gate_profile in {"strict", "balanced", "permissive"} else "balanced"
        paper["confidence_threshold"] = float(paper.get("confidence_threshold", 0.72))
        paper["entry_confidence_threshold"] = max(0.0, min(1.0, float(paper.get("entry_confidence_threshold", paper["confidence_threshold"]))))
        paper["exit_confidence_threshold"] = max(0.0, min(1.0, float(paper.get("exit_confidence_threshold", 0.55))))
        if paper["exit_confidence_threshold"] > paper["entry_confidence_threshold"]:
            paper["exit_confidence_threshold"] = paper["entry_confidence_threshold"]
        paper["minimum_directional_edge"] = max(0.0, min(1.0, float(paper.get("minimum_directional_edge", 0.03))))
        paper["temperature_scaling_enabled"] = _as_bool(paper.get("temperature_scaling_enabled", True), True)
        paper["minimum_action_rate"] = max(0.0, min(1.0, float(paper.get("minimum_action_rate", 0.03))))
        paper["minimum_directional_samples"] = max(1, int(paper.get("minimum_directional_samples", 30)))
        paper["minimum_precision_directional"] = max(0.0, min(1.0, float(paper.get("minimum_precision_directional", 0.0))))
        paper["max_false_long_rate"] = max(0.0, min(1.0, float(paper.get("max_false_long_rate", 0.60))))
        paper["max_false_short_rate"] = max(0.0, min(1.0, float(paper.get("max_false_short_rate", 0.60))))
        paper["min_long_precision"] = max(0.0, min(1.0, float(paper.get("min_long_precision", 0.45))))
        paper["min_short_precision"] = max(0.0, min(1.0, float(paper.get("min_short_precision", 0.45))))
        paper["min_long_when_actual_up"] = max(0.0, min(1.0, float(paper.get("min_long_when_actual_up", 0.20))))
        paper["min_short_when_actual_down"] = max(0.0, min(1.0, float(paper.get("min_short_when_actual_down", 0.20))))
        paper["minimum_directional_trades_per_side"] = max(1, int(paper.get("minimum_directional_trades_per_side", 30)))
        paper["estimated_roundtrip_cost_bps"] = max(0.0, float(paper.get("estimated_roundtrip_cost_bps", 6.0)))
        paper["max_signal_age_ms"] = max(0, int(float(paper.get("max_signal_age_ms", 2000))))
        paper["volatility_guard_enabled"] = _as_bool(paper.get("volatility_guard_enabled", False), False)
        paper["max_realized_volatility"] = max(0.0, float(paper.get("max_realized_volatility", 0.02)))
        vol_action = str(paper.get("volatility_guard_action", "hold_only") or "hold_only").strip().lower()
        paper["volatility_guard_action"] = vol_action if vol_action in {"hold_only", "raise_threshold_multiplier"} else "hold_only"
        paper["max_spread_bps"] = max(0.0, float(paper.get("max_spread_bps", 15.0)))
        paper["min_orderbook_depth"] = max(0.0, float(paper.get("min_orderbook_depth", 0.0)))
        cal_action = str(paper.get("live_calibration_guard_action", "monitor_only") or "monitor_only").strip().lower()
        paper["live_calibration_guard_action"] = cal_action if cal_action in {"monitor_only", "block_entries"} else "monitor_only"
        paper["max_trades_per_hour"] = max(0, int(paper.get("max_trades_per_hour", 30)))
        paper["max_consecutive_entries"] = max(0, int(paper.get("max_consecutive_entries", 5)))
        paper["min_seconds_between_entries"] = max(0, int(paper.get("min_seconds_between_entries", 5)))
        paper["enable_side_threshold_sweep"] = _as_bool(paper.get("enable_side_threshold_sweep", False), False)
        def _norm_thr_list(raw: Any) -> list[float]:
            vals: list[float] = []
            if isinstance(raw, (list, tuple)):
                for item in raw:
                    try:
                        vals.append(float(item))
                    except Exception:
                        continue
            if not vals:
                vals = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
            cleaned = sorted({max(0.0, min(1.0, float(v))) for v in vals})
            return [float(v) for v in cleaned]
        paper["long_confidence_thresholds"] = _norm_thr_list(
            paper.get("long_confidence_thresholds", [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80])
        )
        paper["short_confidence_thresholds"] = _norm_thr_list(
            paper.get("short_confidence_thresholds", [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80])
        )
        paper["quality_threshold"] = float(paper.get("quality_threshold", 0.72))
        paper["min_trade_rate_floor"] = max(0.0, min(1.0, float(paper.get("min_trade_rate_floor", 0.01))))
        paper["min_flat_rate_required"] = max(0.0, min(1.0, float(paper.get("min_flat_rate_required", 0.12))))
        paper["max_label_price_rejected_rate"] = max(0.0, min(1.0, float(paper.get("max_label_price_rejected_rate", 0.005))))
        paper["fee_bps_round_trip"] = max(0.0, float(paper.get("fee_bps_round_trip", 4.0)))
        paper["slippage_bps_round_trip"] = max(0.0, float(paper.get("slippage_bps_round_trip", 2.0)))
        paper["min_samples_per_bucket"] = max(1, int(paper.get("min_samples_per_bucket", 200)))
        paper["poll_delay_ms"] = max(100, min(20_000, int(float(paper.get("poll_delay_ms", 2000)))))
        paper["max_position_qty"] = float(paper.get("max_position_qty", 1.0))
        paper["max_hold_seconds"] = max(10, int(paper.get("max_hold_seconds", 120)))
        paper["leverage"] = max(1.0, float(paper.get("leverage", 5.0)))
        paper["commission_fee_pct"] = max(0.0, float(paper.get("commission_fee_pct", 0.04)))
        paper["one_trade_at_time"] = _as_bool(paper.get("one_trade_at_time", True), True)
        paper["initial_balance"] = max(1.0, float(paper.get("initial_balance", 100.0)))
        paper["take_profit_pct"] = max(0.0, float(paper.get("take_profit_pct", 0.25)))
        paper["stop_loss_pct"] = max(0.0, float(paper.get("stop_loss_pct", 0.2)))
        paper["use_trailing_stop"] = _as_bool(paper.get("use_trailing_stop", False), False)
        paper["trailing_stop_pct"] = max(0.0, float(paper.get("trailing_stop_pct", 0.15)))
        paper["regime_filter_enabled"] = _as_bool(paper.get("regime_filter_enabled", True), True)
        paper["max_spread_pct"] = max(0.0, float(paper.get("max_spread_pct", 0.15)))
        paper["min_trade_rate_10s"] = max(0.0, float(paper.get("min_trade_rate_10s", 3.0)))
        paper["min_confidence_gap"] = max(0.0, float(paper.get("min_confidence_gap", 0.06)))
        return merged

    def _profile_from_row(self, pair_symbol: str, config: dict[str, Any], updated_at: str) -> MlProfile:
        return MlProfile(
            pair_symbol=pair_symbol.upper(),
            selected_exchanges=[str(item) for item in config["selected_exchanges"]],
            training_selected_exchanges=[str(item) for item in config.get("training_selected_exchanges", [])],
            bot_data_selected_exchanges=[str(item) for item in config.get("bot_data_selected_exchanges", [])],
            bot_execution_selected_exchanges=[str(item) for item in config.get("bot_execution_selected_exchanges", [])],
            bot_signal_mode=str(config.get("bot_signal_mode", "combined")),
            execution_position_mode=str(config.get("execution_position_mode", "combined_position")),
            use_local_data=bool(config.get("use_local_data", True)),
            use_historic_data=bool(config["use_historic_data"]),
            historic_data_days=int(config["historic_data_days"]),
            horizons=[int(item) for item in config["horizons"]],
            label_threshold_pct=float(config["label_threshold_pct"]),
            min_data_hours=float(config["min_data_hours"]),
            normalizer_window=int(config["normalizer_window"]),
            replay_candle_limit=int(config.get("replay_candle_limit", 240)),
            training_hour_window_enabled=bool(config.get("training_hour_window_enabled", False)),
            training_hour_start=max(0, int(config.get("training_hour_start", 0))),
            training_hour_end=max(0, int(config.get("training_hour_end", 0))),
            full_data_mode=bool(config.get("full_data_mode", True)),
            strict_full_windows_mode=bool(config.get("strict_full_windows_mode", True)),
            disabled_training_features=[str(item) for item in config.get("disabled_training_features", [])],
            target_mode=str(config.get("target_mode", "triple_barrier")),
            triple_barrier=dict(config.get("triple_barrier", {"tp_pct": 0.08, "sl_pct": 0.05, "timeout_steps": 180})),
            barrier_debug=dict(
                config.get(
                    "barrier_debug",
                    {
                        "enable_future_path_probe": True,
                        "future_path_probe_samples": 5,
                        "future_path_probe_depth": 20,
                        "step_contiguity_tolerance_ms": 500,
                        "timeout_unit_hint": "steps",
                    },
                )
            ),
            training=dict(config["training"]),
            paper_bot=dict(config["paper_bot"]),
            updated_at=updated_at,
        )

    def _run_from_row(self, row: Any) -> dict[str, Any]:
        return {
            "run_id": str(row[0]),
            "pair_symbol": str(row[1]),
            "instance_id": str(row[2] or ""),
            "status": str(row[3]),
            "stage": str(row[4]),
            "stage_progress": float(row[5] or 0.0),
            "device": str(row[6]),
            "created_at": str(row[7]),
            "updated_at": str(row[8]),
            "started_at": str(row[9]) if row[9] is not None else None,
            "ended_at": str(row[10]) if row[10] is not None else None,
            "config": json.loads(str(row[11] or "{}")),
            "metrics": json.loads(str(row[12] or "{}")),
            "error_text": str(row[13] or ""),
        }
