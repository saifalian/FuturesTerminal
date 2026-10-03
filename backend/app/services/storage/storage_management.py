from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _guess_pair_from_name(name: str) -> str:
    head = str(name).split("@", 1)[0].strip().upper()
    if head.endswith("USDT") and head.isalnum():
        return head
    return "UNSCOPED"


def _within(path: Path, root: Path) -> bool:
    try:
        path_resolved = path.resolve(strict=False)
        root_resolved = root.resolve(strict=False)
    except Exception:
        return False
    try:
        return root_resolved == path_resolved or root_resolved in path_resolved.parents
    except Exception:
        return False


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except Exception:
            return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except Exception:
                continue
    return total


@dataclass(slots=True)
class StorageEntry:
    entry_id: str
    category_id: str
    pair_symbol: str
    name: str
    type: str
    path: str
    size_bytes: int
    updated_at: str
    delete_target: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "category_id": self.category_id,
            "pair_symbol": self.pair_symbol,
            "name": self.name,
            "type": self.type,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "updated_at": self.updated_at,
            "delete_target": self.delete_target,
        }


class StorageManagementService:
    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.project_root: Path = settings.project_root
        self.terminal_db: Path = settings.sqlite_path
        self.ml_db: Path = settings.ml_sqlite_path
        self.replay_dir: Path = settings.replay_dir
        self.market_events_dir: Path = settings.market_events_dir
        self.ml_features_dir: Path = settings.ml_features_dir
        self.models_root: Path = settings.models_root

    def _allowed_roots(self) -> list[Path]:
        return [
            self.replay_dir,
            self.market_events_dir,
            self.ml_features_dir,
            self.models_root,
            self.terminal_db.parent,
            self.ml_db.parent,
        ]

    def _is_allowed_path(self, path: Path) -> bool:
        return any(_within(path, root) for root in self._allowed_roots())

    def _collect_path_entries(self, *, category_id: str, base: Path, pair_from_root: bool = False) -> list[StorageEntry]:
        entries: list[StorageEntry] = []
        if not base.exists():
            return entries
        for child in sorted(base.iterdir(), key=lambda p: (p.is_file(), p.name.lower())):
            is_dir = child.is_dir()
            pair_symbol = child.name.upper() if pair_from_root and is_dir else "UNSCOPED"
            size_bytes = _dir_size(child)
            mtime = ""
            try:
                mtime = str(int(child.stat().st_mtime))
            except Exception:
                mtime = ""
            entry_id = f"{category_id}:path:{str(child.resolve(strict=False)).lower()}"
            entries.append(
                StorageEntry(
                    entry_id=entry_id,
                    category_id=category_id,
                    pair_symbol=pair_symbol,
                    name=child.name,
                    type="folder" if is_dir else "file",
                    path=str(child.resolve(strict=False)),
                    size_bytes=size_bytes,
                    updated_at=mtime,
                    delete_target={"kind": "path", "path": str(child.resolve(strict=False))},
                )
            )
        return entries

    def _collect_live_entries(self) -> list[StorageEntry]:
        entries: list[StorageEntry] = []
        if self.terminal_db.exists():
            size = _dir_size(self.terminal_db)
            mtime = str(int(self.terminal_db.stat().st_mtime))
            entries.append(
                StorageEntry(
                    entry_id="live_recorded_data:db_group:raw_events",
                    category_id="live_recorded_data",
                    pair_symbol="UNSCOPED",
                    name="terminal.db/raw_events",
                    type="db_group",
                    path=str(self.terminal_db.resolve(strict=False)),
                    size_bytes=size,
                    updated_at=mtime,
                    delete_target={"kind": "db_group", "db": "terminal", "group": "raw_events"},
                )
            )
            # lightweight pair visibility by stream head
            # NOTE:
            # Avoid full-table GROUP BY on very large raw_events tables because it can freeze
            # Storage Management for minutes. We only sample recent rows for pair discovery.
            try:
                with sqlite3.connect(self.terminal_db, timeout=1.5) as conn:
                    cur = conn.execute(
                        """
                        SELECT stream
                        FROM raw_events
                        ORDER BY id DESC
                        LIMIT 50000
                        """
                    )
                    pairs: set[str] = set()
                    for (stream,) in cur.fetchall():
                        pair = _guess_pair_from_name(str(stream or ""))
                        if pair and pair != "UNSCOPED":
                            pairs.add(pair)
                    for pair in sorted(pairs):
                        entries.append(
                            StorageEntry(
                                entry_id=f"live_recorded_data:db_group:raw_events:{pair}",
                                category_id="live_recorded_data",
                                pair_symbol=pair,
                                name=f"raw_events ({pair})",
                                type="db_group",
                                path=str(self.terminal_db.resolve(strict=False)),
                                size_bytes=0,
                                updated_at=mtime,
                                delete_target={"kind": "db_group", "db": "terminal", "group": "raw_events", "pair_symbol": pair},
                            )
                        )
            except Exception:
                pass
        return entries

    def _collect_market_event_entries(self) -> list[StorageEntry]:
        entries: list[StorageEntry] = []
        if not self.terminal_db.exists():
            return entries
        try:
            with sqlite3.connect(self.terminal_db, timeout=1.5) as conn:
                rows = conn.execute(
                    """
                    SELECT pair_symbol, source_exchange_id, date_key, hour_key, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality
                    FROM market_event_chunks
                    ORDER BY ts_end_ms DESC
                    LIMIT 50000
                    """
                ).fetchall()
            for pair_symbol, exchange_id, date_key, hour_key, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality in rows:
                path = Path(str(file_path or "")).resolve(strict=False)
                if not str(path):
                    continue
                if not path.exists():
                    continue
                pair = str(pair_symbol or "").upper() or "UNSCOPED"
                exchange = str(exchange_id or "unknown").lower()
                size = int(size_bytes or 0)
                if size <= 0:
                    size = _dir_size(path)
                ts_end = str(int(ts_end_ms or 0))
                quality = str(dataset_quality or capture_mode or "unknown")
                name = f"{exchange} {date_key} {hour_key}:00 ({int(row_count or 0)} events, {quality})"
                entry_id = f"market_event_recordings:path:{str(path).lower()}"
                entries.append(
                    StorageEntry(
                        entry_id=entry_id,
                        category_id="market_event_recordings",
                        pair_symbol=pair,
                        name=name,
                        type="parquet_chunk",
                        path=str(path),
                        size_bytes=max(0, size),
                        updated_at=ts_end,
                        delete_target={"kind": "path", "path": str(path)},
                    )
                )
        except Exception:
            return entries
        return entries

    def _collect_ml_db_groups(self) -> tuple[list[StorageEntry], list[StorageEntry], list[StorageEntry]]:
        bot_entries: list[StorageEntry] = []
        training_entries: list[StorageEntry] = []
        session_entries: list[StorageEntry] = []
        if not self.ml_db.exists():
            return bot_entries, training_entries, session_entries
        mtime = str(int(self.ml_db.stat().st_mtime))
        db_path = str(self.ml_db.resolve(strict=False))
        db_size = _dir_size(self.ml_db)
        try:
            with sqlite3.connect(self.ml_db) as conn:
                pair_rows = conn.execute(
                    "SELECT pair_symbol, COUNT(*) FROM ml_runs GROUP BY pair_symbol ORDER BY pair_symbol"
                ).fetchall()
                for pair_symbol, count in pair_rows:
                    pair = str(pair_symbol or "").upper() or "UNSCOPED"
                    training_entries.append(
                        StorageEntry(
                            entry_id=f"training_runs_models:db_group:runs:{pair}",
                            category_id="training_runs_models",
                            pair_symbol=pair,
                            name=f"Training runs ({pair})",
                            type="db_group",
                            path=db_path,
                            size_bytes=0,
                            updated_at=mtime,
                            delete_target={"kind": "db_group", "db": "ml", "group": "training_runs", "pair_symbol": pair},
                        )
                    )
                bot_rows = conn.execute(
                    "SELECT pair_symbol, COUNT(*) FROM ml_bot_trades GROUP BY pair_symbol ORDER BY pair_symbol"
                ).fetchall()
                for pair_symbol, count in bot_rows:
                    pair = str(pair_symbol or "").upper() or "UNSCOPED"
                    bot_entries.append(
                        StorageEntry(
                            entry_id=f"bot_data:db_group:trades:{pair}",
                            category_id="bot_data",
                            pair_symbol=pair,
                            name=f"Bot trades ({pair})",
                            type="db_group",
                            path=db_path,
                            size_bytes=0,
                            updated_at=mtime,
                            delete_target={"kind": "db_group", "db": "ml", "group": "bot_data", "pair_symbol": pair},
                        )
                    )
                session_rows = conn.execute(
                    """
                    SELECT i.pair_symbol, COUNT(*)
                    FROM ml_bot_trade_sessions s
                    LEFT JOIN ml_instances i ON i.instance_id = s.instance_id
                    GROUP BY i.pair_symbol
                    ORDER BY i.pair_symbol
                    """
                ).fetchall()
                for pair_symbol, count in session_rows:
                    pair = str(pair_symbol or "").upper() or "UNSCOPED"
                    session_entries.append(
                        StorageEntry(
                            entry_id=f"sessions:db_group:bot_sessions:{pair}",
                            category_id="sessions",
                            pair_symbol=pair,
                            name=f"Bot sessions ({pair})",
                            type="db_group",
                            path=db_path,
                            size_bytes=0,
                            updated_at=mtime,
                            delete_target={"kind": "db_group", "db": "ml", "group": "sessions", "pair_symbol": pair},
                        )
                    )
                training_entries.insert(
                    0,
                    StorageEntry(
                        entry_id="training_runs_models:db_group:all",
                        category_id="training_runs_models",
                        pair_symbol="UNSCOPED",
                        name="Training DB groups (all pairs)",
                        type="db_group",
                        path=db_path,
                        size_bytes=db_size,
                        updated_at=mtime,
                        delete_target={"kind": "db_group", "db": "ml", "group": "training_runs"},
                    ),
                )
                bot_entries.insert(
                    0,
                    StorageEntry(
                        entry_id="bot_data:db_group:all",
                        category_id="bot_data",
                        pair_symbol="UNSCOPED",
                        name="Bot DB groups (all pairs)",
                        type="db_group",
                        path=db_path,
                        size_bytes=0,
                        updated_at=mtime,
                        delete_target={"kind": "db_group", "db": "ml", "group": "bot_data"},
                    ),
                )
                session_entries.insert(
                    0,
                    StorageEntry(
                        entry_id="sessions:db_group:all",
                        category_id="sessions",
                        pair_symbol="UNSCOPED",
                        name="Session DB groups (all pairs)",
                        type="db_group",
                        path=db_path,
                        size_bytes=0,
                        updated_at=mtime,
                        delete_target={"kind": "db_group", "db": "ml", "group": "sessions"},
                    ),
                )
        except Exception:
            pass
        return bot_entries, training_entries, session_entries

    def overview(self) -> dict[str, Any]:
        categories: list[dict[str, Any]] = []

        replay_entries = self._collect_path_entries(category_id="replay_sessions", base=self.replay_dir, pair_from_root=False)
        market_event_entries = self._collect_market_event_entries()
        feature_entries = self._collect_path_entries(category_id="training_features", base=self.ml_features_dir, pair_from_root=True)
        model_entries = self._collect_path_entries(category_id="training_runs_models", base=self.models_root, pair_from_root=True)
        history_entries: list[StorageEntry] = []
        if self.models_root.exists():
            for pair_dir in sorted([p for p in self.models_root.iterdir() if p.is_dir()]):
                hist_dir = pair_dir / "historic_data_acquired"
                if not hist_dir.exists():
                    continue
                history_entries.extend(self._collect_path_entries(category_id="history_backfill", base=hist_dir, pair_from_root=False))
                for item in history_entries:
                    if item.pair_symbol == "UNSCOPED":
                        item.pair_symbol = pair_dir.name.upper()

        live_entries = self._collect_live_entries()
        bot_entries, training_db_entries, session_entries = self._collect_ml_db_groups()

        # merge model path entries with training db logical entries
        training_entries = model_entries + training_db_entries

        grouped = [
            ("live_recorded_data", "Live Recorded Data", live_entries),
            ("market_event_recordings", "Market Event Recordings (Full Fidelity)", market_event_entries),
            ("replay_sessions", "Replay Sessions", replay_entries),
            ("training_features", "Training Features", feature_entries),
            ("training_runs_models", "Training Runs & Models", training_entries),
            ("bot_data", "Bot Data", bot_entries),
            ("history_backfill", "Acquired Historic Data (Per Pair)", history_entries),
            ("sessions", "Sessions", session_entries),
        ]

        total_bytes = 0
        total_entries = 0
        for category_id, label, entries in grouped:
            entries_dict = [entry.to_dict() for entry in entries]
            cat_bytes = sum(max(0, _safe_int(item.get("size_bytes"), 0)) for item in entries_dict)
            total_bytes += cat_bytes
            total_entries += len(entries_dict)
            categories.append(
                {
                    "category_id": category_id,
                    "label": label,
                    "entry_count": len(entries_dict),
                    "size_bytes": cat_bytes,
                    "entries": entries_dict,
                }
            )

        return {
            "totals": {
                "category_count": len(categories),
                "entry_count": total_entries,
                "size_bytes": total_bytes,
            },
            "categories": categories,
        }

    def _cleanup_market_event_manifest_for_path(self, path: Path, *, is_dir_target: bool) -> None:
        if not self.terminal_db.exists():
            return
        try:
            resolved = str(path.resolve(strict=False))
            prefix = resolved if (not is_dir_target) else f"{resolved}%"
            with sqlite3.connect(self.terminal_db) as conn:
                if not is_dir_target:
                    conn.execute("DELETE FROM market_event_chunks WHERE file_path = ?", (resolved,))
                else:
                    conn.execute("DELETE FROM market_event_chunks WHERE file_path LIKE ?", (prefix,))
                conn.commit()
        except Exception:
            return

    def _delete_db_group(self, target: dict[str, Any]) -> tuple[int, str | None]:
        db_name = str(target.get("db", "")).strip().lower()
        group = str(target.get("group", "")).strip().lower()
        pair_symbol = str(target.get("pair_symbol", "")).strip().upper()
        db_path = self.terminal_db if db_name == "terminal" else self.ml_db
        if not db_path.exists():
            return 0, None
        before = _dir_size(db_path)
        with sqlite3.connect(db_path) as conn:
            if db_name == "terminal" and group == "raw_events":
                if pair_symbol and pair_symbol != "UNSCOPED":
                    prefix = pair_symbol.lower()
                    conn.execute("DELETE FROM raw_events WHERE stream LIKE ?", (f"{prefix}@%",))
                else:
                    conn.execute("DELETE FROM raw_events")
            elif db_name == "terminal" and group == "market_event_chunks":
                if pair_symbol and pair_symbol != "UNSCOPED":
                    rows = conn.execute(
                        "SELECT file_path FROM market_event_chunks WHERE pair_symbol = ?",
                        (pair_symbol,),
                    ).fetchall()
                    for (file_path,) in rows:
                        try:
                            Path(str(file_path)).unlink(missing_ok=True)
                        except Exception:
                            continue
                    conn.execute("DELETE FROM market_event_chunks WHERE pair_symbol = ?", (pair_symbol,))
                else:
                    rows = conn.execute("SELECT file_path FROM market_event_chunks").fetchall()
                    for (file_path,) in rows:
                        try:
                            Path(str(file_path)).unlink(missing_ok=True)
                        except Exception:
                            continue
                    conn.execute("DELETE FROM market_event_chunks")
            elif db_name == "ml" and group == "training_runs":
                if pair_symbol and pair_symbol != "UNSCOPED":
                    run_rows = conn.execute("SELECT run_id FROM ml_runs WHERE pair_symbol = ?", (pair_symbol,)).fetchall()
                    run_ids = [str(row[0]) for row in run_rows]
                    for run_id in run_ids:
                        conn.execute("DELETE FROM ml_run_logs WHERE run_id = ?", (run_id,))
                        conn.execute("DELETE FROM ml_run_epoch_metrics WHERE run_id = ?", (run_id,))
                        conn.execute("DELETE FROM ml_run_prediction_trace WHERE run_id = ?", (run_id,))
                        conn.execute("DELETE FROM ml_models WHERE run_id = ?", (run_id,))
                    conn.execute("DELETE FROM ml_runs WHERE pair_symbol = ?", (pair_symbol,))
                    conn.execute("DELETE FROM ml_feature_manifests WHERE pair_symbol = ?", (pair_symbol,))
                else:
                    conn.execute("DELETE FROM ml_run_logs")
                    conn.execute("DELETE FROM ml_run_epoch_metrics")
                    conn.execute("DELETE FROM ml_run_prediction_trace")
                    conn.execute("DELETE FROM ml_models")
                    conn.execute("DELETE FROM ml_runs")
                    conn.execute("DELETE FROM ml_feature_manifests")
            elif db_name == "ml" and group == "bot_data":
                if pair_symbol and pair_symbol != "UNSCOPED":
                    instance_rows = conn.execute("SELECT instance_id FROM ml_instances WHERE pair_symbol = ?", (pair_symbol,)).fetchall()
                    instance_ids = [str(row[0]) for row in instance_rows]
                    conn.execute("DELETE FROM ml_bot_state WHERE pair_symbol = ?", (pair_symbol,))
                    conn.execute("DELETE FROM ml_bot_trades WHERE pair_symbol = ?", (pair_symbol,))
                    for instance_id in instance_ids:
                        conn.execute("DELETE FROM ml_instance_bot_state WHERE instance_id = ?", (instance_id,))
                        conn.execute("DELETE FROM ml_bot_inference_points WHERE instance_id = ?", (instance_id,))
                else:
                    conn.execute("DELETE FROM ml_bot_state")
                    conn.execute("DELETE FROM ml_bot_trades")
                    conn.execute("DELETE FROM ml_instance_bot_state")
                    conn.execute("DELETE FROM ml_bot_inference_points")
            elif db_name == "ml" and group == "sessions":
                if pair_symbol and pair_symbol != "UNSCOPED":
                    instance_rows = conn.execute("SELECT instance_id FROM ml_instances WHERE pair_symbol = ?", (pair_symbol,)).fetchall()
                    instance_ids = [str(row[0]) for row in instance_rows]
                    for instance_id in instance_ids:
                        conn.execute("DELETE FROM ml_bot_trade_sessions WHERE instance_id = ?", (instance_id,))
                        conn.execute("DELETE FROM ml_bot_trades WHERE instance_id = ?", (instance_id,))
                else:
                    conn.execute("DELETE FROM ml_bot_trade_sessions")
                    conn.execute("DELETE FROM ml_bot_trades")
            else:
                raise ValueError(f"Unsupported db group target: {db_name}:{group}")
            conn.commit()
            try:
                # VACUUM can take a long time on large DBs; keep pair-scoped raw_event deletes responsive.
                should_vacuum = not (db_name == "terminal" and group == "raw_events" and pair_symbol and pair_symbol != "UNSCOPED")
                if should_vacuum:
                    conn.execute("VACUUM")
            except Exception:
                pass
        after = _dir_size(db_path)
        reclaimed = max(0, before - after)
        return reclaimed, None

    def delete_targets(self, targets: list[dict[str, Any]]) -> dict[str, Any]:
        deleted_count = 0
        failed_count = 0
        reclaimed_bytes = 0
        failures: list[dict[str, str]] = []
        per_target_results: list[dict[str, Any]] = []

        for item in targets:
            kind = str(item.get("kind", "")).strip().lower()
            result = {"target": item, "ok": False, "reclaimed_bytes": 0, "error": ""}
            try:
                if kind == "path":
                    raw_path = str(item.get("path", "")).strip()
                    if not raw_path:
                        raise ValueError("Missing path target")
                    path = Path(raw_path).resolve(strict=False)
                    if not self._is_allowed_path(path):
                        raise ValueError(f"Path is out of allowed scope: {raw_path}")
                    if not path.exists():
                        result["ok"] = True
                        per_target_results.append(result)
                        deleted_count += 1
                        continue
                    before = _dir_size(path)
                    is_dir_target = path.is_dir()
                    if is_dir_target:
                        shutil.rmtree(path)
                    else:
                        path.unlink(missing_ok=True)
                    if _within(path, self.market_events_dir):
                        self._cleanup_market_event_manifest_for_path(path, is_dir_target=is_dir_target)
                    reclaimed = max(0, before)
                    result["ok"] = True
                    result["reclaimed_bytes"] = reclaimed
                    reclaimed_bytes += reclaimed
                    deleted_count += 1
                elif kind == "db_group":
                    reclaimed, _ = self._delete_db_group(item)
                    result["ok"] = True
                    result["reclaimed_bytes"] = reclaimed
                    reclaimed_bytes += reclaimed
                    deleted_count += 1
                else:
                    raise ValueError(f"Unsupported delete kind: {kind}")
            except Exception as exc:
                failed_count += 1
                result["error"] = str(exc)
                failures.append({"target": json.dumps(item, ensure_ascii=False), "error": str(exc)})
            per_target_results.append(result)

        return {
            "deleted_count": deleted_count,
            "failed_count": failed_count,
            "reclaimed_bytes": reclaimed_bytes,
            "failures": failures,
            "per_target_results": per_target_results,
        }
