from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.core.time_utils import utc_now_iso
from app.services.ml.repository import MlRepository

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _HAS_PYARROW = True
except Exception:
    pa = None
    pq = None
    _HAS_PYARROW = False


@dataclass(slots=True)
class _BufferEntry:
    rows: list[dict[str, Any]]
    first_ts_ms: int
    last_ts_ms: int


class MlFeatureLogger:
    def __init__(
        self,
        *,
        tab_sessions: Any,
        repository: MlRepository,
        features_root: Path,
        schema_version: str = "v1",
        snapshot_interval_seconds: float = 1.0,
        flush_interval_seconds: float = 5.0,
        retention_days: int = 90,
    ) -> None:
        self._tab_sessions = tab_sessions
        self._repository = repository
        self._features_root = features_root
        self._schema_version = schema_version
        self._snapshot_interval_seconds = snapshot_interval_seconds
        self._flush_interval_seconds = flush_interval_seconds
        self._retention_days = retention_days
        self._stop = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._retention_task: asyncio.Task | None = None
        self._buffers: dict[tuple[str, str], _BufferEntry] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._loop_task and not self._loop_task.done():
            return
        self._features_root.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._loop_task = asyncio.create_task(self._run_capture_loop(), name="ml-feature-capture")
        self._flush_task = asyncio.create_task(self._run_flush_loop(), name="ml-feature-flush")
        self._retention_task = asyncio.create_task(self._run_retention_loop(), name="ml-feature-retention")
        logger.info("ML feature logger started: root=%s interval=%ss", self._features_root, self._snapshot_interval_seconds)

    async def stop(self) -> None:
        self._stop.set()
        tasks = [task for task in (self._loop_task, self._flush_task, self._retention_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._loop_task = None
        self._flush_task = None
        self._retention_task = None
        await self.flush_all()
        logger.info("ML feature logger stopped")

    async def _run_capture_loop(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                snapshots = await self._tab_sessions.collect_ml_snapshots()
                await self._append_snapshots(snapshots)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("ML logger capture loop error: %s", exc)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.05, self._snapshot_interval_seconds - elapsed))

    async def _append_snapshots(self, snapshots: list[dict[str, Any]]) -> None:
        if not snapshots:
            return
        async with self._lock:
            for item in snapshots:
                pair_symbol = str(item.get("pair_symbol", "")).upper()
                tab_id = str(item.get("tab_id", "default"))
                ts_ms = int(item.get("ts_ms", int(time.time() * 1000)))
                if not pair_symbol:
                    continue
                date_key = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                key = (pair_symbol, date_key)
                entry = self._buffers.get(key)
                if entry is None:
                    entry = _BufferEntry(rows=[], first_ts_ms=ts_ms, last_ts_ms=ts_ms)
                    self._buffers[key] = entry
                entry.rows.append(item)
                entry.first_ts_ms = min(entry.first_ts_ms, ts_ms)
                entry.last_ts_ms = max(entry.last_ts_ms, ts_ms)
                # Keep tab_id captured as a field, but key by pair/date for larger parquet blocks.
                item["tab_id"] = tab_id

    async def _run_flush_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self._flush_interval_seconds)
                await self.flush_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("ML logger flush loop error: %s", exc)

    async def flush_all(self) -> None:
        pending: list[tuple[tuple[str, str], _BufferEntry]] = []
        async with self._lock:
            for key, value in self._buffers.items():
                if value.rows:
                    pending.append((key, value))
            self._buffers.clear()
        for key, entry in pending:
            pair_symbol, date_key = key
            try:
                await self._write_parquet(pair_symbol, date_key, entry)
            except Exception as exc:
                logger.exception("ML logger write failed for %s %s: %s", pair_symbol, date_key, exc)

    async def _write_parquet(self, pair_symbol: str, date_key: str, entry: _BufferEntry) -> None:
        rows = entry.rows
        if not rows:
            return
        by_tab: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_tab[str(row.get("tab_id", "default"))].append(row)
        pair_dir = self._features_root / pair_symbol / f"date={date_key}"
        pair_dir.mkdir(parents=True, exist_ok=True)
        for tab_id, tab_rows in by_tab.items():
            if _HAS_PYARROW:
                file_name = f"part_{int(time.time() * 1000)}_{uuid4().hex[:8]}.parquet"
            else:
                file_name = f"part_{int(time.time() * 1000)}_{uuid4().hex[:8]}.jsonl"
            file_path = pair_dir / file_name
            if _HAS_PYARROW:
                table = pa.Table.from_pylist(tab_rows)
                pq.write_table(table, file_path, compression="zstd")
            else:
                with file_path.open("w", encoding="utf-8") as handle:
                    for row in tab_rows:
                        handle.write(json.dumps(row, ensure_ascii=True) + "\n")
            ts_values = [int(item.get("ts_ms", 0)) for item in tab_rows if int(item.get("ts_ms", 0)) > 0]
            if not ts_values:
                ts_values = [entry.first_ts_ms, entry.last_ts_ms]
            await self._repository.record_manifest(
                pair_symbol=pair_symbol,
                tab_id=tab_id,
                date_key=date_key,
                file_path=str(file_path),
                row_count=len(tab_rows),
                ts_start_ms=min(ts_values),
                ts_end_ms=max(ts_values),
                schema_version=self._schema_version if _HAS_PYARROW else f"{self._schema_version}-jsonl",
            )

    async def _run_retention_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(3600)
                cutoff_ms = int(time.time() * 1000) - (self._retention_days * 24 * 60 * 60 * 1000)
                stale_files = await self._repository.purge_old_manifests(cutoff_ms)
                removed = 0
                for file_path in stale_files:
                    try:
                        os.remove(file_path)
                        removed += 1
                    except FileNotFoundError:
                        continue
                    except Exception:
                        continue
                if removed:
                    logger.info("ML retention removed stale parquet files=%s cutoff=%s", removed, utc_now_iso())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("ML retention loop error: %s", exc)
