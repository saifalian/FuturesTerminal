from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None

from app.services.storage.db import connect_sqlite, init_schema
from app.services.tool_mode_matrix import event_stream_to_tool_id, is_tool_enabled

logger = logging.getLogger(__name__)


KNOWN_BINANCE_STREAM_TOKENS = {
    "aggtrade",
    "bookticker",
    "depth",
    "forceorder",
    "kline",
    "kline_1m",
    "markprice",
    "miniTicker".lower(),
    "ticker",
}


class RawEventRecorder:
    def __init__(
        self,
        sqlite_path: Path,
        replay_dir: Path,
        market_events_dir: Path,
        schema_path: Path,
        pair_soft_cap_bytes: int | None = None,
        tool_mode_matrix_resolver: Any | None = None,
    ) -> None:
        self.sqlite_path = sqlite_path
        self.replay_dir = replay_dir
        self.market_events_dir = market_events_dir
        self.schema_path = schema_path
        queue_max = max(10_000, int(os.getenv("RECORDER_QUEUE_MAXSIZE", "120000")))
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=queue_max)
        self._worker: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._warn_interval_sec = max(0.2, float(os.getenv("RECORDER_WARN_INTERVAL_SEC", "2.0")))
        self._last_warn_ts = 0.0
        self._dropped_since_last_warn = 0
        self._drop_noisy_threshold = float(os.getenv("RECORDER_DROP_NOISY_THRESHOLD", "0.90"))
        self._batch_size = max(100, int(os.getenv("RECORDER_BATCH_SIZE", "1000")))
        self._flush_interval_sec = max(0.1, float(os.getenv("RECORDER_FLUSH_INTERVAL_SEC", "0.35")))
        self._bookticker_every_n = max(1, int(os.getenv("RECORDER_BOOKTICKER_EVERY_N", "5")))
        self._depth100ms_every_n = max(1, int(os.getenv("RECORDER_DEPTH100MS_EVERY_N", "3")))
        self._sample_counters: dict[str, int] = {}
        self._recording_enabled: bool = True
        self._recording_symbol: str = ""
        self._recording_mode: str = "lightweight"

        self._chunk_max_rows = max(500, int(os.getenv("MARKET_EVENTS_CHUNK_MAX_ROWS", "3500")))
        self._chunk_flush_interval_sec = max(0.25, float(os.getenv("MARKET_EVENTS_CHUNK_FLUSH_SEC", "1.2")))
        soft_cap_env = float(os.getenv("MARKET_PAIR_SOFT_CAP_GB", "25"))
        cap_bytes = int(max(0.5, soft_cap_env) * (1024**3))
        self._pair_soft_cap_bytes = max(1_000_000_000, int(pair_soft_cap_bytes or cap_bytes))
        self._pair_size_cache: dict[str, tuple[float, int]] = {}
        self._pair_soft_cap_blocked: set[str] = set()
        self._pair_soft_cap_overrides: set[str] = set()
        self._pair_soft_cap_warn_at: dict[str, float] = {}
        self._pair_soft_cap_warn_interval_sec = 30.0
        self._chunk_buffers: dict[str, dict[str, Any]] = {}
        self._tool_mode_matrix_resolver = tool_mode_matrix_resolver

    def _is_noisy_stream(self, stream: str) -> bool:
        s = (stream or "").lower()
        return "@bookticker" in s or "@depth@100ms" in s

    def _should_sample_drop(self, stream: str) -> bool:
        s = (stream or "").lower()
        every_n = 1
        if "@bookticker" in s:
            every_n = self._bookticker_every_n
        elif "@depth@100ms" in s:
            every_n = self._depth100ms_every_n
        if every_n <= 1:
            return False
        key = f"sample:{s}"
        count = self._sample_counters.get(key, 0) + 1
        self._sample_counters[key] = count
        # Keep only each Nth event for very high-frequency streams.
        return (count % every_n) != 0

    async def start(self) -> None:
        if self._worker and not self._worker.done():
            return
        self.replay_dir.mkdir(parents=True, exist_ok=True)
        self.market_events_dir.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._worker = asyncio.create_task(self._run(), name="raw-event-recorder")

    async def stop(self) -> None:
        if self._worker is None:
            return
        self._stop.set()
        if self._worker:
            await self._worker
        self._worker = None

    def set_recording_target(self, *, enabled: bool, symbol: str | None = None) -> None:
        self._recording_enabled = bool(enabled)
        self._recording_symbol = str(symbol or "").strip().upper()

    def set_recording_mode(self, mode: str | None) -> None:
        next_mode = str(mode or "lightweight").strip().lower()
        if next_mode not in {"lightweight", "full_fidelity"}:
            next_mode = "lightweight"
        self._recording_mode = next_mode

    def recording_mode(self) -> str:
        return self._recording_mode

    def recording_state(self) -> dict[str, Any]:
        return {
            "enabled": self._recording_enabled,
            "symbol": self._recording_symbol,
            "mode": self._recording_mode,
            "full_fidelity_available": pa is not None and pq is not None,
            "soft_cap_pairs_blocked": sorted(self._pair_soft_cap_blocked),
            "soft_cap_pairs_overridden": sorted(self._pair_soft_cap_overrides),
        }

    def set_soft_cap_override(self, pair_symbol: str, *, allow_continue: bool = True) -> dict[str, Any]:
        pair = str(pair_symbol or "").strip().upper()
        if not pair:
            return self.recording_state()
        if allow_continue:
            self._pair_soft_cap_overrides.add(pair)
            self._pair_soft_cap_blocked.discard(pair)
            self._pair_soft_cap_warn_at.pop(pair, None)
            logger.warning(
                "Soft-cap override enabled for %s. Full-fidelity recording will continue beyond cap (%s bytes).",
                pair,
                self._pair_soft_cap_bytes,
            )
        else:
            self._pair_soft_cap_overrides.discard(pair)
            self._pair_size_cache.pop(pair, None)
        return self.recording_state()

    def _extract_symbol(self, event: dict) -> str:
        direct = event.get("pair_symbol")
        if isinstance(direct, str) and direct.strip():
            return direct.strip().upper()
        payload = event.get("payload")
        if isinstance(payload, dict):
            direct = payload.get("s")
            if isinstance(direct, str) and direct:
                return direct.upper()
            kline = payload.get("k")
            if isinstance(kline, dict):
                k_symbol = kline.get("s")
                if isinstance(k_symbol, str) and k_symbol:
                    return k_symbol.upper()
            force_order = payload.get("o")
            if isinstance(force_order, dict):
                o_symbol = force_order.get("s")
                if isinstance(o_symbol, str) and o_symbol:
                    return o_symbol.upper()
        stream = str(event.get("stream", ""))
        if "@" in stream:
            return stream.split("@", 1)[0].upper()
        return ""

    @staticmethod
    def _iso_to_ms(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            iv = int(value)
            if iv > 10_000_000_000:
                return iv
            if iv > 0:
                return iv * 1000
            return 0
        text = str(value).strip()
        if not text:
            return 0
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return int(datetime.fromisoformat(text).timestamp() * 1000)
        except Exception:
            return 0

    @staticmethod
    def _ms_to_iso(ts_ms: int) -> str:
        if ts_ms <= 0:
            return datetime.now(tz=timezone.utc).isoformat()
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

    def _extract_exchange_ts_ms(self, payload: Any) -> int:
        if isinstance(payload, dict):
            for key in ("E", "T", "t", "ts_ms", "timestamp", "time"):
                value = payload.get(key)
                ts_ms = self._iso_to_ms(value)
                if ts_ms > 0:
                    return ts_ms
            kline = payload.get("k")
            if isinstance(kline, dict):
                for key in ("T", "t", "ts_ms", "timestamp"):
                    ts_ms = self._iso_to_ms(kline.get(key))
                    if ts_ms > 0:
                        return ts_ms
            force_order = payload.get("o")
            if isinstance(force_order, dict):
                for key in ("T", "t", "ts_ms", "timestamp"):
                    ts_ms = self._iso_to_ms(force_order.get(key))
                    if ts_ms > 0:
                        return ts_ms
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    for key in ("ts_ms", "timestamp", "time", "T", "t"):
                        ts_ms = self._iso_to_ms(item.get(key))
                        if ts_ms > 0:
                            return ts_ms
        return 0

    def _extract_source_exchange_id(self, event: dict, pair_symbol: str) -> str:
        explicit = str(event.get("source_exchange_id", "") or "").strip().lower()
        if explicit:
            return explicit
        stream = str(event.get("stream", "") or "").strip().lower()
        if "@" not in stream:
            return "unknown"
        parts = stream.split("@")
        if len(parts) >= 3 and pair_symbol and parts[0] == pair_symbol.lower():
            maybe_exchange = parts[1].strip().lower()
            if maybe_exchange and maybe_exchange not in KNOWN_BINANCE_STREAM_TOKENS:
                return maybe_exchange
            return "binance"
        return "binance"

    def _canonical_event(self, event: dict) -> dict[str, Any]:
        pair_symbol = self._extract_symbol(event)
        stream = str(event.get("stream", "") or "")
        event_type = str(event.get("event_type", "") or "")
        payload = event.get("payload")
        payload_json = json.dumps(payload if payload is not None else {}, ensure_ascii=False)
        ts_receive_ms = self._iso_to_ms(event.get("ts_receive_ms") or event.get("received_at"))
        if ts_receive_ms <= 0:
            ts_receive_ms = int(time.time() * 1000)
        ts_exchange_ms = self._iso_to_ms(event.get("ts_exchange_ms"))
        if ts_exchange_ms <= 0:
            ts_exchange_ms = self._extract_exchange_ts_ms(payload)
        if ts_exchange_ms <= 0:
            ts_exchange_ms = ts_receive_ms
        source_exchange_id = self._extract_source_exchange_id(event, pair_symbol)
        capture_mode = "full_fidelity" if self._recording_mode == "full_fidelity" else "lightweight"
        return {
            "pair_symbol": pair_symbol,
            "source_exchange_id": source_exchange_id,
            "stream": stream,
            "event_type": event_type,
            "ts_exchange_ms": int(ts_exchange_ms),
            "ts_receive_ms": int(ts_receive_ms),
            "payload_json": payload_json,
            "capture_mode": capture_mode,
            "dataset_quality": capture_mode,
            "received_at": str(event.get("received_at") or self._ms_to_iso(ts_receive_ms)),
        }

    def _recording_stream_allowed(self, stream: str, event_type: str) -> bool:
        resolver = self._tool_mode_matrix_resolver
        if not callable(resolver):
            return True
        tool_id = event_stream_to_tool_id(stream, event_type)
        if not tool_id:
            return True
        try:
            matrix = resolver()
        except Exception:
            return True
        return is_tool_enabled(matrix, tool_id=tool_id, mode_id="recording")

    def _estimate_pair_size_bytes(self, pair_symbol: str) -> int:
        now = time.monotonic()
        cached = self._pair_size_cache.get(pair_symbol)
        if cached and (now - cached[0]) < 20.0:
            return int(cached[1])
        pair_dir = self.market_events_dir / f"pair={pair_symbol}"
        total = 0
        if pair_dir.exists():
            for file in pair_dir.rglob("*.parquet"):
                try:
                    total += int(file.stat().st_size)
                except Exception:
                    continue
        self._pair_size_cache[pair_symbol] = (now, total)
        return total

    def _pair_above_soft_cap(self, pair_symbol: str) -> bool:
        if not pair_symbol:
            return False
        if pair_symbol in self._pair_soft_cap_overrides:
            self._pair_soft_cap_blocked.discard(pair_symbol)
            return False
        size = self._estimate_pair_size_bytes(pair_symbol)
        if size < self._pair_soft_cap_bytes:
            self._pair_soft_cap_blocked.discard(pair_symbol)
            return False
        self._pair_soft_cap_blocked.add(pair_symbol)
        now = time.monotonic()
        last_warn = self._pair_soft_cap_warn_at.get(pair_symbol, 0.0)
        if (now - last_warn) >= self._pair_soft_cap_warn_interval_sec:
            logger.warning(
                "Soft cap reached for %s full-fidelity chunks (size=%s bytes, cap=%s). "
                "New chunks for this pair are paused until cleanup.",
                pair_symbol,
                size,
                self._pair_soft_cap_bytes,
            )
            self._pair_soft_cap_warn_at[pair_symbol] = now
        return True

    def _buffer_full_fidelity_event(self, item: dict[str, Any]) -> None:
        if item.get("capture_mode") != "full_fidelity":
            return
        if pa is None or pq is None:
            return
        pair_symbol = str(item.get("pair_symbol", "") or "").upper()
        source_exchange_id = str(item.get("source_exchange_id", "") or "unknown").lower()
        ts_receive_ms = int(item.get("ts_receive_ms", 0) or 0)
        if not pair_symbol or ts_receive_ms <= 0:
            return
        if self._pair_above_soft_cap(pair_symbol):
            return
        dt = datetime.fromtimestamp(ts_receive_ms / 1000.0, tz=timezone.utc)
        date_key = dt.strftime("%Y-%m-%d")
        hour_key = dt.strftime("%H")
        buffer_key = f"{pair_symbol}|{source_exchange_id}|{date_key}|{hour_key}"
        buffer = self._chunk_buffers.get(buffer_key)
        if buffer is None:
            buffer = {
                "pair_symbol": pair_symbol,
                "source_exchange_id": source_exchange_id,
                "date_key": date_key,
                "hour_key": hour_key,
                "rows": [],
                "ts_start_ms": ts_receive_ms,
                "ts_end_ms": ts_receive_ms,
                "last_flush_ts": time.monotonic(),
            }
            self._chunk_buffers[buffer_key] = buffer
        rows: list[dict[str, Any]] = buffer["rows"]
        rows.append(item)
        if ts_receive_ms < int(buffer["ts_start_ms"]):
            buffer["ts_start_ms"] = ts_receive_ms
        if ts_receive_ms > int(buffer["ts_end_ms"]):
            buffer["ts_end_ms"] = ts_receive_ms

    async def _flush_full_fidelity_buffers(self, conn: Any, *, force: bool = False) -> None:
        if pa is None or pq is None:
            self._chunk_buffers.clear()
            return
        if not self._chunk_buffers:
            return
        now = time.monotonic()
        for key in list(self._chunk_buffers.keys()):
            buffer = self._chunk_buffers.get(key)
            if not isinstance(buffer, dict):
                continue
            rows = buffer.get("rows", [])
            if not isinstance(rows, list) or not rows:
                self._chunk_buffers.pop(key, None)
                continue
            due = (
                force
                or len(rows) >= self._chunk_max_rows
                or (now - float(buffer.get("last_flush_ts", 0.0))) >= self._chunk_flush_interval_sec
            )
            if not due:
                continue
            pair_symbol = str(buffer.get("pair_symbol", "UNSCOPED")).upper()
            source_exchange_id = str(buffer.get("source_exchange_id", "unknown")).lower()
            date_key = str(buffer.get("date_key", "1970-01-01"))
            hour_key = str(buffer.get("hour_key", "00"))
            if self._pair_above_soft_cap(pair_symbol):
                self._chunk_buffers.pop(key, None)
                continue
            ts_start_ms = int(buffer.get("ts_start_ms", 0) or 0)
            ts_end_ms = int(buffer.get("ts_end_ms", 0) or 0)
            partition_dir = (
                self.market_events_dir
                / f"pair={pair_symbol}"
                / f"exchange={source_exchange_id}"
                / f"date={date_key}"
                / f"hour={hour_key}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            part_name = f"part-{ts_start_ms}-{ts_end_ms}-{uuid4().hex[:10]}.parquet"
            file_path = partition_dir / part_name
            payload = {
                "pair_symbol": [str(row.get("pair_symbol", "")) for row in rows],
                "source_exchange_id": [str(row.get("source_exchange_id", "unknown")) for row in rows],
                "stream": [str(row.get("stream", "")) for row in rows],
                "event_type": [str(row.get("event_type", "")) for row in rows],
                "ts_exchange_ms": [int(row.get("ts_exchange_ms", 0) or 0) for row in rows],
                "ts_receive_ms": [int(row.get("ts_receive_ms", 0) or 0) for row in rows],
                "payload_json": [str(row.get("payload_json", "{}")) for row in rows],
                "capture_mode": [str(row.get("capture_mode", "full_fidelity")) for row in rows],
                "dataset_quality": [str(row.get("dataset_quality", "full_fidelity")) for row in rows],
            }
            table = pa.table(payload)
            pq.write_table(table, file_path, compression="zstd")
            file_size = int(file_path.stat().st_size) if file_path.exists() else 0
            await conn.execute(
                """
                INSERT INTO market_event_chunks
                (pair_symbol, source_exchange_id, date_key, hour_key, ts_start_ms, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_symbol,
                    source_exchange_id,
                    date_key,
                    hour_key,
                    ts_start_ms,
                    ts_end_ms,
                    len(rows),
                    str(file_path.resolve(strict=False)),
                    file_size,
                    "full_fidelity",
                    "full_fidelity",
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            self._pair_size_cache.pop(pair_symbol, None)
            self._chunk_buffers.pop(key, None)

    async def _ensure_column(self, conn: Any, table: str, column: str, dtype: str) -> None:
        cursor = await conn.execute(f"PRAGMA table_info({table})")
        rows = await cursor.fetchall()
        await cursor.close()
        cols = {str(row[1]).lower() for row in rows}
        if column.lower() in cols:
            return
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")

    async def _ensure_storage_schema(self, conn: Any) -> None:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_event_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_symbol TEXT NOT NULL,
                source_exchange_id TEXT NOT NULL,
                date_key TEXT NOT NULL,
                hour_key TEXT NOT NULL,
                ts_start_ms INTEGER NOT NULL,
                ts_end_ms INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                capture_mode TEXT NOT NULL,
                dataset_quality TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_event_chunks_pair_ts ON market_event_chunks(pair_symbol, ts_start_ms, ts_end_ms)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_event_chunks_pair_ex_hour ON market_event_chunks(pair_symbol, source_exchange_id, date_key, hour_key)"
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_market_event_chunks_file_path ON market_event_chunks(file_path)"
        )
        await self._ensure_column(conn, "raw_events", "pair_symbol", "TEXT")
        await self._ensure_column(conn, "raw_events", "source_exchange_id", "TEXT")
        await self._ensure_column(conn, "raw_events", "ts_exchange_ms", "INTEGER")
        await self._ensure_column(conn, "raw_events", "ts_receive_ms", "INTEGER")
        await self._ensure_column(conn, "raw_events", "capture_mode", "TEXT")
        await self._ensure_column(conn, "market_event_chunks", "dataset_quality", "TEXT")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_pair_ts ON raw_events(pair_symbol, ts_receive_ms)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_capture_mode ON raw_events(capture_mode)")
        await conn.commit()

    def push_nowait(self, event: dict) -> None:
        if not self._recording_enabled:
            return
        if self._recording_symbol:
            symbol = self._extract_symbol(event)
            if symbol != self._recording_symbol:
                return
        stream = str(event.get("stream", "unknown"))
        if self._recording_mode != "full_fidelity":
            if self._should_sample_drop(stream):
                return
            # Under sustained pressure, drop ultra-noisy streams first to protect core pipeline.
            fill_ratio = self._queue.qsize() / max(1, self._queue.maxsize)
            if fill_ratio >= self._drop_noisy_threshold and self._is_noisy_stream(stream):
                self._dropped_since_last_warn += 1
                now = time.monotonic()
                if now - self._last_warn_ts >= self._warn_interval_sec:
                    logger.warning(
                        "Recorder under pressure (queue=%s/%s); dropped noisy events=%s (latest stream=%s)",
                        self._queue.qsize(),
                        self._queue.maxsize,
                        self._dropped_since_last_warn,
                        stream,
                    )
                    self._last_warn_ts = now
                    self._dropped_since_last_warn = 0
                return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped_since_last_warn += 1
            now = time.monotonic()
            if now - self._last_warn_ts >= self._warn_interval_sec:
                logger.warning(
                    "Recorder queue full; dropped=%s (latest stream=%s, queue=%s/%s)",
                    self._dropped_since_last_warn,
                    stream,
                    self._queue.qsize(),
                    self._queue.maxsize,
                )
                self._last_warn_ts = now
                self._dropped_since_last_warn = 0

    async def _run(self) -> None:
        conn = await connect_sqlite(self.sqlite_path)
        await init_schema(conn, self.schema_path)
        await self._ensure_storage_schema(conn)
        replay_name = datetime.now(tz=timezone.utc).strftime("session_%Y%m%d_%H%M%S.jsonl")
        replay_path = self.replay_dir / replay_name
        logger.info("Recorder started. SQLite=%s replay=%s", self.sqlite_path, replay_path)
        if (pa is None or pq is None) and self._recording_mode == "full_fidelity":
            logger.warning("Full-fidelity mode requested but pyarrow is unavailable. Falling back to lightweight-only storage.")

        with replay_path.open("a", encoding="utf-8") as replay_file:
            last_flush_ts = time.monotonic()
            while not (self._stop.is_set() and self._queue.empty()):
                try:
                    event = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    now = time.monotonic()
                    if now - last_flush_ts >= self._flush_interval_sec:
                        await self._flush_full_fidelity_buffers(conn, force=False)
                        await conn.commit()
                        replay_file.flush()
                        last_flush_ts = now
                    continue

                batch = [event]
                while len(batch) < self._batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                rows = []
                for item in batch:
                    canonical = self._canonical_event(item)
                    if not self._recording_stream_allowed(
                        str(canonical.get("stream", "")),
                        str(canonical.get("event_type", "")),
                    ):
                        continue
                    rows.append(
                        (
                            canonical.get("received_at", ""),
                            canonical.get("stream", ""),
                            canonical.get("event_type", ""),
                            canonical.get("payload_json", "{}"),
                            canonical.get("pair_symbol", ""),
                            canonical.get("source_exchange_id", "unknown"),
                            int(canonical.get("ts_exchange_ms", 0) or 0),
                            int(canonical.get("ts_receive_ms", 0) or 0),
                            canonical.get("capture_mode", "lightweight"),
                        )
                    )
                    replay_file.write(json.dumps(item, ensure_ascii=False) + "\n")
                    self._buffer_full_fidelity_event(canonical)

                if rows:
                    await conn.executemany(
                    """
                    INSERT INTO raw_events
                    (ts, stream, event_type, payload, pair_symbol, source_exchange_id, ts_exchange_ms, ts_receive_ms, capture_mode)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        rows,
                    )
                for _ in batch:
                    self._queue.task_done()

                now = time.monotonic()
                if len(batch) >= self._batch_size or (now - last_flush_ts) >= self._flush_interval_sec:
                    await self._flush_full_fidelity_buffers(conn, force=False)
                    await conn.commit()
                    replay_file.flush()
                    last_flush_ts = now

            await self._flush_full_fidelity_buffers(conn, force=True)
            await conn.commit()
            replay_file.flush()
        await conn.close()
        logger.info("Recorder stopped")
