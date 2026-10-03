from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class Bucket:
    rows: list[dict]
    ts_start_ms: int
    ts_end_ms: int


def _utc_date_hour(ts_ms: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def rebuild_pair(project_root: Path, pair_symbol: str, max_rows_per_chunk: int = 50_000) -> None:
    pair = pair_symbol.upper().strip()
    if not pair:
        raise ValueError("pair_symbol required")

    db_path = project_root / "backend" / "data" / "sqlite" / "terminal.db"
    market_root = project_root / "backend" / "data" / "market_events" / f"pair={pair}"
    market_root.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Start from a clean chunk index for this pair (files are not deleted).
    cur.execute("DELETE FROM market_event_chunks WHERE pair_symbol = ?", (pair,))
    conn.commit()

    q = """
        SELECT pair_symbol, source_exchange_id, stream, event_type, ts_exchange_ms, ts_receive_ms, payload, capture_mode
        FROM raw_events
        WHERE pair_symbol = ?
        ORDER BY ts_receive_ms ASC
    """
    stream = conn.execute(q, (pair,))

    buckets: dict[tuple[str, str, str], Bucket] = {}
    written_files = 0
    written_rows = 0

    def flush_bucket(key: tuple[str, str, str]) -> None:
        nonlocal written_files, written_rows
        bucket = buckets.get(key)
        if not bucket or not bucket.rows:
            return
        ex, date_key, hour_key = key
        partition_dir = market_root / f"exchange={ex}" / f"date={date_key}" / f"hour={hour_key}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        ts_start_ms = bucket.ts_start_ms
        ts_end_ms = bucket.ts_end_ms
        file_path = partition_dir / f"part-{ts_start_ms}-{ts_end_ms}-{uuid4().hex[:10]}.parquet"

        payload = {
            "pair_symbol": [r["pair_symbol"] for r in bucket.rows],
            "source_exchange_id": [r["source_exchange_id"] for r in bucket.rows],
            "stream": [r["stream"] for r in bucket.rows],
            "event_type": [r["event_type"] for r in bucket.rows],
            "ts_exchange_ms": [int(r["ts_exchange_ms"]) for r in bucket.rows],
            "ts_receive_ms": [int(r["ts_receive_ms"]) for r in bucket.rows],
            "payload_json": [r["payload_json"] for r in bucket.rows],
            "capture_mode": [r["capture_mode"] for r in bucket.rows],
            "dataset_quality": ["full_fidelity"] * len(bucket.rows),
        }
        table = pa.table(payload)
        pq.write_table(table, file_path, compression="zstd")
        size_bytes = int(file_path.stat().st_size) if file_path.exists() else 0

        cur.execute(
            """
            INSERT INTO market_event_chunks
            (pair_symbol, source_exchange_id, date_key, hour_key, ts_start_ms, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pair,
                ex,
                date_key,
                hour_key,
                ts_start_ms,
                ts_end_ms,
                len(bucket.rows),
                str(file_path.resolve(strict=False)),
                size_bytes,
                "full_fidelity",
                "full_fidelity",
                datetime.now(tz=timezone.utc).isoformat(),
            ),
        )
        written_files += 1
        written_rows += len(bucket.rows)
        buckets[key] = Bucket(rows=[], ts_start_ms=0, ts_end_ms=0)

    batch = stream.fetchmany(25_000)
    while batch:
        for row in batch:
            ex = str(row["source_exchange_id"] or "unknown")
            recv = int(row["ts_receive_ms"] or 0)
            if recv <= 0:
                continue
            date_key, hour_key = _utc_date_hour(recv)
            key = (ex, date_key, hour_key)
            b = buckets.get(key)
            if b is None:
                b = Bucket(rows=[], ts_start_ms=recv, ts_end_ms=recv)
                buckets[key] = b
            b.ts_start_ms = min(b.ts_start_ms, recv) if b.rows else recv
            b.ts_end_ms = max(b.ts_end_ms, recv) if b.rows else recv
            b.rows.append(
                {
                    "pair_symbol": pair,
                    "source_exchange_id": ex,
                    "stream": str(row["stream"] or ""),
                    "event_type": str(row["event_type"] or ""),
                    "ts_exchange_ms": int(row["ts_exchange_ms"] or 0),
                    "ts_receive_ms": recv,
                    "payload_json": str(row["payload"] or "{}"),
                    "capture_mode": str(row["capture_mode"] or "full_fidelity"),
                }
            )
            if len(b.rows) >= max_rows_per_chunk:
                flush_bucket(key)
        conn.commit()
        batch = stream.fetchmany(25_000)

    # Flush all remaining buckets.
    for key in list(buckets.keys()):
        flush_bucket(key)
    conn.commit()

    summary = conn.execute(
        "SELECT COUNT(*), SUM(row_count), MIN(ts_start_ms), MAX(ts_end_ms) FROM market_event_chunks WHERE pair_symbol = ?",
        (pair,),
    ).fetchone()
    conn.close()
    print(
        f"rebuild_complete pair={pair} files_written={written_files} rows_written={written_rows} "
        f"db_chunks={summary[0] or 0} db_rows={summary[1] or 0} range=[{summary[2]}..{summary[3]}]"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--pair", required=True)
    ap.add_argument("--max-rows-per-chunk", type=int, default=50_000)
    args = ap.parse_args()
    rebuild_pair(Path(args.project_root), args.pair, args.max_rows_per_chunk)


if __name__ == "__main__":
    main()

