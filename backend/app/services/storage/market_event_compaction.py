from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import shutil
from pathlib import Path
import sqlite3
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pa = None
    pq = None


@dataclass
class ChunkRow:
    pair_symbol: str
    source_exchange_id: str
    date_key: str
    hour_key: str
    ts_start_ms: int
    ts_end_ms: int
    row_count: int
    file_path: str
    size_bytes: int
    capture_mode: str
    dataset_quality: str


class MarketEventCompactionService:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.sqlite_path = project_root / "backend" / "data" / "sqlite" / "terminal.db"
        self.market_root = project_root / "backend" / "data" / "market_events"

    def compact_pair(self, pair_symbol: str, *, max_input_files: int = 200, max_rows_per_output: int = 750_000) -> dict[str, Any]:
        if pa is None or pq is None:
            raise RuntimeError("pyarrow is required for market-event compaction.")
        pair = str(pair_symbol or "").upper().strip()
        if not pair:
            raise ValueError("pair_symbol is required.")
        before = self._load_chunks(pair)
        if not before:
            return {"ok": True, "pair_symbol": pair, "compacted": False, "reason": "no_chunks"}
        repaired_missing_rows = self._prune_missing_chunk_rows(pair, before)
        if repaired_missing_rows > 0:
            before = self._load_chunks(pair)
            if not before:
                return {
                    "ok": True,
                    "pair_symbol": pair,
                    "compacted": False,
                    "reason": "all_chunks_missing_after_repair",
                    "repaired_missing_rows": repaired_missing_rows,
                }
        out_dir = self.market_root / f"pair={pair}" / "_compacted"
        out_dir.mkdir(parents=True, exist_ok=True)
        compaction_run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        tmp_out_dir = out_dir / f"_tmp_{compaction_run_id}"
        tmp_out_dir.mkdir(parents=True, exist_ok=True)
        created: list[ChunkRow] = []
        files_before = len(before)
        bytes_before = int(sum(c.size_bytes for c in before))
        rows_before_declared = int(sum(c.row_count for c in before))
        min_before_declared = min(c.ts_start_ms for c in before)
        max_before_declared = max(c.ts_end_ms for c in before)
        min_before_actual: int | None = None
        max_before_actual: int | None = None
        rows_before_actual = 0
        missing_files_count = 0
        unreadable_files_count = 0
        by_exchange: dict[str, list[ChunkRow]] = defaultdict(list)
        for row in before:
            by_exchange[str(row.source_exchange_id or "unknown")].append(row)

        file_seq = 0
        for ex, rows_for_exchange in by_exchange.items():
            rows_for_exchange.sort(key=lambda r: r.ts_start_ms)
            i = 0
            while i < len(rows_for_exchange):
                group = rows_for_exchange[i : i + max_input_files]
                i += max_input_files
                tables: list[pa.Table] = []
                for row in group:
                    p = Path(row.file_path)
                    if not p.exists():
                        missing_files_count += 1
                        continue
                    try:
                        table = pq.ParquetFile(str(p)).read()
                    except Exception:
                        unreadable_files_count += 1
                        continue
                    tables.append(self._normalize_table_for_concat(table))
                if not tables:
                    continue
                combo = pa.concat_tables(tables, promote_options="default")
                rows_before_actual += int(combo.num_rows)
                combo_ts_vals = self._extract_ts_values(combo)
                if combo_ts_vals:
                    combo_min = min(combo_ts_vals)
                    combo_max = max(combo_ts_vals)
                else:
                    combo_min = min((r.ts_start_ms for r in group), default=0)
                    combo_max = max((r.ts_end_ms for r in group), default=combo_min)
                if min_before_actual is None or combo_min < min_before_actual:
                    min_before_actual = combo_min
                if max_before_actual is None or combo_max > max_before_actual:
                    max_before_actual = combo_max
                sort_col = "ts_receive_ms" if "ts_receive_ms" in combo.schema.names else None
                if sort_col:
                    combo = combo.sort_by([(sort_col, "ascending")])
                total_rows = combo.num_rows
                cursor = 0
                rows_written_in_group = 0
                group_fallback_start = min((r.ts_start_ms for r in group), default=0)
                group_fallback_end = max((r.ts_end_ms for r in group), default=group_fallback_start)
                while cursor < total_rows:
                    end = min(total_rows, cursor + max_rows_per_output)
                    part = combo.slice(cursor, end - cursor)
                    cursor = end
                    ts_vals = self._extract_ts_values(part)
                    if ts_vals:
                        ts_start = min(ts_vals)
                        ts_end = max(ts_vals)
                    else:
                        # Never drop rows during compaction; fall back to source chunk bounds.
                        ts_start = group_fallback_start
                        ts_end = group_fallback_end
                    date_key = datetime.fromtimestamp(ts_start / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                    hour_key = datetime.fromtimestamp(ts_start / 1000, tz=timezone.utc).strftime("%H")
                    out_path = tmp_out_dir / f"part-{ts_start}-{ts_end}-{date_key}-{hour_key}-{file_seq:06d}.parquet"
                    file_seq += 1
                    pq.write_table(part, out_path, compression="zstd")
                    part_rows = int(part.num_rows)
                    rows_written_in_group += part_rows
                    created.append(
                        ChunkRow(
                            pair_symbol=pair,
                            source_exchange_id=ex,
                            date_key=date_key,
                            hour_key=hour_key,
                            ts_start_ms=ts_start,
                            ts_end_ms=ts_end,
                            row_count=part_rows,
                            file_path=str(out_path.resolve(strict=False)),
                            size_bytes=int(out_path.stat().st_size if out_path.exists() else 0),
                            capture_mode="full_fidelity",
                            dataset_quality="full_fidelity",
                        )
                    )
                if rows_written_in_group != total_rows:
                    raise RuntimeError(
                        f"Group row parity failed for {ex}: expected={total_rows}, written={rows_written_in_group}"
                    )
        if not created:
            try:
                shutil.rmtree(tmp_out_dir, ignore_errors=True)
            except Exception:
                pass
            return {"ok": True, "pair_symbol": pair, "compacted": False, "reason": "no_output"}
        rows_after = int(sum(c.row_count for c in created))
        min_after = min(c.ts_start_ms for c in created)
        max_after = max(c.ts_end_ms for c in created)
        if rows_before_actual != rows_after:
            raise RuntimeError(f"Row parity failed: before={rows_before_actual}, after={rows_after}")
        if min_before_actual is None or max_before_actual is None:
            raise RuntimeError("Timestamp parity failed: no readable timestamp bounds found in input.")
        if min_before_actual != min_after or max_before_actual != max_after:
            raise RuntimeError(
                "Timestamp parity failed: "
                f"before=({min_before_actual},{max_before_actual}) after=({min_after},{max_after})"
            )
        if missing_files_count > 0 or unreadable_files_count > 0:
            try:
                shutil.rmtree(tmp_out_dir, ignore_errors=True)
            except Exception:
                pass
            raise RuntimeError(
                "Input file integrity failed before swap: "
                f"missing_files={missing_files_count}, unreadable_files={unreadable_files_count}"
            )
        # Promote temp outputs into the canonical compacted directory only after all checks pass.
        promoted: list[ChunkRow] = []
        for c in created:
            src = Path(c.file_path)
            if not src.exists():
                try:
                    shutil.rmtree(tmp_out_dir, ignore_errors=True)
                except Exception:
                    pass
                raise RuntimeError(f"Compaction output missing before promote: {src}")
            dst = out_dir / src.name
            if dst.exists():
                # Extremely unlikely with unique tmp dir + seq, but protect against collisions.
                dst = out_dir / f"{src.stem}-{compaction_run_id}{src.suffix}"
            shutil.move(str(src), str(dst))
            promoted.append(
                ChunkRow(
                    pair_symbol=c.pair_symbol,
                    source_exchange_id=c.source_exchange_id,
                    date_key=c.date_key,
                    hour_key=c.hour_key,
                    ts_start_ms=c.ts_start_ms,
                    ts_end_ms=c.ts_end_ms,
                    row_count=c.row_count,
                    file_path=str(dst.resolve(strict=False)),
                    size_bytes=int(dst.stat().st_size if dst.exists() else c.size_bytes),
                    capture_mode=c.capture_mode,
                    dataset_quality=c.dataset_quality,
                )
            )
        try:
            shutil.rmtree(tmp_out_dir, ignore_errors=True)
        except Exception:
            pass
        self._swap_chunks(pair, promoted)
        removed = 0
        backup_dir = self.market_root / f"pair={pair}" / "_precompact_backup" / datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_dir.mkdir(parents=True, exist_ok=True)
        for c in before:
            try:
                p = Path(c.file_path)
                if p.exists():
                    dst = backup_dir / p.name
                    suffix = 1
                    while dst.exists():
                        dst = backup_dir / f"{p.stem}_{suffix}{p.suffix}"
                        suffix += 1
                    shutil.move(str(p), str(dst))
                    removed += 1
            except Exception:
                pass
        return {
            "ok": True,
            "pair_symbol": pair,
            "compacted": True,
            "files_before": files_before,
            "files_after": len(promoted),
            "rows_before": rows_before_actual,
            "rows_after": rows_after,
            "rows_before_declared": rows_before_declared,
            "missing_files_count": missing_files_count,
            "unreadable_files_count": unreadable_files_count,
            "min_ts_declared": min_before_declared,
            "max_ts_declared": max_before_declared,
            "min_ts_actual": min_before_actual,
            "max_ts_actual": max_before_actual,
            "bytes_before": bytes_before,
            "bytes_after": int(sum(c.size_bytes for c in promoted)),
            "min_ts_ms": min_after,
            "max_ts_ms": max_after,
            "old_files_removed": removed,
            "backup_dir": str(backup_dir.resolve(strict=False)),
            "repaired_missing_rows": repaired_missing_rows,
        }

    def _load_chunks(self, pair_symbol: str) -> list[ChunkRow]:
        if not self.sqlite_path.exists():
            return []
        out: list[ChunkRow] = []
        with sqlite3.connect(self.sqlite_path) as conn:
            rows = conn.execute(
                """
                SELECT pair_symbol, source_exchange_id, date_key, hour_key, ts_start_ms, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality
                FROM market_event_chunks
                WHERE pair_symbol = ?
                ORDER BY ts_start_ms ASC
                """,
                (pair_symbol,),
            ).fetchall()
        for r in rows:
            out.append(ChunkRow(str(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4] or 0), int(r[5] or 0), int(r[6] or 0), str(r[7] or ""), int(r[8] or 0), str(r[9] or "full_fidelity"), str(r[10] or "full_fidelity")))
        return out

    @staticmethod
    def _normalize_table_for_concat(table: "pa.Table") -> "pa.Table":
        """Normalize per-file schema so concat doesn't fail on dictionary/plain type mismatches."""
        columns: list[pa.Array | pa.ChunkedArray] = []
        fields: list[pa.Field] = []
        for field, chunked in zip(table.schema, table.columns):
            normalized = chunked.combine_chunks()
            value_type = normalized.type
            if pa.types.is_dictionary(value_type):
                value_type = value_type.value_type
                normalized = normalized.cast(value_type, safe=False)
            # Avoid Arrow offset overflow when concatenating large string payload columns
            # across many chunks/files (e.g. payload_json). Promote to large_string early.
            if pa.types.is_string(value_type):
                value_type = pa.large_string()
                normalized = normalized.cast(value_type, safe=False)
            fields.append(pa.field(field.name, value_type, nullable=field.nullable, metadata=field.metadata))
            columns.append(normalized)
        return pa.Table.from_arrays(columns, schema=pa.schema(fields, metadata=table.schema.metadata))

    @staticmethod
    def _extract_ts_values(table: "pa.Table") -> list[int]:
        for col in ("ts_receive_ms", "ts_event_ms", "ts_exchange_ms"):
            if col in table.schema.names:
                out = [int(v or 0) for v in table.column(col).to_pylist() if int(v or 0) > 0]
                if out:
                    return out
        return []

    def _swap_chunks(self, pair_symbol: str, chunks: list[ChunkRow]) -> None:
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM market_event_chunks WHERE pair_symbol = ?", (pair_symbol,))
            for c in chunks:
                conn.execute(
                    """
                    INSERT INTO market_event_chunks
                    (pair_symbol, source_exchange_id, date_key, hour_key, ts_start_ms, ts_end_ms, row_count, file_path, size_bytes, capture_mode, dataset_quality, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        c.pair_symbol,
                        c.source_exchange_id,
                        c.date_key,
                        c.hour_key,
                        c.ts_start_ms,
                        c.ts_end_ms,
                        c.row_count,
                        c.file_path,
                        c.size_bytes,
                        c.capture_mode,
                        c.dataset_quality,
                        datetime.now(tz=timezone.utc).isoformat(),
                    ),
                )
            conn.commit()

    def _prune_missing_chunk_rows(self, pair_symbol: str, chunks: list[ChunkRow]) -> int:
        missing_paths: list[str] = []
        for c in chunks:
            p = Path(c.file_path)
            if not p.exists():
                missing_paths.append(c.file_path)
        if not missing_paths:
            return 0
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("BEGIN")
            for fp in missing_paths:
                conn.execute("DELETE FROM market_event_chunks WHERE pair_symbol = ? AND file_path = ?", (pair_symbol, fp))
            conn.commit()
        return len(missing_paths)
