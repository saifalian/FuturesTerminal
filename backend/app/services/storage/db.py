from __future__ import annotations

from pathlib import Path

import aiosqlite


async def connect_sqlite(path: Path) -> aiosqlite.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(path)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


async def init_schema(conn: aiosqlite.Connection, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    await conn.executescript(sql)
    await conn.commit()
