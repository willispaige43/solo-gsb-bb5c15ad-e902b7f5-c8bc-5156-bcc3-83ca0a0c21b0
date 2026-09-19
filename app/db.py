"""SQLite 连接、建表初始化与小工具。

启动时（FastAPI lifespan）调用 init_db() 完成建表，
因此 Docker 容器第一次启动即可直接使用，无需手工初始化数据库。
"""

import os
import pathlib
from typing import Any, Optional

import aiosqlite

DB_PATH = os.environ.get(
    "DB_PATH",
    str(pathlib.Path(__file__).resolve().parent.parent / "data" / "drill.db"),
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS rooms (
    code           TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    host_token     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    run_id         INTEGER NOT NULL DEFAULT 1,
    started_at     REAL,
    last_resume_at REAL,
    base_elapsed   REAL NOT NULL DEFAULT 0,
    created_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS cards (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    room_code   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    trigger_at  REAL NOT NULL,
    title       TEXT NOT NULL,
    body        TEXT NOT NULL DEFAULT '',
    severity    TEXT NOT NULL DEFAULT 'major',
    revealed    INTEGER NOT NULL DEFAULT 0,
    revealed_at REAL,
    FOREIGN KEY (room_code) REFERENCES rooms(code)
);
CREATE INDEX IF NOT EXISTS idx_cards_room ON cards(room_code);
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    room_code TEXT NOT NULL,
    run_id    INTEGER NOT NULL,
    type      TEXT NOT NULL,
    payload   TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_room_run ON events(room_code, run_id, id);
"""

db: Optional[aiosqlite.Connection] = None  # 在 connect() 后赋值


async def init_db() -> None:
    """创建数据目录并执行建表脚本（幂等）。"""
    parent = os.path.dirname(DB_PATH)
    if parent:
        pathlib.Path(parent).mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(DB_PATH)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.executescript(SCHEMA)
        await conn.commit()
    finally:
        await conn.close()


async def connect() -> None:
    global db
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA busy_timeout=5000")
    await db.commit()


async def close() -> None:
    global db
    if db is not None:
        await db.close()
        db = None


async def fetch_one(query: str, params: tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
    assert db is not None
    cur = await db.execute(query, params)
    return await cur.fetchone()


async def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
    assert db is not None
    cur = await db.execute(query, params)
    return await cur.fetchall()
