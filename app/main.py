"""事故演练实时情景注入台 —— FastAPI 服务端。

核心设计：
* 虚拟时钟：每个房间用 (status, base_elapsed, last_resume_at) 计算权威演练时间，
  paused/finished/pending 期间不随真实时间推进，pause/resume 幂等。
* 稳定揭示：调度器按 (trigger_at, seq) 顺序揭示卡片，暂停时立即停止推进。
* 重连不回退：WebSocket 连接后先下发权威快照（含最后事件 id），之后只推增量；
  客户端按事件 id 单调应用，同一 run 内状态只会前进。
"""

import asyncio
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

import aiosqlite
from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import db as database

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")

STATUSES = ("pending", "running", "paused", "finished")
SEVERITIES = ("info", "minor", "major", "critical")
TICK_INTERVAL = 0.1          # 调度器节拍（秒）
HEARTBEAT_INTERVAL = 2.0     # 虚拟时钟心跳（秒）
CLOCK_GRACE = 0.05           # 触发判定容差，避免边界漏触发

# 全局写锁：保证“写库 + 发事件”顺序化，事件 id 即权威因果顺序
write_lock = asyncio.Lock()


# --------------------------------------------------------------------------- #
# 数据映射与虚拟时钟
# --------------------------------------------------------------------------- #

def room_dict(row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "code": row["code"],
        "name": row["name"],
        "hostToken": row["host_token"],
        "status": row["status"],
        "runId": row["run_id"],
        "startedAt": row["started_at"],
        "lastResumeAt": row["last_resume_at"],
        "baseElapsed": row["base_elapsed"],
        "createdAt": row["created_at"],
    }


def now_ts() -> float:
    return time.time()


def virtual_now(room: dict[str, Any]) -> float:
    """房间当前权威演练时间（秒）。仅 running 状态随墙钟推进。"""
    if room["status"] != "running" or room["lastResumeAt"] is None:
        return float(room["baseElapsed"])
    return float(room["baseElapsed"]) + (time.time() - float(room["lastResumeAt"]))


def clock_payload(room: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": room["status"],
        "elapsed": round(virtual_now(room), 3),
        "baseElapsed": float(room["baseElapsed"]),
        "lastResumeAt": room["lastResumeAt"],
        "serverTime": now_ts(),
        "runId": room["runId"],
    }


# --------------------------------------------------------------------------- #
# 发布订阅枢纽：按 (房间, 角色) 隔离，参与者永远收不到未揭示卡片
# --------------------------------------------------------------------------- #

class Subscriber:
    __slots__ = ("queue", "role", "participant_id", "run_id")

    def __init__(self, role: str, participant_id: str, run_id: int):
        self.queue: asyncio.Queue[Optional[dict[str, Any]]] = asyncio.Queue(maxsize=200)
        self.role = role
        self.participant_id = participant_id
        self.run_id = run_id


# 主持人编排/控台类事件不下发给参与者（参与者只收到注入与演练状态）
HOST_ONLY_EVENTS = frozenset({
    "card_created", "card_deleted",
    "participant_joined", "participant_left",
    "room_created",
})


class Hub:
    def __init__(self) -> None:
        # code -> set[Subscriber]
        self.rooms: dict[str, set[Subscriber]] = {}

    def join(self, code: str, sub: Subscriber) -> None:
        self.rooms.setdefault(code, set()).add(sub)

    def leave(self, code: str, sub: Subscriber) -> None:
        subs = self.rooms.get(code)
        if subs:
            subs.discard(sub)
            if not subs:
                self.rooms.pop(code, None)

    def count_participants(self, code: str) -> int:
        return sum(1 for s in self.rooms.get(code, ()) if s.role == "participant")

    @staticmethod
    def _visible(sub: Subscriber, event_type: str) -> bool:
        if sub.role == "host":
            return True
        # 参与者：编排与控台信息（卡片增删、参与者进出等）不广播
        return event_type not in HOST_ONLY_EVENTS

    async def publish_old_run(self, code: str, new_run_id: int, msg: dict[str, Any]) -> None:
        """重置演练时通知仍挂在旧 run 上的连接（客户端据此重连拉取新快照）。"""
        for sub in list(self.rooms.get(code, ())):  # type: Subscriber
            if sub.run_id == new_run_id:
                continue
            try:
                sub.queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass

    async def publish(self, code: str, run_id: int, msg: dict[str, Any]) -> None:
        """向房间内同 run 的连接投递增量事件；慢消费者断开时丢弃其消息。"""
        for sub in list(self.rooms.get(code, ())):  # type: Subscriber
            if sub.run_id != run_id:
                continue
            if not self._visible(sub, msg["event"]["type"]):
                continue
            try:
                sub.queue.put_nowait(msg)
            except asyncio.QueueFull:
                # 通知对端其队列溢出，客户端应以快照重连
                try:
                    sub.queue.put_nowait({
                        "kind": "overflow",
                        "event": {"id": msg["event"]["id"], "type": "overflow"},
                    })
                except asyncio.QueueFull:
                    pass


hub = Hub()


async def insert_event(code: str, run_id: int, event_type: str, payload: dict[str, Any]) -> int:
    """写入事件并返回其自增 id（仅做写库，调用方自行负责加锁）。"""
    assert database.db is not None
    cur = await database.db.execute(
        "INSERT INTO events(room_code, run_id, type, payload, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (code, run_id, event_type, json.dumps(payload, ensure_ascii=False), now_ts()),
    )
    await database.db.commit()
    return int(cur.lastrowid)


async def emit(event_type: str, room: dict[str, Any], payload: Optional[dict[str, Any]] = None,
               *, run_id: Optional[int] = None) -> int:
    """持久化事件并增量推送给在线连接。必须在状态写入并 commit 之后调用。"""
    effective_run = room["runId"] if run_id is None else run_id
    async with write_lock:
        event_id = await insert_event(room["code"], effective_run, event_type, payload or {})
    await push_event(room["code"], effective_run, event_id, event_type, payload or {})
    return event_id


async def push_event(code: str, run_id: int, event_id: int, event_type: str,
                     payload: dict[str, Any]) -> None:
    msg = {
        "kind": "event",
        "event": {"id": event_id, "type": event_type, "payload": payload, "runId": run_id},
    }
    await hub.publish(code, run_id, msg)


# --------------------------------------------------------------------------- #
# 房间状态变更（均为幂等、且配合 write_lock 保证与调度器互斥）
# --------------------------------------------------------------------------- #

async def get_room_row(code: str) -> Optional[aiosqlite.Row]:
    return await database.fetch_one("SELECT * FROM rooms WHERE code=?", (code,))


async def update_room(code: str, **fields: Any) -> None:
    assert database.db is not None
    cols = ", ".join(f"{k}=?" for k in fields)
    await database.db.execute(
        f"UPDATE rooms SET {cols} WHERE code=?", (*fields.values(), code)
    )
    await database.db.commit()


async def _persist_status(room: dict[str, Any], status: str,
                          elapsed: Optional[float] = None) -> dict[str, Any]:
    """更新虚拟时钟记账字段（调用方自行处理互斥），返回最新房间字典。"""
    fields: dict[str, Any] = {"status": status}
    if status == "running":
        fields["last_resume_at"] = now_ts()
        if room["startedAt"] is None:
            fields["started_at"] = now_ts()
    elif status in ("paused", "finished", "pending"):
        fields["base_elapsed"] = virtual_now(room) if elapsed is None else elapsed
        fields["last_resume_at"] = None
    await update_room(room["code"], **fields)
    row = await get_room_row(room["code"])
    assert row is not None
    room.clear()
    room.update(room_dict(row))
    return room


async def set_status(room: dict[str, Any], status: str, elapsed: Optional[float] = None) -> dict[str, Any]:
    """统一的状态迁移入口：更新虚拟时钟记账字段并广播 room_state。"""
    async with write_lock:
        room = await _persist_status(room, status, elapsed)
    await emit("room_state", room, clock_payload(room))
    return room


# --------------------------------------------------------------------------- #
# 后台：调度器（按时揭示）+ 心跳（权威时钟）
# --------------------------------------------------------------------------- #

async def scheduler_tick() -> None:
    """处理所有 running 房间：按 (trigger_at, seq) 揭示到期卡片。

    暂停时状态已变为 paused，本函数不会读到该房间，因此绝不会在暂停期间推进揭示。
    """
    rows = await database.fetch_all("SELECT * FROM rooms WHERE status='running'")
    for row in rows:
        room = room_dict(row)
        vt = virtual_now(room)
        due = await database.fetch_all(
            "SELECT * FROM cards WHERE room_code=? AND revealed=0 "
            "AND trigger_at<=? ORDER BY trigger_at ASC, seq ASC",
            (room["code"], vt + CLOCK_GRACE),
        )
        if not due:
            continue
        revealed_cards: list[dict[str, Any]] = []
        finish_after = False
        async with write_lock:
            latest = await get_room_row(room["code"])
            if latest is None or latest["status"] != "running":
                continue  # 已暂停/结束：暂停期间不揭示
            current = virtual_now(room_dict(latest))
            assert database.db is not None
            for card_row in due:
                if card_row["trigger_at"] > current + CLOCK_GRACE:
                    break  # 重新按最新时钟校验，避免暂停边界多揭示
                await database.db.execute(
                    "UPDATE cards SET revealed=1, revealed_at=? WHERE id=? AND revealed=0",
                    (current, card_row["id"]),
                )
                revealed_cards.append({
                    "id": card_row["id"],
                    "seq": card_row["seq"],
                    "triggerAt": card_row["trigger_at"],
                    "title": card_row["title"],
                    "body": card_row["body"],
                    "severity": card_row["severity"],
                    "revealedAt": current,
                })
            await database.db.commit()
            # 所有卡片均已揭示 -> 自动结束（在锁内冻结时钟，锁外发事件）
            left = await database.fetch_one(
                "SELECT COUNT(*) AS n FROM cards WHERE room_code=? AND revealed=0",
                (room["code"],),
            )
            total = await database.fetch_one(
                "SELECT COUNT(*) AS n FROM cards WHERE room_code=?", (room["code"],)
            )
            if total is not None and total["n"] > 0 and left is not None and left["n"] == 0:
                room = await _persist_status(room_dict(latest), "finished")
                finish_after = True
            else:
                room = room_dict(latest)

        # 锁外逐条发事件，保持 id 与 (trigger_at, seq) 一致的稳定顺序
        for card in revealed_cards:
            await emit("card_revealed", room, {"card": card})
        if finish_after:
            await emit("room_state", room, clock_payload(room))


async def scheduler_loop() -> None:
    while True:
        try:
            await scheduler_tick()
        except Exception as exc:  # 后台任务不能因单次错误退出
            print(f"[scheduler] {exc!r}")
        await asyncio.sleep(TICK_INTERVAL)


async def heartbeat_loop() -> None:
    """周期性向所有在线连接下发权威虚拟时钟（暂停时 elapsed 自然冻结）。"""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        for code in list(hub.rooms.keys()):
            row = await get_room_row(code)
            if row is None:
                continue
            room = room_dict(row)
            msg = {"kind": "clock", "clock": clock_payload(room)}
            for sub in list(hub.rooms.get(code, ())):  # type: Subscriber
                if sub.run_id != room["runId"]:
                    continue
                try:
                    sub.queue.put_nowait(msg)
                except asyncio.QueueFull:
                    pass


# --------------------------------------------------------------------------- #
# 快照（重连时的权威状态）
# --------------------------------------------------------------------------- #

async def load_events(code: str, run_id: int, since: int = 0) -> list[dict[str, Any]]:
    rows = await database.fetch_all(
        "SELECT id, type, payload FROM events "
        "WHERE room_code=? AND run_id=? AND id>? ORDER BY id ASC",
        (code, run_id, since),
    )
    return [
        {"id": r["id"], "type": r["type"], "payload": json.loads(r["payload"]), "runId": run_id}
        for r in rows
    ]


async def build_snapshot(room: dict[str, Any], role: str) -> dict[str, Any]:
    if role == "host":
        card_rows = await database.fetch_all(
            "SELECT * FROM cards WHERE room_code=? ORDER BY trigger_at ASC, seq ASC",
            (room["code"],),
        )
        cards = [
            {
                "id": r["id"],
                "seq": r["seq"],
                "triggerAt": r["trigger_at"],
                "title": r["title"],
                "body": r["body"],
                "severity": r["severity"],
                "revealed": bool(r["revealed"]),
                "revealedAt": r["revealed_at"],
            }
            for r in card_rows
        ]
    else:
        card_rows = await database.fetch_all(
            "SELECT * FROM cards WHERE room_code=? AND revealed=1 "
            "ORDER BY trigger_at ASC, seq ASC",
            (room["code"],),
        )
        cards = [
            {
                "id": r["id"],
                "seq": r["seq"],
                "triggerAt": r["trigger_at"],
                "title": r["title"],
                "body": r["body"],
                "severity": r["severity"],
                "revealedAt": r["revealed_at"],
            }
            for r in card_rows
        ]

    last_id_row = await database.fetch_one(
        "SELECT COALESCE(MAX(id), 0) AS m FROM events WHERE room_code=? AND run_id=?",
        (room["code"], room["runId"]),
    )
    last_event_id = int(last_id_row["m"]) if last_id_row else 0
    all_events = await load_events(room["code"], room["runId"])
    if role == "host":
        events = all_events
    else:
        events = [e for e in all_events if e["type"] not in HOST_ONLY_EVENTS]

    return {
        "kind": "snapshot",
        "role": role,
        "lastEventId": last_event_id,
        "room": {
            "code": room["code"],
            "name": room["name"],
            "status": room["status"],
            "runId": room["runId"],
        },
        "clock": clock_payload(room),
        "participants": hub.count_participants(room["code"]),
        "cards": cards,
        "events": events,
    }


# --------------------------------------------------------------------------- #
# 请求模型
# --------------------------------------------------------------------------- #

class CreateRoomIn(BaseModel):
    name: str = Field(default="", max_length=80)


class CardIn(BaseModel):
    triggerAt: float = Field(ge=0, le=24 * 3600)
    title: str = Field(min_length=1, max_length=120)
    body: str = Field(default="", max_length=4000)
    severity: str = Field(default="major")

    @field_validator("severity")
    @classmethod
    def _severity_ok(cls, v: str) -> str:
        if v not in SEVERITIES:
            raise ValueError(f"severity 必须是 {SEVERITIES} 之一")
        return v


class RevealIn(BaseModel):
    cardId: int


# --------------------------------------------------------------------------- #
# FastAPI
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即建表（依赖安装 + 数据库初始化都纳入容器启动）
    await database.init_db()
    await database.connect()
    tasks = [asyncio.create_task(scheduler_loop()), asyncio.create_task(heartbeat_loop())]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await database.close()


app = FastAPI(title="事故演练情景注入台", lifespan=lifespan)


async def auth_host(code: str, host_token: Optional[str]) -> dict[str, Any]:
    if not host_token:
        raise HTTPException(status_code=401, detail="缺少主持人令牌 X-Host-Token")
    row = await get_room_row(code)
    if row is None:
        raise HTTPException(status_code=404, detail="房间不存在")
    room = room_dict(row)
    if not secrets.compare_digest(host_token, room["hostToken"]):
        raise HTTPException(status_code=403, detail="主持人令牌无效")
    return room


@app.exception_handler(HTTPException)
async def http_exc_handler(request, exc: HTTPException):  # noqa: ANN001
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


# ---- 房间 ------------------------------------------------------------------ #

@app.post("/api/rooms")
async def create_room(body: CreateRoomIn) -> dict[str, Any]:
    async with write_lock:
        while True:
            code = secrets.token_hex(2).upper()  # 4 位大写房间码
            exists = await get_room_row(code)
            if exists is None:
                break
        host_token = secrets.token_urlsafe(18)
        ts = now_ts()
        assert database.db is not None
        name = body.name.strip() or "桌面事故演练"
        await database.db.execute(
            "INSERT INTO rooms(code, name, host_token, status, run_id, created_at) "
            "VALUES (?, ?, ?, 'pending', 1, ?)",
            (code, name, host_token, ts),
        )
        await database.db.commit()
        await insert_event(code, 1, "room_created", {"name": name})
    return {"code": code, "hostToken": host_token, "name": name,
            "status": "pending", "joinUrl": f"/room.html?code={code}"}


@app.get("/api/rooms/{code}")
async def room_status(code: str) -> dict[str, Any]:
    """参与者入房前校验房间码（只暴露非敏感状态）。"""
    row = await get_room_row(code.upper())
    if row is None:
        raise HTTPException(status_code=404, detail="房间不存在")
    room = room_dict(row)
    return {
        "code": room["code"],
        "name": room["name"],
        "status": room["status"],
        "runId": room["runId"],
        "participants": hub.count_participants(room["code"]),
    }


# ---- 主持人：时钟控制（全部幂等） ------------------------------------------- #

@app.post("/api/rooms/{code}/start")
async def start_room(code: str, x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] == "pending":
        room = await set_status(room, "running")
    return {"clock": clock_payload(room)}


@app.post("/api/rooms/{code}/pause")
async def pause_room(code: str, x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] == "running":
        room = await set_status(room, "paused")
    return {"clock": clock_payload(room)}


@app.post("/api/rooms/{code}/resume")
async def resume_room(code: str, x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] == "paused":
        room = await set_status(room, "running")
    return {"clock": clock_payload(room)}


@app.post("/api/rooms/{code}/finish")
async def finish_room(code: str, x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] in ("running", "paused"):
        room = await set_status(room, "finished")
    return {"clock": clock_payload(room)}


@app.post("/api/rooms/{code}/reset")
async def reset_room(code: str, x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    """重置演练：时钟归零、揭示清空、事件按新 run 重新开始（旧事件作废，杜绝回退）。"""
    room = await auth_host(code.upper(), x_host_token)
    async with write_lock:
        assert database.db is not None
        await database.db.execute(
            "UPDATE cards SET revealed=0, revealed_at=NULL WHERE room_code=?",
            (room["code"],),
        )
        await database.db.execute(
            "UPDATE rooms SET status='pending', run_id=run_id+1, started_at=NULL, "
            "last_resume_at=NULL, base_elapsed=0 WHERE code=?",
            (room["code"],),
        )
        await database.db.commit()
        row = await get_room_row(room["code"])
        assert row is not None
        room = room_dict(row)
        reset_id = await insert_event(room["code"], room["runId"], "room_reset", {})
    # 让所有仍在旧 run 的连接也收到重置通知，由其重新拉取新 run 快照
    await hub.publish_old_run(
        room["code"], room["runId"],
        {"kind": "event",
         "event": {"id": reset_id, "type": "room_reset", "payload": {},
                   "runId": room["runId"]}},
    )
    return {"clock": clock_payload(room), "runId": room["runId"]}


# ---- 主持人：卡片编排 ------------------------------------------------------- #

@app.post("/api/rooms/{code}/cards")
async def create_card(code: str, body: CardIn,
                      x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] not in ("pending", "paused"):
        raise HTTPException(status_code=409, detail="演练进行中不能新增卡片，请先暂停")
    assert database.db is not None
    async with write_lock:
        seq_row = await database.fetch_one(
            "SELECT COALESCE(MAX(seq), -1) AS m FROM cards WHERE room_code=?", (room["code"],)
        )
        seq = int(seq_row["m"]) + 1 if seq_row else 0
        cur = await database.db.execute(
            "INSERT INTO cards(room_code, seq, trigger_at, title, body, severity) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (room["code"], seq, body.triggerAt, body.title.strip(), body.body.strip(), body.severity),
        )
        await database.db.commit()
        card_id = int(cur.lastrowid)
        payload = {"card": {
            "id": card_id, "seq": seq, "triggerAt": body.triggerAt,
            "title": body.title.strip(), "body": body.body.strip(),
            "severity": body.severity, "revealed": False, "revealedAt": None,
        }}
        event_id = await insert_event(room["code"], room["runId"], "card_created", payload)
    await push_event(room["code"], room["runId"], event_id, "card_created", payload)
    return payload["card"]


@app.delete("/api/rooms/{code}/cards/{card_id}")
async def delete_card(code: str, card_id: int,
                      x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    room = await auth_host(code.upper(), x_host_token)
    if room["status"] not in ("pending", "paused"):
        raise HTTPException(status_code=409, detail="演练进行中不能删除卡片，请先暂停")
    row = await database.fetch_one(
        "SELECT * FROM cards WHERE id=? AND room_code=?", (card_id, room["code"])
    )
    if row is None:
        raise HTTPException(status_code=404, detail="卡片不存在")
    if row["revealed"]:
        raise HTTPException(status_code=409, detail="已揭示的卡片不能删除（状态不可回退）")
    assert database.db is not None
    async with write_lock:
        await database.db.execute("DELETE FROM cards WHERE id=?", (card_id,))
        await database.db.commit()
        payload = {"cardId": card_id}
        event_id = await insert_event(room["code"], room["runId"], "card_deleted", payload)
    await push_event(room["code"], room["runId"], event_id, "card_deleted", payload)
    return {"ok": True}


@app.post("/api/rooms/{code}/reveal")
async def reveal_now(code: str, body: RevealIn,
                     x_host_token: Optional[str] = Header(default=None)) -> dict[str, Any]:
    """主持人手动立即揭示（注入），即便演练已暂停也允许。"""
    room = await auth_host(code.upper(), x_host_token)
    row = await database.fetch_one(
        "SELECT * FROM cards WHERE id=? AND room_code=?", (body.cardId, room["code"])
    )
    if row is None:
        raise HTTPException(status_code=404, detail="卡片不存在")
    if row["revealed"]:
        raise HTTPException(status_code=409, detail="卡片已揭示，不能重复揭示")
    if room["status"] not in ("running", "paused"):
        raise HTTPException(status_code=409, detail="演练未开始，无法揭示")
    assert database.db is not None
    async with write_lock:
        stamp = virtual_now(room)
        await database.db.execute(
            "UPDATE cards SET revealed=1, revealed_at=? WHERE id=? AND revealed=0",
            (stamp, body.cardId),
        )
        await database.db.commit()
        payload = {"card": {
            "id": row["id"], "seq": row["seq"], "triggerAt": row["trigger_at"],
            "title": row["title"], "body": row["body"], "severity": row["severity"],
            "revealedAt": stamp, "manual": True,
        }}
        event_id = await insert_event(room["code"], room["runId"], "card_revealed", payload)
    await push_event(room["code"], room["runId"], event_id, "card_revealed", payload)
    return payload["card"]


# ---- WebSocket：先快照、后增量 --------------------------------------------- #

@app.websocket("/ws/rooms/{code}")
async def ws_room(websocket: WebSocket, code: str,
                  host_token: Optional[str] = Query(default=None),
                  since: int = Query(default=0),
                  pid: Optional[str] = Query(default=None)) -> None:
    code = code.upper()
    row = await get_room_row(code)
    if row is None:
        await websocket.close(code=4404)
        return
    room = room_dict(row)
    is_host = bool(host_token) and secrets.compare_digest(host_token, room["hostToken"])
    if host_token and not is_host:
        await websocket.close(code=4403)
        return
    role = "host" if is_host else "participant"

    await websocket.accept()
    participant_id = pid or secrets.token_hex(4)
    sub = Subscriber(role, participant_id, room["runId"])
    hub.join(code, sub)

    # 1) 权威快照（重连时以服务端为准重建全部状态）
    await websocket.send_json(await build_snapshot(room, role))

    # 2) 若客户端带 since，补发缺失增量（快照之后产生的事件仍走队列，不会丢）
    if since > 0:
        missed = await load_events(code, room["runId"], since)
        if role == "participant":
            missed = [e for e in missed if e["type"] not in HOST_ONLY_EVENTS]
        for ev in missed:
            await websocket.send_json({"kind": "event", "event": ev})

    joined = False
    if role == "participant":
        joined = True
        await emit("participant_joined", room,
                   {"participantId": participant_id,
                    "participants": hub.count_participants(code)})

    sender_task: Optional[asyncio.Task] = None

    async def pump_out() -> None:
        while True:
            msg = await sub.queue.get()
            if msg is None:
                return
            await websocket.send_json(msg)

    async def pump_in() -> None:
        # 客户端只允许发心跳/确认，任何消息都不改变服务端状态
        while True:
            await websocket.receive_json()

    sender_task = asyncio.create_task(pump_out())
    receiver_task = asyncio.create_task(pump_in())
    try:
        done, pending = await asyncio.wait(
            {sender_task, receiver_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    except WebSocketDisconnect:
        pass
    finally:
        hub.leave(code, sub)
        if joined:
            latest = await get_room_row(code)
            if latest is not None:
                await emit("participant_left", room_dict(latest),
                           {"participantId": participant_id,
                            "participants": hub.count_participants(code)})


# ---- 静态前端 ---------------------------------------------------------------- #

@app.get("/")
async def index() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/host.html")
async def host_page() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "host.html"))


@app.get("/room.html")
async def room_page() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "room.html"))
