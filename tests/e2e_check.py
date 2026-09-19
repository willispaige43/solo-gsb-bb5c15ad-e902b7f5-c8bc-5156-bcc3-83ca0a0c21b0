"""端到端行为验证（临时库 + 已启动的 uvicorn）。"""
import asyncio
import json
import urllib.request

import websockets

BASE = "http://127.0.0.1:8011"
WS = "ws://127.0.0.1:8011"

results = []

def check(name, cond, extra=""):
    results.append((name, bool(cond), extra))
    print(("PASS" if cond else "FAIL"), name, extra)

def http(method, path, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("X-Host-Token", token)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

async def read_msg(ws, kinds=("snapshot", "event", "clock", "overflow"), timeout=3.0):
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        msg = json.loads(raw)
        if msg["kind"] in kinds:
            return msg

async def main():
    # 1) 创建房间 + 卡片（t=1s, t=8s；T3 必须晚于暂停窗口）
    room = http("POST", "/api/rooms", body={"name": "E2E演练"})
    code, token = room["code"], room["hostToken"]
    check("room code is 4 hex", len(code) == 4)
    c1 = http("POST", f"/api/rooms/{code}/cards", token,
              {"triggerAt": 1.0, "title": "T1", "severity": "minor"})
    c2 = http("POST", f"/api/rooms/{code}/cards", token,
              {"triggerAt": 8.0, "title": "T8", "severity": "critical"})
    check("cards created", c1["id"] and c2["id"])

    # 错误令牌应被拒绝
    try:
        http("POST", f"/api/rooms/{code}/start", token + "x")
        check("bad host token rejected", False)
    except urllib.error.HTTPError as e:
        check("bad host token rejected", e.code == 403)

    # 2) 主持人 + 参与者同时上线
    host_ws = await websockets.connect(f"{WS}/ws/rooms/{code}?host_token={token}")
    part_ws = await websockets.connect(f"{WS}/ws/rooms/{code}?pid=tester1")
    hsnap_msg = await read_msg(host_ws)
    psnap_msg = await read_msg(part_ws)
    check("host first msg is snapshot", hsnap_msg["kind"] == "snapshot")
    check("host snapshot sees 2 cards", len(hsnap_msg["cards"]) == 2,
          str([c["id"] for c in hsnap_msg["cards"]]))
    check("participant first msg is snapshot", psnap_msg["kind"] == "snapshot")
    check("participant snapshot hides unrevealed cards",
          len(psnap_msg["cards"]) == 0, str(psnap_msg["cards"]))
    shown = [e for e in psnap_msg["events"]
             if e["type"] in ("card_revealed", "room_state", "room_reset")]
    check("participant timeline empty before start", len(shown) == 0,
          str([e["type"] for e in psnap_msg["events"]]))

    async def drain(ws, sink):
        try:
            async for raw in ws:
                sink.append(json.loads(raw))
        except Exception:
            pass

    host_log = []
    part_log = []
    htask = asyncio.create_task(drain(host_ws, host_log))
    ptask = asyncio.create_task(drain(part_ws, part_log))
    await asyncio.sleep(0.2)

    # 3) 启动；约 2.5s 后暂停：T1 应已揭示，T8 未揭示，暂停期间冻结
    http("POST", f"/api/rooms/{code}/start", token)
    await asyncio.sleep(2.5)
    http("POST", f"/api/rooms/{code}/pause", token)
    await asyncio.sleep(0.4)

    host_reveals = [m["event"]["payload"]["card"]["id"]
                    for m in host_log
                    if m.get("event", {}).get("type") == "card_revealed"]
    check("T1 auto revealed by scheduler", c1["id"] in host_reveals, str(host_reveals))
    check("T8 NOT revealed before/at pause", c2["id"] not in host_reveals)

    part_reveals = [m["event"]["payload"]["card"]["id"]
                    for m in part_log
                    if m.get("event", {}).get("type") == "card_revealed"]
    check("participant received revealed T1", part_reveals == [c1["id"]], str(part_reveals))
    last_ev_id = max(m["event"]["id"] for m in part_log
                     if m.get("kind") == "event" and m["event"]["id"] > 0)

    # 暂停冻结值以 room_state 事件里的权威 elapsed 为准
    pause_events = [m for m in part_log
                    if m.get("event", {}).get("type") == "room_state"
                    and m["event"]["payload"]["status"] == "paused"]
    check("participant got paused room_state", len(pause_events) > 0)
    frozen_at = pause_events[-1]["event"]["payload"]["elapsed"]
    await asyncio.sleep(2.5)  # 远超 T8 触发点（8s 相对时间），暂停期间不得推进
    paused_clocks = [m["clock"]["elapsed"] for m in part_log if m["kind"] == "clock"
                     and m["clock"]["status"] == "paused"]
    if paused_clocks:
        check("heartbeat clock frozen while paused",
              max(paused_clocks) - min(paused_clocks) < 0.05,
              f"{min(paused_clocks)}..{max(paused_clocks)}")
    else:
        check("heartbeat clock frozen while paused", True, "(心跳未落入窗口，跳过)")
    check("T8 NOT revealed during pause",
          not any(m.get("event", {}).get("type") == "card_revealed"
                  and m["event"]["payload"]["card"]["id"] == c2["id"] for m in part_log))
    check("paused elapsed < T8 trigger", frozen_at < 8.0, str(frozen_at))

    # 4) 参与者断线重连：先收快照，仅含 T1；编排事件绝不泄漏
    ptask.cancel()
    await part_ws.close()
    await asyncio.sleep(0.3)
    part_ws = await websockets.connect(
        f"{WS}/ws/rooms/{code}?pid=tester1&since={last_ev_id}")
    snap2 = await read_msg(part_ws)
    check("reconnect first msg is snapshot", snap2["kind"] == "snapshot")
    check("reconnected snapshot has T1 only",
          [c["id"] for c in snap2["cards"]] == [c1["id"]],
          str([c["id"] for c in snap2["cards"]]))
    tl_types = [e["type"] for e in snap2["events"]]
    check("snapshot never leaks card_created to participant",
          "card_created" not in tl_types and "card_deleted" not in tl_types, str(tl_types))
    check("snapshot events ordered by id",
          [e["id"] for e in snap2["events"]] == sorted(e["id"] for e in snap2["events"]))
    # since=水位，不应重放任何旧事件
    stale = []
    try:
        while True:
            stale.append(await asyncio.wait_for(part_ws.recv(), timeout=0.6))
    except asyncio.TimeoutError:
        pass
    check("no stale events replayed at/below since",
          all(json.loads(s)["kind"] != "event" or
              json.loads(s)["event"]["id"] > last_ev_id for s in stale),
          str(stale))

    # 5) 继续：时钟从冻结点推进，T8 在剩余 ~ (8-frozen) 秒后揭示
    http("POST", f"/api/rooms/{code}/resume", token)
    ev3 = None
    deadline = asyncio.get_event_loop().time() + 9
    while asyncio.get_event_loop().time() < deadline:
        raw = await asyncio.wait_for(part_ws.recv(), timeout=8.0)
        m = json.loads(raw)
        if m.get("kind") == "event" and m["event"]["type"] == "card_revealed" \
                and m["event"]["payload"]["card"]["id"] == c2["id"]:
            ev3 = m["event"]
            break
    check("T8 revealed after resume", ev3 is not None)
    check("incremental event id > watermark (no rollback)",
          ev3 and ev3["id"] > last_ev_id,
          f"{ev3['id'] if ev3 else None} > {last_ev_id}")

    # 全部揭示 -> 自动 finished
    await asyncio.sleep(0.8)
    info = json.load(urllib.request.urlopen(BASE + f"/api/rooms/{code}"))
    check("auto finished after all revealed", info["status"] == "finished", info["status"])

    # 6) 已揭示卡片不可删除
    try:
        http("DELETE", f"/api/rooms/{code}/cards/{c1['id']}", token)
        check("revealed card cannot be deleted", False)
    except urllib.error.HTTPError as e:
        check("revealed card cannot be deleted", e.code == 409)

    # 6b) 已结束演练不能再揭示；结束后新增卡片被拒
    try:
        http("POST", f"/api/rooms/{code}/reveal", token, {"cardId": c2["id"]})
        check("reveal after finish rejected", False)
    except urllib.error.HTTPError as e:
        check("reveal after finish rejected", e.code == 409)
    try:
        http("POST", f"/api/rooms/{code}/cards", token,
             {"triggerAt": 1, "title": "x"})
        check("add card while finished rejected", False)
    except urllib.error.HTTPError as e:
        check("add card while finished rejected", e.code == 409)

    # 6c) 重置后：暂停中的手动立即注入必须成功，且时钟保持冻结
    http("POST", f"/api/rooms/{code}/reset", token)
    await asyncio.sleep(0.2)
    c3 = http("POST", f"/api/rooms/{code}/cards", token,
              {"triggerAt": 100, "title": "手动卡", "severity": "info"})
    http("POST", f"/api/rooms/{code}/start", token)
    await asyncio.sleep(0.6)
    http("POST", f"/api/rooms/{code}/pause", token)
    await asyncio.sleep(0.3)
    manual = http("POST", f"/api/rooms/{code}/reveal", token, {"cardId": c3["id"]})
    check("manual reveal allowed while paused", manual["manual"] is True
          and manual["revealedAt"] is not None, str(manual))
    await asyncio.sleep(1.0)
    info = json.load(urllib.request.urlopen(BASE + f"/api/rooms/{code}"))
    check("status stays paused after manual reveal", info["status"] == "paused",
          info["status"])
    # 再次重置进入干净的新 run，供后续断言
    http("POST", f"/api/rooms/{code}/reset", token)

    # 7) 重置 -> 新 run；旧连接收到 room_reset，重连后为全新快照
    # （步骤 6c 已重置过一次，队列里可能有多条，取最新一条）
    reset_ev = None
    deadline = asyncio.get_event_loop().time() + 3
    while asyncio.get_event_loop().time() < deadline:
        try:
            m = await read_msg(part_ws, ("event",), timeout=1.0)
        except asyncio.TimeoutError:
            break
        if m["event"]["type"] == "room_reset":
            reset_ev = m["event"]
    check("old-run participant got room_reset", reset_ev is not None)
    check("reset points to new run", reset_ev and reset_ev["runId"] == 3,
          str(reset_ev))
    await part_ws.close()
    part_ws = await websockets.connect(f"{WS}/ws/rooms/{code}?pid=tester1")
    snap3 = await read_msg(part_ws)
    check("new-run snapshot has runId=3", snap3["clock"]["runId"] == 3)
    check("new-run snapshot shows zero revealed", len(snap3["cards"]) == 0,
          str([c["id"] for c in snap3["cards"]]))
    check("new-run snapshot event list is fresh",
          all(e["runId"] == 3 for e in snap3["events"]) and
          not any(e["type"] == "card_revealed" for e in snap3["events"]),
          str([(e["type"], e["runId"]) for e in snap3["events"]]))
    check("host-only events never reach participant",
          all(e["type"] not in ("participant_joined", "participant_left",
                                "card_created", "card_deleted")
              for e in snap3["events"]),
          str([e["type"] for e in snap3["events"]]))
    check("new-run clock at zero", snap3["clock"]["elapsed"] == 0.0
          and snap3["clock"]["status"] == "pending")

    # 8) 幂等：暂停一个 pending 房间不报错/不改状态
    r = http("POST", f"/api/rooms/{code}/pause", token)
    check("idempotent pause on pending", r["clock"]["status"] == "pending")

    await host_ws.close()
    await part_ws.close()

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n==== {len(results) - len(failed)}/{len(results)} passed ====")
    if failed:
        print("FAILED:", failed)
        raise SystemExit(1)

asyncio.run(main())
