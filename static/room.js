/* 参与者视图：只读。重连先应用权威快照重建时间线，再单调追加增量事件。 */

const code = qs("code");
if (!code) location.href = "/";

const state = {
  room: null,
  events: [], // 权威全量事件（快照）+ 增量，按 id 升序
  seenIds: new Set(),
};

const SHOW_TYPES = new Set([
  "card_revealed",
  "room_state",
  "room_reset",
]);

function bannerHtml(status) {
  if (status === "pending") {
    return `<div class="banner pending">⏳ 演练尚未开始，请等待主持人启动。页面将自动同步。</div>`;
  }
  if (status === "paused") {
    return `<div class="banner paused">⏸ 演练已暂停，虚拟时钟冻结在 ${fmtClock(ClockState.elapsed())}，等待主持人继续。</div>`;
  }
  if (status === "finished") {
    return `<div class="banner finished">■ 演练已结束，全部注入事件如下。</div>`;
  }
  return ""; // running 不占横幅
}

function eventHtml(ev, isNew) {
  const p = ev.payload || {};
  let timeText;
  let body = "";

  if (ev.type === "card_revealed" && p.card) {
    const c = p.card;
    timeText = `T+${fmtFull(c.revealedAt != null ? c.revealedAt : ClockState.elapsed())}` +
      (c.manual ? " · 手动注入" : "");
    body = `
      <div class="card-item sev-${c.severity}" style="margin:0;">
        <div class="card-head">
          <span class="tag sev-${c.severity}">${SEVERITY_LABEL[c.severity] || c.severity}</span>
          <span class="card-title">${escapeHtml(c.title)}</span>
        </div>
        ${c.body ? `<div class="card-body">${escapeHtml(c.body)}</div>` : ""}
      </div>`;
  } else if (ev.type === "room_state") {
    timeText = `T+${fmtFull(p.elapsed != null ? p.elapsed : ClockState.elapsed())}`;
    const label = STATUS_LABEL[p.status] || p.status;
    const icon = p.status === "running" ? "▶" : p.status === "paused" ? "⏸" : "■";
    body = `<div style="color:var(--muted);font-size:14px;">${icon} 演练状态：${label}</div>`;
  } else if (ev.type === "room_reset") {
    timeText = "新一轮";
    body = `<div style="color:#d8b4fe;font-size:14px;">↺ 主持人已重置演练，注入时间线已重新开始。</div>`;
  } else {
    return "";
  }

  return `
    <div class="tl-item evt-${ev.type} ${isNew ? "flash-new" : ""}">
      <div class="tl-dot"></div>
      <div class="tl-time">${timeText}</div>
      ${body}
    </div>`;
}

function renderTimeline(newIds) {
  const el = $("#timeline");
  const visible = state.events.filter((e) => SHOW_TYPES.has(e.type));
  if (!visible.length) {
    el.innerHTML = `<div class="empty-state">
      ${state.room && state.room.status === "finished"
        ? "本轮演练没有注入事件。"
        : "等待第一条注入情景…"}
    </div>`;
    return;
  }
  // 相邻的重复 room_state（同一 status）折叠，避免时间线噪声
  const rows = [];
  for (const e of visible) {
    if (e.type === "room_state" && rows.length) {
      const prev = rows[rows.length - 1];
      if (prev.type === "room_state" && prev.payload.status === e.payload.status) continue;
    }
    rows.push(e);
  }
  el.innerHTML = `<div class="timeline">${rows
    .map((e) => eventHtml(e, newIds && newIds.has(e.id)))
    .join("")}</div>`;
}

function renderRoom() {
  if (!state.room) return;
  $("#roomName").textContent = state.room.name;
  $("#roomCode").textContent = state.room.code;
  const badge = $("#statusBadge");
  badge.textContent = STATUS_LABEL[state.room.status];
  badge.className = `badge ${state.room.status}`;
  $("#banner").innerHTML = bannerHtml(state.room.status);
}

function renderClock() {
  $("#clock").textContent = fmtClock(ClockState.elapsed());
  const s = ClockState.status;
  $("#clock").className = "clock" + (s === "paused" ? " paused" : s === "finished" ? " finished" : "");
  if (state.room && state.room.status === "paused") {
    $("#banner").innerHTML = bannerHtml("paused");
  }
}
setInterval(renderClock, 200);

/* ---------- 快照（权威）与增量（不回退） ---------- */

function applySnapshot(snap) {
  state.room = snap.room;
  state.events = snap.events.slice().sort((a, b) => a.id - b.id);
  state.seenIds = new Set(state.events.map((e) => e.id));
  renderRoom();
  renderTimeline();
}

function applyEvent(ev) {
  if (state.seenIds.has(ev.id)) return;
  state.seenIds.add(ev.id);
  state.events.push(ev);
  if (ev.type === "room_state" && ev.payload && ev.payload.status) {
    if (state.room) state.room.status = ev.payload.status;
    renderRoom();
  }
  renderTimeline(new Set([ev.id]));
  // 新卡片注入时轻提示（不抢焦点）
  if (ev.type === "card_revealed" && ev.payload.card) {
    toast("新注入：" + ev.payload.card.title);
  }
  if (ev.type === "room_reset") {
    // 正常情况下 reset 属于新 run，会走重连快照；保险兜底
    state.events = [];
    state.seenIds = new Set();
    renderTimeline();
  }
}

/* ---------- WebSocket ---------- */

const sock = new RoomSocket({
  code,
  pid: getParticipantId(),
  onSnapshot: applySnapshot,
  onEvent: applyEvent,
  onClock: () => renderClock(),
  onStatus: (s) => {
    const dot = $("#connDot");
    const txt = $("#connText");
    dot.className = "conn-dot " + (s === "online" ? "online" : s === "offline" ? "offline" : "");
    txt.textContent = s === "online" ? "已连接" : s === "offline" ? "断线重连中…" : "连接中…";
  },
});
sock.connect();
