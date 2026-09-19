/* 主持人控制台：时钟控制 + 卡片编排。状态以服务端快照/事件为唯一权威。 */

const code = qs("code");
const token = code && localStorage.getItem(`drill.host.${code}`);

if (!code) {
  location.href = "/";
} else if (!token) {
  alert("缺少该房间的主持人令牌，请在创建房间的同一浏览器打开，或重新创建房间。");
  location.href = "/";
}

const state = {
  room: null,
  cards: new Map(), // id -> card
};

function cardHtml(c) {
  const revealed = !!c.revealed;
  const stamp = c.revealedAt != null ? fmtClock(c.revealedAt) : null;
  return `
    <div class="card-item sev-${c.severity} ${revealed ? "revealed" : ""}" data-id="${c.id}">
      <div class="card-head">
        <span class="card-time">T+${Number(c.triggerAt).toFixed(1)}s · #${c.seq}</span>
        <span class="tag sev-${c.severity}">${SEVERITY_LABEL[c.severity] || c.severity}</span>
        ${c.manual ? '<span class="tag manual-tag">手动注入</span>' : ""}
        ${revealed ? '<span class="tag revealed-tag">已揭示 ' + fmtClock(stamp) + "</span>" : ""}
        <span style="flex:1"></span>
      </div>
      <div class="card-title" style="margin-top:6px;">${escapeHtml(c.title)}</div>
      ${c.body ? `<div class="card-body">${escapeHtml(c.body)}</div>` : ""}
      <div class="card-foot">
        <span style="flex:1"></span>
        ${revealed
          ? ""
          : `<button class="btn small warn" data-act="reveal" data-id="${c.id}">立即揭示</button>
             <button class="btn small danger" data-act="delete" data-id="${c.id}">删除</button>`}
      </div>
    </div>`;
}

function renderCards(flashId) {
  const list = [...state.cards.values()].sort(
    (a, b) => a.triggerAt - b.triggerAt || a.seq - b.seq
  );
  const el = $("#cardList");
  if (!list.length) {
    el.innerHTML = `<div class="empty-state">还没有卡片。在左侧按“触发秒数”创建第一张注入情景。</div>`;
    return;
  }
  el.innerHTML = list.map(cardHtml).join("");
  if (flashId) {
    const node = el.querySelector(`[data-id="${flashId}"]`);
    if (node) node.classList.add("flash-new");
  }
}

function renderRoom() {
  if (!state.room) return;
  $("#roomName").textContent = state.room.name;
  $("#roomCode").textContent = state.room.code;
  const badge = $("#statusBadge");
  badge.textContent = STATUS_LABEL[state.room.status];
  badge.className = `badge ${state.room.status}`;
  refreshButtons();
}

function refreshButtons() {
  const s = state.room ? state.room.status : "pending";
  $("#btnStart").disabled = s !== "pending";
  $("#btnPause").disabled = s !== "running";
  $("#btnResume").disabled = s !== "paused";
  $("#btnFinish").disabled = !(s === "running" || s === "paused");
  const editing = s === "pending" || s === "paused";
  $("#btnAddCard").disabled = !editing;
  $("#fTime").disabled = !editing;
  $("#fTitle").disabled = !editing;
  $("#fSeverity").disabled = !editing;
  $("#fBody").disabled = !editing;
}

function renderClock() {
  const el = $("#clock");
  const s = ClockState.status;
  el.textContent = fmtClock(ClockState.elapsed());
  el.className = "clock" + (s === "paused" ? " paused" : s === "finished" ? " finished" : "");
}

setInterval(renderClock, 200);

/* ---------- 快照与增量 ---------- */

function applySnapshot(snap) {
  state.room = snap.room;
  state.cards = new Map(snap.cards.map((c) => [c.id, c]));
  $("#participantCount").textContent = snap.participants;
  renderRoom();
  renderCards();
}

function applyEvent(ev) {
  const p = ev.payload || {};
  switch (ev.type) {
    case "card_created":
      state.cards.set(p.card.id, p.card);
      renderCards(p.card.id);
      break;
    case "card_deleted":
      state.cards.delete(p.cardId);
      renderCards();
      break;
    case "card_revealed": {
      const existing = state.cards.get(p.card.id);
      if (existing && existing.revealed) break; // 已揭示绝不回退/重复
      state.cards.set(p.card.id, { ...(existing || {}), ...p.card, revealed: true });
      renderCards(p.card.id);
      toast(`已注入：${p.card.title}`);
      break;
    }
    case "room_state":
      ClockState.sync({
        status: p.status,
        baseElapsed: p.baseElapsed,
        lastResumeAt: p.lastResumeAt,
        serverTime: p.serverTime,
        runId: p.runId,
      });
      if (state.room) state.room.status = p.status;
      renderRoom();
      renderClock();
      if (p.status === "paused") toast("时钟已暂停");
      if (p.status === "running") toast("时钟继续/启动");
      if (p.status === "finished") toast("演练已结束");
      break;
    case "participant_joined":
    case "participant_left":
      $("#participantCount").textContent = p.participants;
      break;
    default:
      break;
  }
}

/* ---------- WebSocket ---------- */

const sock = new RoomSocket({
  code,
  hostToken: token,
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

/* ---------- 主持人操作 ---------- */

async function control(act) {
  try {
    await api(`/api/rooms/${code}/${act}`, { method: "POST", headers: hostHeaders(token) });
  } catch (e) {
    toast(e.message);
  }
}

$("#btnStart").onclick = () => control("start");
$("#btnPause").onclick = () => control("pause");
$("#btnResume").onclick = () => control("resume");
$("#btnFinish").onclick = () => {
  if (confirm("确定结束本轮演练？时钟将冻结在当前时刻。")) control("finish");
};
$("#btnReset").onclick = () => {
  if (confirm("重置将清空全部揭示并把时钟归零（新一轮），参与者视图自动刷新。确定？")) {
    control("reset");
  }
};

$("#btnCopyLink").onclick = async () => {
  const url = `${location.origin}/room.html?code=${code}`;
  try {
    await navigator.clipboard.writeText(url);
    toast("参与者链接已复制：" + url);
  } catch (_) {
    prompt("复制参与者链接：", url);
  }
};

$("#btnAddCard").onclick = async () => {
  const triggerAt = parseFloat($("#fTime").value);
  const title = $("#fTitle").value.trim();
  if (!(triggerAt >= 0)) return toast("请输入不小于 0 的触发秒数");
  if (!title) return toast("请填写情景标题");
  try {
    await api(`/api/rooms/${code}/cards`, {
      method: "POST",
      headers: hostHeaders(token),
      body: JSON.stringify({
        triggerAt,
        title,
        body: $("#fBody").value.trim(),
        severity: $("#fSeverity").value,
      }),
    });
    $("#fTitle").value = "";
    $("#fBody").value = "";
  } catch (e) {
    toast(e.message);
  }
};

$("#cardList").addEventListener("click", async (e) => {
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const id = parseInt(btn.dataset.id, 10);
  const act = btn.dataset.act;
  try {
    if (act === "delete") {
      if (!confirm("删除这张未揭示的卡片？")) return;
      await api(`/api/rooms/${code}/cards/${id}`, {
        method: "DELETE",
        headers: hostHeaders(token),
      });
    } else if (act === "reveal") {
      await api(`/api/rooms/${code}/reveal`, {
        method: "POST",
        headers: hostHeaders(token),
        body: JSON.stringify({ cardId: id }),
      });
    }
  } catch (err) {
    toast(err.message);
  }
});
