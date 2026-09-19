/* 公共工具：API 封装、格式化、权威虚拟时钟、自动重连 WebSocket。 */

const $ = (sel) => document.querySelector(sel);

function qs(name) {
  return new URLSearchParams(location.search).get(name);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let data = null;
  try { data = await res.json(); } catch (_) { /* 非 JSON */ }
  if (!res.ok) {
    throw new Error((data && data.error) || `请求失败 (${res.status})`);
  }
  return data;
}

const hostHeaders = (token) => ({ "X-Host-Token": token });

function pad2(n) { return String(n).padStart(2, "0"); }

function fmtClock(seconds) {
  seconds = Math.max(0, seconds || 0);
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${pad2(h)}:${pad2(m)}:${pad2(s)}`;
}

function fmtFull(seconds) {
  const base = fmtClock(seconds);
  const frac = Math.floor((seconds % 1) * 10);
  return `${base}.${frac}`;
}

const SEVERITY_LABEL = {
  info: "提示",
  minor: "一般",
  major: "较大",
  critical: "重大",
};

const STATUS_LABEL = {
  pending: "待开始",
  running: "进行中",
  paused: "已暂停",
  finished: "已结束",
};

let toastTimer = null;
function toast(msg) {
  let el = $("#toast");
  if (!el) {
    el = document.createElement("div");
    el.id = "toast";
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove("show"), 2200);
}

/* 权威虚拟时钟：服务端心跳每 2s 校准，本地按墙钟插值平滑走秒，暂停即冻结。 */
const ClockState = {
  status: "pending",
  baseElapsed: 0,
  lastResumeAt: null,
  runId: 0,

  sync(clock) {
    this.status = clock.status;
    this.baseElapsed = clock.baseElapsed;
    this.lastResumeAt = clock.lastResumeAt;
    this.runId = clock.runId;
    this._serverNow = clock.serverTime;
    this._localNow = Date.now() / 1000;
  },

  elapsed() {
    if (this.status !== "running" || this.lastResumeAt == null) {
      return this.baseElapsed;
    }
    const skew = (Date.now() / 1000) - (this._localNow || 0);
    return this.baseElapsed + ((this._serverNow || 0) + skew - this.lastResumeAt);
  },
};

/**
 * 自动重连的房间 WebSocket。
 * 协议保证：连接后先收 snapshot（权威全量），之后收 event（增量）。
 * 重连时带 since=lastEventId 与当前 runId，服务端先补快照，客户端绝不回退。
 */
class RoomSocket {
  constructor({ code, hostToken = null, pid = null, onSnapshot, onEvent, onClock, onStatus }) {
    this.code = code;
    this.hostToken = hostToken;
    this.pid = pid;
    this.onSnapshot = onSnapshot;
    this.onEvent = onEvent;
    this.onClock = onClock;
    this.onStatus = onStatus;
    this.lastEventId = 0;
    this.runId = null;
    this.ws = null;
    this.closedByUser = false;
    this.retry = 0;
  }

  connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const params = new URLSearchParams();
    if (this.hostToken) params.set("host_token", this.hostToken);
    if (this.pid) params.set("pid", this.pid);
    if (this.lastEventId > 0) params.set("since", this.lastEventId);

    this.onStatus && this.onStatus("connecting");
    const ws = new WebSocket(
      `${proto}://${location.host}/ws/rooms/${encodeURIComponent(this.code)}?${params}`
    );
    this.ws = ws;

    ws.onopen = () => {
      this.retry = 0;
      this.onStatus && this.onStatus("online");
    };

    ws.onmessage = (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (_) { return; }

      if (msg.kind === "snapshot") {
        // 权威快照：整体替换（含新 run 的重置场景），并更新水位
        this.runId = msg.clock.runId;
        this.lastEventId = msg.lastEventId;
        ClockState.sync(msg.clock);
        this.onSnapshot && this.onSnapshot(msg);
        return;
      }

      if (msg.kind === "clock") {
        ClockState.sync(msg.clock);
        this.onClock && this.onClock(msg.clock);
        return;
      }

      if (msg.kind === "overflow") {
        // 队列积压：立即重连，以快照追平，避免乱序
        this._reconnectSoon(0);
        return;
      }

      if (msg.kind === "event") {
        const e = msg.event;
        // 重置：旧 run 的重置事件 -> 立刻重连，拉取新 run 的权威快照
        if (e.type === "room_reset" && this.runId != null && e.runId !== this.runId) {
          this.lastEventId = 0;
          this._reconnectSoon(60);
          return;
        }
        // 同 run 内按 id 单调应用，状态不得回退
        if (e.id >= 0 && e.id <= this.lastEventId) return;
        if (this.runId != null && e.runId !== this.runId) return;
        if (e.id >= 0) this.lastEventId = e.id;
        this.onEvent && this.onEvent(e);
      }
    };

    ws.onclose = () => {
      this.onStatus && this.onStatus("offline");
      if (!this.closedByUser) this._reconnectSoon();
    };

    ws.onerror = () => {
      try { ws.close(); } catch (_) { /* ignore */ }
    };
  }

  _reconnectSoon(delay) {
    if (this.ws) {
      try { this.ws.close(); } catch (_) { /* ignore */ }
    }
    if (delay == null) {
      this.retry += 1;
      delay = Math.min(1000 * Math.pow(1.6, Math.min(this.retry, 5)), 8000);
    }
    setTimeout(() => {
      if (!this.closedByUser) this.connect();
    }, delay);
  }

  close() {
    this.closedByUser = true;
    try { this.ws && this.ws.close(); } catch (_) { /* ignore */ }
  }
}

/* 参与者本机稳定 id，便于重连时服务端识别同一人（存在 localStorage）。 */
function getParticipantId() {
  let pid = localStorage.getItem("drill.pid");
  if (!pid) {
    pid = "p" + Math.random().toString(16).slice(2, 10);
    localStorage.setItem("drill.pid", pid);
  }
  return pid;
}

function escapeHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
