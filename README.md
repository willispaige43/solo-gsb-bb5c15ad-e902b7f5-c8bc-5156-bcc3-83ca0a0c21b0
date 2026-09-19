# 桌面事故演练 · 实时情景注入台

主持人按**相对触发秒数**编排注入卡片并启动演练；服务端维护可**暂停 / 继续**的虚拟时钟，
按「时间 + 序号」稳定揭示卡片，**暂停期间时钟与揭示一律冻结**。参与者凭 4 位**房间码**
实时收看；断线重连时**先恢复服务端权威快照，再续传增量事件**，同轮内事件按 id 单调应用，
**状态绝不回退**。

技术栈：FastAPI + SQLite（aiosqlite，WAL）+ 原生 HTML/CSS/JS（无构建、无前端框架），
实时通道为 WebSocket。

---

## 一条命令启动

```bash
docker compose up --build -d
```

容器启动时会自动安装依赖并**幂等建表初始化数据库**（`app/db.py:init_db`），无需任何手工步骤。

启动后打开：

| 用途 | 地址 |
| --- --- | --- |
| 入口（创建房间 / 凭码进入） | <http://localhost:8000/> |
| 主持人控制台 | `http://localhost:8000/host.html?code=<房间码>`（创建后自动跳转） |
| 参与者视图 | `http://localhost:8000/room.html?code=<房间码>` |

- **Web 地址**：`http://<主机>:8000/`
- **监听端口**：容器内 `8000`，宿主机默认映射 `8000`（修改 `docker-compose.yml` 的 `"8000:8000"` 左侧即可）
- **数据库**：容器内 `/data/drill.db`，通过命名卷 `drill-data` 持久化

停止与清理：

```bash
docker compose down          # 停止（保留数据卷）
docker compose down -v       # 停止并删除演练数据
```

### 不用 Docker 的本地启动（可选）

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 必要配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DB_PATH` | 镜像内 `/data/drill.db`；本地默认 `./data/drill.db` | SQLite 数据库文件路径，父目录不存在会自动创建 |

其余均为零配置：房间码（4 位）与主持人令牌在创建房间时由服务端随机生成，
主持人令牌仅保存在主持人浏览器的 `localStorage` 中，并通过 `X-Host-Token` 头鉴权。

---

## 使用流程

1. 主持人打开 `/`，填写演练名称 → **创建演练房间**，自动进入控制台。
2. 在左侧按 **触发秒数（T+秒）** 创建卡片（标题 / 严重程度 / 描述），可随时增删未揭示卡片（演练进行中需先暂停）。
3. 点击 **▶ 启动演练**：虚拟时钟从 `00:00:00` 开始走动，到达卡片触发秒数时自动揭示并广播给所有参与者。
4. **⏸ 暂停 / ⏵ 继续**：暂停时虚拟时钟立刻冻结，到期检查与揭示完全停止；继续后从冻结点接着走。
5. **立即揭示**：任意卡片可由主持人手动注入（暂停时也可），标记为「手动注入」。
6. **↺ 重置**：时钟归零、揭示清空、开启新的一轮（`run_id` 递增，旧事件作废，参与者自动重连拉取新快照）。
7. 参与者在 `/` 输入房间码进入，只读查看注入时间线；刷新 / 断网 / 休眠后重连，自动以服务端快照恢复并续传。

## 一致性保证（设计要点）

- **虚拟时钟**：房间记录 `base_elapsed + last_resume_at`，仅 `running` 状态随墙钟推进；
  `paused/finished/pending` 完全冻结。状态迁移全部幂等。
- **稳定揭示**：调度器每 100ms 扫描 `running` 房间，按 `(trigger_at ASC, seq ASC)` 揭示；
  写库前重新读取状态与最新时钟，暂停边界不会多揭示；所有卡片揭示后自动结束。
- **先快照、后增量**：WebSocket 建立后第一条消息恒为 `snapshot`（房间、时钟、卡片、事件、
  最后事件 id），之后才是 `event` 增量；重连可带 `since=<最后事件id>` 补缺。
- **不回退**：同一 `run_id` 内客户端只接受 id 严格增大的事件；重置会产生新 `run_id`，
  旧连接收到 `room_reset` 后强制以新 run 快照重建；已揭示卡片不可删除、不可重复揭示。
- **角色隔离**：卡片创建 / 删除等编排信息只推送给主持人，参与者只能看到已揭示内容。
- **心跳校准**：服务端每 2 秒下发权威时钟，客户端本地插值平滑走秒并定期纠偏。

## 目录结构

```
app/
  main.py        # FastAPI：REST、WebSocket 枢纽、虚拟时钟、揭示调度器
  db.py          # SQLite 连接与幂等建表（启动时执行）
static/
  index.html     # 入口页
  host.html/.js  # 主持人控制台
  room.html/.js  # 参与者时间线
  common.js      # API 封装、权威时钟、自动重连 WS 客户端
  style.css
Dockerfile / docker-compose.yml / requirements.txt
tests/e2e_check.py  # 行为验证脚本（需先启动服务：DB_PATH=/tmp/x.db uvicorn app.main:app --port 8011）
```

## 主要 HTTP / WS 接口

- `POST /api/rooms` 创建房间 → `{code, hostToken}`
- `GET  /api/rooms/{code}` 校验房间码（参与者入房）
- `POST /api/rooms/{code}/start|pause|resume|finish|reset`（需 `X-Host-Token`，均幂等）
- `POST /api/rooms/{code}/cards`、`DELETE /api/rooms/{code}/cards/{id}`（需令牌）
- `POST /api/rooms/{code}/reveal` 手动立即揭示（需令牌）
- `WS   /ws/rooms/{code}?host_token=...&since=...&pid=...`：主持人带令牌，参与者不带
