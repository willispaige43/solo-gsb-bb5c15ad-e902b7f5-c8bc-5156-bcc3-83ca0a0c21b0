# 事故演练情景注入台 —— 单容器镜像
# 启动时由 FastAPI lifespan 自动完成：建表（幂等）+ 挂接调度器，无需额外初始化步骤。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_PATH=/data/drill.db

WORKDIR /app

# 先装依赖，利用层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static

# SQLite 数据目录（compose 中挂载命名卷持久化）
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 依赖安装与数据库初始化均在应用启动时完成（init_db 幂等）
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
