"""
FastAPI-сервис рекомендаций: офлайн + онлайн (blending)

Схема файлов:
  recommendations.parquet: ⟨user_id, track_id, ..., cb_score?, rank?⟩
  top_popular.parquet:     ⟨track_id, count⟩
  personal_als.parquet:    ⟨user_id, track_id, score, rank⟩  (не обязателен)
  similar.parquet:         Не используется в этой версии

Окружение (.env):
  S3_ENDPOINT_URL             – https://storage.yandexcloud.net
  AWS_BUCKET_NAME             – имя S3-бакета
  AWS_ACCESS_KEY_ID           – ключ
  AWS_SECRET_ACCESS_KEY       – секрет

Пример путей:
  s3://$AWS_BUCKET_NAME/$S3_PREFIX/recommendations/recommendations.parquet
  s3://$AWS_BUCKET_NAME/$S3_PREFIX/recommendations/top_popular.parquet

Запуск:
  uvicorn recommendations_service:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
from typing import List
from contextlib import asynccontextmanager

from fastapi import FastAPI
from dotenv import load_dotenv

from stores import EventStore, RecommendationsStore
from utils.storage import s3_path, dedup_ids

load_dotenv()
logger = logging.getLogger("uvicorn.error")

# -------- stores --------
rec_store = RecommendationsStore()
events_store = EventStore(max_events_per_user=50)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Загружаем офлайн-рекомендации на старте
    rec_store.load(
        personal_path=s3_path("recommendations", "recommendations.parquet"),
        default_path=s3_path("recommendations", "top_popular.parquet"),
    )
    logger.info("Service is ready")
    yield
    logger.info("Stats: %s", rec_store.stats)

app = FastAPI(
    title="recommendations",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# -------- health & stats --------
@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/stats")
async def stats():
    return {
        "offline": rec_store.stats,
        "events_users": len(events_store.events),
    }

# -------- events API (онлайн-история) --------
@app.post("/events/put")
async def put_event(user_id: int, track_id: int):
    events_store.put(user_id, track_id)
    return {"result": "ok"}

@app.post("/events/get")
async def get_events(user_id: int, k: int = 10):
    return {"events": events_store.get(user_id, k)}

# -------- offline --------
@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    recs = rec_store.get(user_id, k)
    return {"recs": recs}

# -------- online: по последним 3 событиям (без i2i) --------
@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Простая онлайн-логика:
      - берём последние 3 события пользователя
      - кандидаты: топ-популярные, но исключаем последние события
      - чтобы добавить разнообразия, исключаем голову офлайна
      - отдаём первые k
    """
    recent = events_store.get(user_id, k=3)
    if not recent or rec_store.default is None:
        return {"recs": []}

    ban = set(recent)
    offline_seed = (await recommendations_offline(user_id, k=min(k * 2, 500)))["recs"]
    ban.update(offline_seed[: min(len(offline_seed), k)])

    candidates = [tid for tid in rec_store.default["track_id"].tolist() if tid not in ban]
    return {"recs": candidates[:k]}

# -------- blended --------
@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    offline = (await recommendations_offline(user_id, k))["recs"]
    online  = (await recommendations_online(user_id,  k))["recs"]

    blended: List[int] = []
    n = min(len(offline), len(online))
    for i in range(n):
        blended.append(online[i])   # 1,3,5…
        blended.append(offline[i])  # 2,4,6…
    if len(online) > n:
        blended.extend(online[n:])
    if len(offline) > n:
        blended.extend(offline[n:])

    blended = dedup_ids(blended)[:k]
    return {"recs": blended}
