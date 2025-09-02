"""
Тестирование микросервиса рекомендаций.

Сценарии:
1) Пользователь без персональных рекомендаций (фолбэк на топ-популяр).
2) Пользователь с персональными рекомендациями, но без онлайн-истории.
3) Пользователь с персональными рекомендациями и онлайн-историей.

Требуется задать:
  BASE_API_URL           (по умолчанию http://127.0.0.1:8000)
  PERSONAL_USER_ID   (обязателен, int; должен существовать в recommendations.parquet)

Вывод пишется в test_service.log и дублируется в stdout.
"""

import os
import sys
import logging
from typing import Any, Dict

import requests

from dotenv import load_dotenv
load_dotenv()

BASE_API_URL = os.getenv("BASE_API_URL", "http://127.0.0.1:8000")
LOG_FILE = "test_service.log"
TIMEOUT = 10

# ---------- логирование ----------
logger = logging.getLogger("tests")
logger.setLevel(logging.INFO)
logger.handlers.clear()
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

fh = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.addHandler(sh)

# ---------- http хелперы ----------
def post(path: str, params: Dict[str, Any]) -> requests.Response:
    url = BASE_API_URL.rstrip("/") + path
    return requests.post(url, params=params, headers={"Content-Type": "application/json"}, timeout=TIMEOUT)

def get(path: str) -> requests.Response:
    url = BASE_API_URL.rstrip("/") + path
    return requests.get(url, timeout=TIMEOUT)

def must_ok(resp: requests.Response) -> Dict[str, Any]:
    try:
        resp.raise_for_status()
    except Exception as e:
        logger.error("HTTP %s %s failed: %s\nBody: %s", resp.request.method, resp.url, e, getattr(resp, "text", ""))
        raise
    try:
        return resp.json()
    except Exception:
        logger.error("Not a JSON response from %s: %s", resp.url, resp.text)
        raise

# ---------- тест-кейсы ----------
def test_no_personal():
    """
    Пользователь без персоналки: офлайн идёт в фолбэк (top_popular) и /stats увеличивает request_default_count.
    """
    before = must_ok(get("/stats"))
    default_before = int(before.get("offline", {}).get("request_default_count", 0))

    user_id = 10_000_000  # гарантированно вне персоналки
    k = 10
    data = must_ok(post("/recommendations_offline", {"user_id": user_id, "k": k}))
    recs = data.get("recs", [])

    after = must_ok(get("/stats"))
    default_after = int(after.get("offline", {}).get("request_default_count", 0))

    logger.info("[CASE 1] user_id=%s offline_fallback=%s", user_id, recs[:10])
    assert isinstance(recs, list), "offline должен вернуть список"
    assert len(recs) > 0, "для отсутствующего пользователя должен вернуться фолбэк top_popular"
    assert default_after == default_before + 1, "request_default_count должен увеличиться на 1"

def test_personal_no_online(user_id: int):
    """
    Пользователь с персоналкой, но без онлайн-истории:
      - offline не пуст
      - online пуст
      - blended совпадает с offline по префиксу (потому что онлайн пуст)
    ВАЖНО: EventStore должен быть пуст при старте сервиса.
    """
    k = 10
    offline = must_ok(post("/recommendations_offline", {"user_id": user_id, "k": k})).get("recs", [])
    online  = must_ok(post("/recommendations_online",  {"user_id": user_id, "k": k})).get("recs", [])
    blended = must_ok(post("/recommendations",         {"user_id": user_id, "k": k})).get("recs", [])

    logger.info("[CASE 2] user_id=%s offline=%s", user_id, offline[:10])
    logger.info("[CASE 2] user_id=%s online=%s",  user_id, online[:10])
    logger.info("[CASE 2] user_id=%s blended=%s", user_id, blended[:10])

    assert len(offline) > 0, "ожидаем персональные офлайн-рекомендации для PERSONAL_USER_ID"
    assert len(online) == 0, "без событий онлайн-рекомендации должны быть пустыми (перезапусти сервис, если не так)"
    assert blended[:min(k, len(offline))] == offline[:min(k, len(offline))], "без онлайна blended должен совпадать с offline"

def test_personal_with_online(user_id: int):
    """
    Пользователь с персоналкой и онлайн-историей:
      - добавляем 2 события из офлайна
      - online становится непустым
      - blended стартует с online, офлайн попадает на чётные позиции (с учётом дедупа)
    """
    k = 10
    offline = must_ok(post("/recommendations_offline", {"user_id": user_id, "k": k})).get("recs", [])
    assert offline, "нет офлайн-рекомендаций для PERSONAL_USER_ID"

    # добавим онлайн-события
    for tid in offline[:2]:
        must_ok(post("/events/put", {"user_id": user_id, "track_id": tid}))

    online  = must_ok(post("/recommendations_online", {"user_id": user_id, "k": k})).get("recs", [])
    blended = must_ok(post("/recommendations",        {"user_id": user_id, "k": k})).get("recs", [])

    logger.info("[CASE 3] user_id=%s added_events=%s", user_id, offline[:2])
    logger.info("[CASE 3] user_id=%s online=%s",       user_id, online[:10])
    logger.info("[CASE 3] user_id=%s offline=%s",      user_id, offline[:10])
    logger.info("[CASE 3] user_id=%s blended=%s",      user_id, blended[:10])

    assert len(online) > 0, "после добавления событий онлайн-рекомендации должны быть непустыми"
    if blended and online:
        assert blended[0] == online[0], "первая позиция blended должна быть из online"
    # офлайн[0] может быть удалён дедупом, поэтому допускаем попадание в первые 3 позиции
    if len(blended) > 1 and offline:
        assert (blended[1] in offline[:2]) or (offline[0] in blended[:3]), "вторая позиция blended ожидаемо из offline (с оговоркой на дедуп)"

# ---------- запуск ----------
def main():
    # читаем PERSONAL_USER_ID
    raw_uid = os.getenv("PERSONAL_USER_ID")
    if not raw_uid:
        logger.error("PERSONAL_USER_ID не задан. Экспортируй переменную окружения, например: PERSONAL_USER_ID=53 python test_service.py")
        sys.exit(2)
    try:
        user_id = int(raw_uid)
    except ValueError:
        logger.error("PERSONAL_USER_ID должен быть целым числом, а не '%s'", raw_uid)
        sys.exit(2)

    # здоровье
    health = must_ok(get("/health"))
    logger.info("HEALTH: %s", health)

    # кейс 1: без персоналки
    test_no_personal()

    # кейс 2: персоналка без онлайна
    test_personal_no_online(user_id)

    # кейс 3: персоналка с онлайном
    test_personal_with_online(user_id)

    logger.info("Все тесты завершились успешно")

if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        logger.exception("ASSERTION FAILED: %s", e)
        sys.exit(1)
    except requests.RequestException as e:
        logger.exception("HTTP ERROR: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("UNEXPECTED ERROR: %s", e)
        sys.exit(1)
