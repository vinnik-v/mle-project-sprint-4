# S3 
Имя бакета: `s3-student-mle-20250529-e59a5780ac-freetrack`
# Подготовка виртуальной машины

## Склонируйте репозиторий

Склонируйте репозиторий проекта:

```
git clone https://github.com/yandex-praktikum/mle-project-sprint-4-v001.git
```

## Активируйте виртуальное окружение

Используйте то же самое виртуальное окружение, что и созданное для работы с уроками. Если его не существует, то его следует создать.

Создать новое виртуальное окружение можно командой:

```
python3 -m venv env_recsys_start
```

После его инициализации следующей командой

```
. env_recsys_start/bin/activate
```

установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```

### Скачайте файлы с данными

Для начала работы понадобится три файла с данными:
- [tracks.parquet](https://storage.yandexcloud.net/mle-data/ym/tracks.parquet)
- [catalog_names.parquet](https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet)
- [interactions.parquet](https://storage.yandexcloud.net/mle-data/ym/interactions.parquet)
 
Скачайте их в директорию локального репозитория. Для удобства вы можете воспользоваться командой wget:

```
wget https://storage.yandexcloud.net/mle-data/ym/tracks.parquet

wget https://storage.yandexcloud.net/mle-data/ym/catalog_names.parquet

wget https://storage.yandexcloud.net/mle-data/ym/interactions.parquet
```

## Запустите Jupyter Lab

Запустите Jupyter Lab в командной строке

```
jupyter lab --ip=0.0.0.0 --no-browser
```

# Расчёт рекомендаций

Код для выполнения первой части проекта находится в файле `recommendations.ipynb`. Изначально, это шаблон. Используйте его для выполнения первой части проекта.

# Сервис рекомендаций

Код сервиса рекомендаций находится в файле `recommendations-service/recommendations_service.py`.

## Запуск сервиса
```bash
cd recommendations-service/
uvicorn recommendations_service:app --host 0.0.0.0 --port 8000
```

## Swagger
[Swagger UI](http://127.0.0.1:8000/docs)

## Структура проекта
recommendations-service/
├─ recommendations_service.py
├─ stores/
│  ├─ __init__.py
│  ├─ event_store.py
│  └─ recommendations_store.py
└─ utils/
   ├─ __init__.py
   └─ storage.py

## Стратегия смешивания (blending) онлайн- и офлайн-рекомендаций

### Цель
Объединить офлайн-модель по истории и фичам и онлайн, учитывающий последнтие действия пользователя, чтобы выдача одновременно была персональной и чувствительной к последним действиям.

### Источники
- **Офлайн**: `recommendations.parquet`  
  Колонки: `user_id`, `track_id`, `rank?`, `cb_score?`.  
  Порядок: если есть `rank` — сортировка по возрастанию `rank`; иначе, если есть `cb_score` — по убыванию `cb_score`.  
  Результат: отсортированный список `track_id` для пользователя.
- **Онлайн**: формируется на лету без матрицы похожестей.
  1. Берём последние `N=3` событий пользователя из `EventStore` (эндпоинт `/events/get`).
  2. Формируем множество запретов `ban`:
     - `ban - {последние N track_id из EventStore}`
     - чтобы повысить разнообразие, исключаем верхнюю часть офлайн-списка:  
       `ban - ban ∪ head(offline_recs, min(k, len(offline_recs)))`
  3. Кандидаты онлайн: глобально популярные треки из `top_popular.parquet` (`track_id`, `count`), отсортированные по `count DESC`, **с фильтрацией по `ban`**.
  4. Онлайн-результат: первые `k` элементов после фильтра.

> Почему так: это стабильный способ реагировать на последние действия без отдельного i2i-хранилища. В проде его можно заменить на item2item или session-based модель, но текущая логика даёт предсказуемый выигрыш в актуальности без усложнения архитектуры.

### Алгоритм блендинга
Пусть:
- `O = offline_recs(user, k)` — офлайн-список `track_id`.
- `L = online_recs(user, k)` — онлайн-список `track_id`.
- `k` — длина целевого списка.

Шаги:
1. **Чередование**: онлайн на нечётные позиции, офлайн на чётные.  
```python
   blended = []
   n = min(len(O), len(L))
   for i in range(n):
       blended.append(L[i])  # позиции 1,3,5...
       blended.append(O[i])  # позиции 2,4,6...
   if len(L) > n: blended += L[n:]
   if len(O) > n: blended += O[n:]
```

2. **Дедупликация** с сохранением первого вхождения:

   ```python
   def dedup_ids(ids):
       seen = set(); out = []
       for x in ids:
           if x not in seen:
               out.append(x); seen.add(x)
       return out
   blended = dedup_ids(blended)
   ```
3. **Обрезка** до `k`:

   ```python
   blended = blended[:k]
   ```

# Инструкции для тестирования сервиса

Код для тестирования сервиса находится в файле `test_service.py`.

## Тестирование микросервиса рекомендаций.

Сценарии:
1) Пользователь без персональных рекомендаций (фолбэк на топ-популяр).
2) Пользователь с персональными рекомендациями, но без онлайн-истории.
3) Пользователь с персональными рекомендациями и онлайн-историей.

Требуется задать в переменных окружения:
  `BASE_API_URL`           (по умолчанию http://127.0.0.1:8000)
  `PERSONAL_USER_ID`   (обязателен, int; должен существовать в recommendations.parquet)

Вывод пишется в `test_service.log` и дублируется в stdout.

## Запуск тестов
```bash
python test_service.py | tee test_service.log
```
