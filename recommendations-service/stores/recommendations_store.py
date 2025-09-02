from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

from utils.storage import storage_options

logger = logging.getLogger("uvicorn.error")

class RecommendationsStore:
    """
    Офлайн-рекомендации (финальные + дефолт топ-популяр).
    Ожидаемые схемы:
      recommendations.parquet: ⟨user_id, track_id, …, rank? cb_score?⟩
      top_popular.parquet:     ⟨track_id, count⟩
    """

    def __init__(self) -> None:
        self.personal: Optional[pd.DataFrame] = None  # index=user_id, cols: track_id, rank/cb_score/…
        self.default: Optional[pd.DataFrame] = None   # cols: track_id, count
        self.stats: Dict[str, int] = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, *, personal_path: str, default_path: str) -> None:
        opts = storage_options()
        logger.info(f"Loading offline recs: personal={personal_path}, default={default_path}")
        try:
            df_personal = pd.read_parquet(personal_path, storage_options=opts)
            if {"user_id", "track_id"}.issubset(df_personal.columns) is False:
                raise ValueError("recommendations.parquet должен содержать user_id и track_id")

            # Сортировка: rank ASC приоритетнее, иначе cb_score DESC
            if "rank" in df_personal.columns:
                df_personal = df_personal.sort_values(["user_id", "rank"], ascending=[True, True])
            elif "cb_score" in df_personal.columns:
                df_personal = df_personal.sort_values(["user_id", "cb_score"], ascending=[True, False])

            self.personal = df_personal.set_index("user_id")
        except Exception as e:
            logger.error(f"Failed to load personal recommendations: {e}")
            self.personal = None

        try:
            df_default = pd.read_parquet(default_path, storage_options=opts)
            if {"track_id", "count"}.issubset(df_default.columns) is False:
                raise ValueError("top_popular.parquet должен содержать track_id и count")
            self.default = df_default.sort_values("count", ascending=False)
        except Exception as e:
            logger.error(f"Failed to load default recommendations: {e}")
            self.default = pd.DataFrame({"track_id": []})

        logger.info("Offline recs loaded")

    def get(self, user_id: int, k: int = 100) -> List[int]:
        try:
            assert self.personal is not None
            recs = self.personal.loc[user_id]["track_id"].tolist()[:k]
            self.stats["request_personal_count"] += 1
            return recs
        except Exception:
            df = self.default if self.default is not None else pd.DataFrame({"track_id": []})
            recs = df["track_id"].tolist()[:k]
            self.stats["request_default_count"] += 1
            return recs
