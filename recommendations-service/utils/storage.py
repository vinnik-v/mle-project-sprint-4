import os
from typing import Dict, List

from dotenv import load_dotenv
load_dotenv()

def s3_path(*parts: str) -> str:
    """
    Формирует s3://bucket/prefix/key из окружения.
    Если бакет не задан — возвращает локальный путь (fallback).
    """
    bucket = os.getenv("AWS_BUCKET_NAME")
    prefix = "recsys/"
    prefix = (prefix + "/") if prefix and not prefix.endswith("/") else prefix
    key = "/".join(p.strip("/") for p in parts)
    if not bucket:
        return key
    return f"s3://{bucket}/{prefix}{key}"

def storage_options() -> Dict[str, object]:
    """
    Опции для pandas.read_parquet(..., storage_options=...) с s3fs/pyarrow.
    """
    opts: Dict[str, object] = {}
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        opts["client_kwargs"] = {"endpoint_url": endpoint}
    ak = os.getenv("AWS_ACCESS_KEY_ID")
    sk = os.getenv("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        opts["key"] = ak
        opts["secret"] = sk
    return opts

def dedup_ids(ids: List[int]) -> List[int]:
    """Удаляет дубликаты"""
    seen = set()
    out: List[int] = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out
