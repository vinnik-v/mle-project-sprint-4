import os
from typing import Dict, List

def s3_path(*parts: str) -> str:
    """
    Формирует s3://bucket/prefix/key из окружения.
    Предпочитает AWS_BUCKET_NAME, но понимает и S3_BUCKET.
    """
    bucket = os.getenv("AWS_BUCKET_NAME") or os.getenv("S3_BUCKET") or ""
    prefix = os.getenv("S3_PREFIX", "").lstrip("/")
    prefix = (prefix + "/") if prefix and not prefix.endswith("/") else prefix
    key = "/".join(p.strip("/") for p in parts)
    if not bucket:
        # локальный путь как fallback (на случай локальных файлов)
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
    """Удаляет дубликаты, сохраняя первый порядок встречаемости."""
    seen = set()
    out: List[int] = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out
