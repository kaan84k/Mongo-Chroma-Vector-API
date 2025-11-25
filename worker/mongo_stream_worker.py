import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from prometheus_client import Counter, Gauge, start_http_server
from pymongo import MongoClient
from bson.objectid import ObjectId

from backend.config import (
    API_BASE,
    API_TOKEN,
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION,
    POLL_INTERVAL_SEC,
    WORKER_BACKOFF_BASE_SEC,
    WORKER_CHECKPOINT_FILE,
    WORKER_MAX_RETRIES,
    USE_CHANGE_STREAM,
    WORKER_METRICS_PORT,
)

# Metrics
WORKER_PROCESSED = Counter("worker_processed_total", "Processed docs", ["status"])
WORKER_RETRIES = Counter("worker_retries_total", "Retries attempted")
WORKER_LAST_ID = Gauge("worker_last_mongo_id", "Last processed Mongo ObjectId (timestamp component)")


def _headers():
    headers = {}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    return headers


def _post_with_retry(payload: dict) -> bool:
    """
    Sends payload to /ingest with retries and exponential backoff.
    Returns True on success, False on failure after retries.
    """
    url = f"{API_BASE}/ingest"
    for attempt in range(WORKER_MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=10, headers=_headers())
            r.raise_for_status()
            return True
        except Exception as e:
            WORKER_RETRIES.inc()
            sleep_for = WORKER_BACKOFF_BASE_SEC * (2**attempt) * (1 + random.random())
            _log(
                "ingest_retry",
                attempt=attempt + 1,
                max_attempts=WORKER_MAX_RETRIES,
                error=str(e),
                sleep_for=round(sleep_for, 2),
            )
            time.sleep(sleep_for)
    _log("ingest_failed", mongo_id=payload.get("mongo_id"))
    return False


def _load_checkpoint() -> Optional[ObjectId]:
    path = Path(WORKER_CHECKPOINT_FILE)
    if not path.exists():
        return None
    try:
        val = path.read_text().strip()
        return ObjectId(val) if val else None
    except Exception:
        return None


def _save_checkpoint(obj_id: ObjectId) -> None:
    path = Path(WORKER_CHECKPOINT_FILE)
    path.write_text(str(obj_id))


def _process_doc(doc) -> bool:
    payload = {
        "mongo_id": str(doc["_id"]),
        "title": doc.get("title", ""),
        "body": doc.get("body", ""),
        "tags": doc.get("tags", []),
    }
    ok = _post_with_retry(payload)
    if ok:
        _log("synced", mongo_id=payload["mongo_id"])
        WORKER_PROCESSED.labels(status="success").inc()
        try:
            WORKER_LAST_ID.set(doc["_id"].generation_time.timestamp())
        except Exception:
            pass
    else:
        WORKER_PROCESSED.labels(status="error").inc()
    return ok


def run_polling_worker():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    last_seen_id = _load_checkpoint()
    _log(
        "worker_start_polling",
        interval_sec=POLL_INTERVAL_SEC,
        api_base=API_BASE,
        checkpoint=str(last_seen_id),
        metrics_port=WORKER_METRICS_PORT,
    )

    start_http_server(WORKER_METRICS_PORT)

    while True:
        query = {"_id": {"$gt": last_seen_id}} if last_seen_id else {}
        docs = list(coll.find(query).sort("_id", 1))

        for doc in docs:
            if _process_doc(doc):
                last_seen_id = doc["_id"]
                _save_checkpoint(last_seen_id)

        time.sleep(POLL_INTERVAL_SEC)


def run_change_stream_worker():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    _log("worker_start_change_stream", api_base=API_BASE, metrics_port=WORKER_METRICS_PORT)

    start_http_server(WORKER_METRICS_PORT)

    with coll.watch(full_document="updateLookup") as stream:
        for change in stream:
            if change["operationType"] not in {"insert", "replace", "update"}:
                continue
            doc = change.get("fullDocument")
            if not doc:
                continue
            _process_doc(doc)


def _log(message: str, **kwargs):
    record = {"msg": message, "ts": datetime.utcnow().isoformat(), **kwargs}
    print(json.dumps(record))


if __name__ == "__main__":
    if USE_CHANGE_STREAM:
        run_change_stream_worker()
    else:
        run_polling_worker()
