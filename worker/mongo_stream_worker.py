import random
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
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
)


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
            sleep_for = WORKER_BACKOFF_BASE_SEC * (2**attempt) * (1 + random.random())
            print(
                f"[WARN] /ingest attempt {attempt + 1}/{WORKER_MAX_RETRIES} failed: {e}; retrying in {sleep_for:.2f}s"
            )
            time.sleep(sleep_for)
    print("[ERROR] Exhausted retries for payload", payload.get("mongo_id"))
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
        print(f"[{datetime.utcnow().isoformat()}] Synced Mongo _id={payload['mongo_id']} to Chroma")
    return ok


def run_polling_worker():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    last_seen_id = _load_checkpoint()
    print(
        f"Starting polling worker (every {POLL_INTERVAL_SEC}s)… "
        f"(API_BASE={API_BASE}, checkpoint={last_seen_id})"
    )

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
    print(f"Starting change stream worker… (API_BASE={API_BASE})")

    with coll.watch(full_document="updateLookup") as stream:
        for change in stream:
            if change["operationType"] not in {"insert", "replace", "update"}:
                continue
            doc = change.get("fullDocument")
            if not doc:
                continue
            _process_doc(doc)


if __name__ == "__main__":
    if USE_CHANGE_STREAM:
        run_change_stream_worker()
    else:
        run_polling_worker()
