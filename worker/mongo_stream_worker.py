import time
from datetime import datetime

import requests
from pymongo import MongoClient
from bson.objectid import ObjectId

from backend.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION, API_TOKEN

API_BASE = "http://localhost:8000"

POLL_INTERVAL_SEC = 5


def run_polling_worker():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    print(f"Starting polling worker (every {POLL_INTERVAL_SEC}s)…")

    # track last seen _id
    last_seen_id = None

    while True:
        query = {}
        if last_seen_id is not None:
            query = {"_id": {"$gt": last_seen_id}}

        docs = list(coll.find(query).sort("_id", 1))

        for doc in docs:
            payload = {
                "mongo_id": str(doc["_id"]),
                "title": doc.get("title", ""),
                "body": doc.get("body", ""),
                "tags": doc.get("tags", []),
            }

            try:
                headers = {}
                if API_TOKEN:
                    headers["Authorization"] = f"Bearer {API_TOKEN}"

                r = requests.post(
                    f"{API_BASE}/ingest", json=payload, timeout=10, headers=headers
                )
                r.raise_for_status()
                print(
                    f"[{datetime.utcnow().isoformat()}] Synced Mongo _id={payload['mongo_id']} to Chroma"
                )
                last_seen_id = doc["_id"]
            except Exception as e:
                print(f"[ERROR] Failed to call /ingest: {e}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_polling_worker()
