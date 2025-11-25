# Mongo → Chroma Vector Sync API

A lightweight vectorization backend that syncs documents from **MongoDB** into **ChromaDB** in near real time. Designed as a foundational service for building Retrieval-Augmented systems, data indexing engines, or semantic search pipelines.

Gemini-based RAG is included **only as an optional test endpoint**.

---

## 🔧 Features

* **MongoDB → ChromaDB automated sync** (via polling worker)
* **FastAPI Vector API**

  * `POST /ingest` – index a document
  * `POST /search` – semantic vector search
  * `POST /delete` – remove a document by ID
* **Optional Test Endpoint**

  * `POST /ask-test` – Gemini-backed RAG for debugging retrieval quality
* Clean modular structure
* Fully local (Chroma embedded mode)

---

## 📂 Project Structure

```
mongo-chroma-vector-api/
│
├── backend/
│   ├── app.py                # FastAPI application
│   ├── config.py             # Env configuration
│   ├── vector_store.py       # Chroma wrapper
│   ├── api_models.py         # Request/Response schemas
│   ├── gemini_test.py        # Optional RAG test
│
├── worker/
│   ├── mongo_stream_worker.py  # Polling worker for Mongo
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🛠️ Installation

### 1️⃣ Clone the Project

```bash
git clone https://github.com/kaan84k/Mongo-Chroma-Vector-API.git
cd mongo-chroma-vector-api
```

### 2️⃣ Create & Activate Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Configure Environment

Copy `.env.example` → `.env`:

```bash
cp .env.example .env
```

Fill values such as (defaults in example are for local dev):

```
APP_ENV=development
MONGO_URI=mongodb://localhost:27017
MONGO_DB=realtime_demo
MONGO_COLLECTION=articles

CHROMA_DIR=./chroma_store
CHROMA_COLLECTION=realtime_demo
CHROMA_HNSW_SPACE=cosine
CHROMA_HNSW_M=32
CHROMA_HNSW_CONSTRUCTION_EF=200
CHROMA_HNSW_SEARCH_EF=50

# Payload limits
MAX_DOC_CHARS=12000

# Worker batching/metrics
WORKER_BATCH_SIZE=50

# Optional Gemini for test endpoint
GEMINI_API_KEY=your_key_here
GEMINI_MODEL=gemini-2.0-flash
GEMINI_EMBED_MODEL=text-embedding-004
```

For multiple environments, set `APP_ENV` (e.g., `production`, `staging`) and optionally create `.env.production` / `.env.staging` to override the base `.env`. Startup will fail fast if required environment variables are missing to avoid falling back to unsafe defaults.

### Worker configuration

- `API_BASE` (default `http://localhost:8000`)
- `POLL_INTERVAL_SEC` (default `5`)
- `WORKER_MAX_RETRIES` / `WORKER_BACKOFF_BASE_SEC` (exponential backoff for /ingest calls)
- `WORKER_CHECKPOINT_FILE` (persists last processed Mongo `_id` so restarts resume)
- `USE_CHANGE_STREAM` (set `true` to use Mongo change streams; requires a replica set)
- `WORKER_METRICS_PORT` (default `9001`) for Prometheus `/metrics` served by the worker
- `WORKER_BATCH_SIZE` (default `50`) for batch ingest when catching up

When `USE_CHANGE_STREAM=true`, the worker listens for `insert/replace/update` events via Mongo change streams instead of polling. For deletes, call the API’s `/delete` endpoint separately.

### 🐳 Docker / Docker Compose

Build and run API + worker + Mongo:

```bash
docker-compose up --build
```

Compose overrides `MONGO_URI`/`API_BASE` to use service names (`mongo`, `api`) and shares `chroma_store` via a named volume. Stop with `docker-compose down` (add `-v` to drop volumes).

## 🔒 Security (auth, CORS, rate limits, HTTPS)

- **Auth token**: set `API_TOKEN` (Bearer) and include `Authorization: Bearer <token>` on all API calls.
- **CORS**: set `CORS_ALLOW_ORIGINS` as a comma-separated list (e.g., `https://yourapp.com,https://admin.yourapp.com`).
- **Rate limit**: configure `RATE_LIMIT_PER_MIN` (default 120/min per client IP).
- **HTTPS**: run FastAPI behind a reverse proxy (e.g., nginx/Traefik) that terminates TLS and forwards to Uvicorn. Example nginx snippet:

```
server {
  listen 443 ssl;
  server_name api.yourdomain.com;

  ssl_certificate /etc/ssl/fullchain.pem;
  ssl_certificate_key /etc/ssl/privkey.pem;

  location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
  }
}
```

---

## 🚀 Running the System

### 1️⃣ Start the FastAPI Server

```bash
uvicorn backend.app:app --reload --port 8000
```

Visit docs at:

```
http://localhost:8000/docs
```

### 2️⃣ Start Mongo Sync Worker (Polling Mode)

From project root:

```bash
python -m worker.mongo_stream_worker
```

You should see:

```
Starting polling worker (every 5s)…
```

---

## 🧪 Testing the Pipeline

### ✔ Insert Data into MongoDB

Create a test file or run in Python shell:

```python
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017")
db = client["realtime_demo"]
coll = db["articles"]

coll.insert_one({
    "title": "Diabetic Retinopathy Detection",
    "body": "We use a custom CNN trained on ODIR-5K for DR classification.",
    "tags": ["DR", "CNN", "medical"],
    "created_at": datetime.utcnow()
})
```

Worker output should show:

```
[2025-...] Synced Mongo _id=<id> to Chroma
```

---

## 🔍 Testing Vector Search

Send a query:

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query":"cnn diabetic retinopathy","top_k":3}'
```

You should get your newly indexed document back.

---

## ❌ Delete a Document

```bash
curl -X POST http://localhost:8000/delete \
  -H "Content-Type: application/json" \
  -d '{"mongo_id":"<id_from_mongo>"}'
```

---

## 🤖 (Optional) Gemini RAG Test Endpoint

Enable in `.env` by adding your Gemini API key.

Test:

```bash
curl -X POST http://localhost:8000/ask-test \
  -H "Content-Type: application/json" \
  -d '{"query":"how do we detect DR?","top_k":3}'
```

This checks retrieval + Gemini reasoning.

---

## ✅ CI

GitHub Actions runs on push/PR to `main` and performs dependency install plus a compile check (`python -m compileall backend worker`).

---

## 🧼 Cleaning Chroma Storage

To reset database:

```bash
rm -rf chroma_store
```

---

## 📌 Notes

* This design keeps **vector API production-focused**, and Gemini used **only for debugging**.
* Worker uses **polling**, not change streams, so **replica set is NOT required**.
* You can later upgrade the worker to real change streams once replica set support is available.

---
