import json
import time
import uuid
from collections import deque, defaultdict
from typing import Deque, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pymongo import MongoClient

from .api_models import IngestPayload, SearchRequest, DeletePayload
from .vector_store import upsert_document, query_documents, delete_document, collection
from .config import (
    API_TOKEN,
    ALLOWED_CORS_ORIGINS,
    RATE_LIMIT_PER_MIN,
    MONGO_URI,
    MONGO_DB,
)

app = FastAPI(
    title="Mongo → Chroma Vector API",
    version="1.0.0",
    description="Core vector service for syncing MongoDB documents into ChromaDB.",
)

# CORS allowlist (empty list → no CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Very small in-memory rate limiter per client IP
RATE_WINDOW_SEC = 60
request_log: Dict[str, Deque[float]] = defaultdict(deque)

# Metrics
REQUEST_COUNT = Counter(
    "api_requests_total", "Total API requests", ["path", "method", "status"]
)
REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds", "API request latency seconds", ["path", "method"]
)

# Mongo client for health checks
_mongo_client: Optional[MongoClient] = None


def get_mongo_client() -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return _mongo_client


def log_json(message: str, **kwargs):
    record = {"msg": message, **kwargs}
    print(json.dumps(record))


@app.middleware("http")
async def auth_and_rate_limit(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())

    start = time.time()

    # Rate limiting
    now = time.time()
    timestamps = request_log[client_ip]
    while timestamps and now - timestamps[0] > RATE_WINDOW_SEC:
        timestamps.popleft()
    if len(timestamps) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=429, detail="Too many requests")
    timestamps.append(now)

    # Token auth (Bearer or raw token)
    if API_TOKEN:
        auth_header = request.headers.get("authorization", "")
        token = auth_header.replace("Bearer ", "").strip() if auth_header else ""
        if token != API_TOKEN:
            raise HTTPException(status_code=401, detail="Unauthorized")

    response = await call_next(request)

    latency = time.time() - start
    path_template = request.scope.get("path", "")
    status = response.status_code
    REQUEST_COUNT.labels(path=path_template, method=request.method, status=status).inc()
    REQUEST_LATENCY.labels(path=path_template, method=request.method).observe(latency)

    response.headers["X-Request-ID"] = request_id
    log_json(
        "request",
        request_id=request_id,
        path=path_template,
        method=request.method,
        status=status,
        latency_ms=round(latency * 1000, 2),
        client_ip=client_ip,
    )
    return response


def build_text_from_payload(p: IngestPayload) -> str:
    tags_str = ", ".join(p.tags) if p.tags else ""
    return f"Title: {p.title}\nBody: {p.body}\nTags: {tags_str}".strip()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """
    Checks connectivity to Mongo and Chroma.
    """
    mongo_ok = False
    chroma_ok = False

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        mongo_ok = True
    except Exception as e:
        log_json("readyz_mongo_fail", error=str(e))

    try:
        # count triggers a quick round-trip to Chroma
        collection.count()
        chroma_ok = True
    except Exception as e:
        log_json("readyz_chroma_fail", error=str(e))

    status = 200 if (mongo_ok and chroma_ok) else 503
    return Response(
        content=json.dumps(
            {"status": "ok" if status == 200 else "degraded", "mongo": mongo_ok, "chroma": chroma_ok}
        ),
        media_type="application/json",
        status_code=status,
    )


@app.post("/ingest")
async def ingest_document(payload: IngestPayload):
    text = build_text_from_payload(payload)

    # Chroma metadata must be scalar-valued → convert list → string
    tags_value = ", ".join(payload.tags) if payload.tags else None

    metadata = {
        "source": "mongo",
        "title": payload.title,
    }
    if tags_value is not None:
        metadata["tags"] = tags_value

    upsert_document(
        doc_id=payload.mongo_id,
        document=text,
        metadata=metadata,
        embedding=payload.embedding,  # optional
    )

    return {"status": "ingested", "id": payload.mongo_id}


@app.post("/search")
async def search(req: SearchRequest):
    """
    Core search endpoint.
    - Vector search over the Chroma index
    - Returns raw docs + metadata for your own apps to consume
    """
    res = query_documents(req.query, top_k=req.top_k)

    if not res["ids"] or len(res["ids"][0]) == 0:
        return {"query": req.query, "results": []}

    docs = res["documents"][0]
    ids = res["ids"][0]
    metas = res["metadatas"][0]

    results = []
    for _id, doc, meta in zip(ids, docs, metas):
        results.append(
            {
                "id": _id,
                "document": doc,
                "metadata": meta,
            }
        )

    return {"query": req.query, "results": results}


@app.post("/delete")
async def delete(req: DeletePayload):
    delete_document(req.mongo_id)
    return {"status": "deleted", "id": req.mongo_id}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
