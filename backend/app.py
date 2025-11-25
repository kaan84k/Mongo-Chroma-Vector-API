import time
from collections import deque, defaultdict
from typing import Deque, Dict, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from .api_models import IngestPayload, SearchRequest, DeletePayload
from .vector_store import upsert_document, query_documents, delete_document
from .config import API_TOKEN, ALLOWED_CORS_ORIGINS, RATE_LIMIT_PER_MIN

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


@app.middleware("http")
async def auth_and_rate_limit(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"

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
    return response


def build_text_from_payload(p: IngestPayload) -> str:
    tags_str = ", ".join(p.tags) if p.tags else ""
    return f"Title: {p.title}\nBody: {p.body}\nTags: {tags_str}".strip()


@app.get("/health")
async def health():
    return {"status": "ok"}


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
