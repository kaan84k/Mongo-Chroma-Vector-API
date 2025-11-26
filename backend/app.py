from fastapi import FastAPI, HTTPException

from .api_models import IngestPayload, SearchRequest, DeletePayload
from .vector_store import upsert_document, query_documents, delete_document

app = FastAPI(
    title="Mongo → Chroma Vector API",
    version="1.0.0",
    description="Core vector service for syncing MongoDB documents into ChromaDB.",
)


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
