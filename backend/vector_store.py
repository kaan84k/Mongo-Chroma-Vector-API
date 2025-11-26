from typing import Dict, Any, List

import chromadb
from chromadb.config import Settings

from .config import CHROMA_DIR, CHROMA_COLLECTION


# Embedded Chroma instance
client = chromadb.PersistentClient(
    path=CHROMA_DIR,
    settings=Settings(anonymized_telemetry=False),
)

collection = client.get_or_create_collection(
    name=CHROMA_COLLECTION,
    metadata={"hnsw:space": "cosine"},
)


def upsert_document(
    doc_id: str,
    document: str,
    metadata: Dict[str, Any],
    embedding: List[float] | None = None,
) -> None:
    """
    Core upsert into Chroma.
    If `embedding` is provided, we pass it.
    If not, Chroma's collection will use its own embedding function.
    """
    kwargs: Dict[str, Any] = {
        "ids": [doc_id],
        "documents": [document],
        "metadatas": [metadata],
    }
    if embedding is not None:
        kwargs["embeddings"] = [embedding]

    collection.upsert(**kwargs)


def delete_document(doc_id: str) -> None:
    collection.delete(ids=[doc_id])


def query_documents(query: str, top_k: int = 5) -> Dict[str, Any]:
    return collection.query(
        query_texts=[query],
        n_results=top_k,
    )
