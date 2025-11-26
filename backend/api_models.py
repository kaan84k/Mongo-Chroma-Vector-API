from typing import List, Optional
from pydantic import BaseModel, Field


class IngestPayload(BaseModel):
    mongo_id: str = Field(..., description="MongoDB _id as string")
    title: str
    body: str
    tags: Optional[List[str]] = None
    # Optional: raw embedding if you compute it elsewhere
    embedding: Optional[List[float]] = None


class SearchRequest(BaseModel):
    query: str
    top_k: int = Field(5, ge=1, le=50)


class DeletePayload(BaseModel):
    mongo_id: str
