from pathlib import Path
import os

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]

# Load base .env, then overlay environment-specific file when present.
BASE_ENV_PATH = ROOT_DIR / ".env"
load_dotenv(BASE_ENV_PATH)
APP_ENV = os.getenv("APP_ENV", "development")
ENV_SPECIFIC = ROOT_DIR / f".env.{APP_ENV}"
if ENV_SPECIFIC.exists():
load_dotenv(ENV_SPECIFIC, override=True)

# Fail fast if required vars are missing to avoid accidental defaults in production.
REQUIRED_VARS = [
    "MONGO_URI",
    "MONGO_DB",
    "MONGO_COLLECTION",
    "CHROMA_DIR",
    "CHROMA_COLLECTION",
]
missing = [key for key in REQUIRED_VARS if not os.getenv(key)]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

# API security
API_TOKEN = os.getenv("API_TOKEN")
if APP_ENV != "development" and not API_TOKEN:
    error_msg = (
        f"API_TOKEN must be set when APP_ENV is '{APP_ENV}'. "
        "Add it to your environment or .env files for non-development runs."
    )
    print(error_msg)
    raise RuntimeError(error_msg)

ALLOWED_CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",")
    if origin.strip()
]
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "120"))
API_BASE = os.getenv("API_BASE", "http://localhost:8000")

# Worker settings
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "5"))
WORKER_MAX_RETRIES = int(os.getenv("WORKER_MAX_RETRIES", "3"))
WORKER_BACKOFF_BASE_SEC = float(os.getenv("WORKER_BACKOFF_BASE_SEC", "1.0"))
WORKER_CHECKPOINT_FILE = os.getenv(
    "WORKER_CHECKPOINT_FILE", str(ROOT_DIR / "worker_checkpoint.txt")
)
USE_CHANGE_STREAM = os.getenv("USE_CHANGE_STREAM", "false").lower() == "true"
WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "9001"))

# Mongo
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# Chroma
CHROMA_DIR = os.getenv("CHROMA_DIR")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION")
CHROMA_HNSW_SPACE = os.getenv("CHROMA_HNSW_SPACE", "cosine")
CHROMA_HNSW_M = int(os.getenv("CHROMA_HNSW_M", "32"))
CHROMA_HNSW_CONSTRUCTION_EF = int(os.getenv("CHROMA_HNSW_CONSTRUCTION_EF", "200"))
CHROMA_HNSW_SEARCH_EF = int(os.getenv("CHROMA_HNSW_SEARCH_EF", "50"))

# Optional Gemini (test only)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_EMBED_MODEL = os.getenv("GEMINI_EMBED_MODEL", "text-embedding-004")

# Request payload limits
MAX_DOC_CHARS = int(os.getenv("MAX_DOC_CHARS", "12000"))

# Worker batching
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "50"))
