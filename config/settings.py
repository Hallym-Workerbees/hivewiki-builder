import os

from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY가 .env 파일에 없음")
if not ANTHROPIC_API_KEY:
    raise ValueError("ANTHROPIC_API_KEY가 .env 파일에 없음")

# Database Config
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DATABASE_DSN = os.getenv(
    "DATABASE_DSN",
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

# Redis Config
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "wikify_queue")
REDIS_SOCKET_TIMEOUT_SECONDS = int(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "5"))
REDIS_HEALTH_CHECK_INTERVAL_SECONDS = int(
    os.getenv("REDIS_HEALTH_CHECK_INTERVAL_SECONDS", "30")
)

# LLM Model Config
EMBEDDING_MODEL = "text-embedding-3-small"
AGENT_MODEL = "gpt-4o-mini"
SYNTHESIS_MODEL = "claude-sonnet-4-6"
