import logging

import redis
from openai import OpenAI

from config import settings
from data import db_writer
from data.payload import JobPayload, parse_payload
from storm_engine import wiki_generator
from storm_engine.llm_config import setup_llms

logger = logging.getLogger(__name__)


def process_job(payload: JobPayload, lm_configs, openai_client: OpenAI) -> None:
    job = payload.job
    try:
        with db_writer.transaction() as conn:
            db_writer.mark_job_started(conn, job.id)

        wiki = wiki_generator.generate_wiki(payload, lm_configs, openai_client)

        with db_writer.transaction() as conn:
            db_writer.insert_chunk_with_embedding(
                conn,
                source_document_id=job.source_document_id,
                content_text=wiki["chunk_content"],
                embedding=wiki["embedding"],
            )
            slug = db_writer.make_unique_slug(conn, wiki["slug_base"])
            db_writer.insert_wiki(
                conn,
                title=wiki["title"],
                slug=slug,
                summary=wiki["summary"],
                content_markdown=wiki["content_markdown"],
                generation_model=settings.SYNTHESIS_MODEL,
            )
            db_writer.mark_job_completed(conn, job.id, job.source_document_id)
    except Exception as e:
        logger.exception(f"[job {job.id}] 처리 실패")
        with db_writer.transaction() as conn:
            db_writer.mark_job_failed(conn, job.id, job.source_document_id, str(e))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
    lm_configs = setup_llms()

    client = redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    )
    logger.info(f"[컨슈머 시작] queue={settings.REDIS_QUEUE_NAME}")

    while True:
        _, raw = client.blpop(settings.REDIS_QUEUE_NAME)
        try:
            payload = parse_payload(raw)
        except Exception:
            logger.exception(f"[스킵] payload 파싱 실패: {raw[:200]}")
            continue
        logger.info(f"[수신] job={payload.job.id} doc={payload.document.title}")
        process_job(payload, lm_configs, openai_client)


if __name__ == "__main__":
    main()
