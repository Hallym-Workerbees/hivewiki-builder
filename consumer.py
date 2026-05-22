import logging

import redis

from config import settings
from data import db_writer
from data.payload import JobPayload, parse_payload

logger = logging.getLogger(__name__)


def generate_wiki(payload: JobPayload) -> dict:
    raise NotImplementedError("generate_wiki: 클러스터링 A/B(C-1) 결정 후 구현 예정")


def process_job(payload: JobPayload) -> None:
    job = payload.job
    try:
        wiki = generate_wiki(payload)
        with db_writer.transaction() as conn:
            db_writer.insert_wiki(
                conn,
                title=wiki["title"],
                slug=wiki["slug"],
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
    client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    logger.info(f"[컨슈머 시작] queue={settings.WIKIFY_QUEUE}")

    while True:
        _, raw = client.blpop(settings.WIKIFY_QUEUE)
        try:
            payload = parse_payload(raw)
        except Exception:
            logger.exception(f"[스킵] payload 파싱 실패: {raw[:200]}")
            continue
        logger.info(f"[수신] job={payload.job.id} doc={payload.document.title}")
        process_job(payload)


if __name__ == "__main__":
    main()
