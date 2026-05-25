import logging
import os
import time

import redis
from openai import OpenAI

from config import settings
from data import db_writer
from data.payload import JobPayload, parse_payload
from storm_engine import wiki_generator
from storm_engine.llm_config import setup_llms

logger = logging.getLogger(__name__)

REDIS_POP_TIMEOUT_SECONDS = 10
IDLE_LOG_INTERVAL_SECONDS = 60


def _tmp_writable() -> bool:
    path = "/tmp/hivewiki-builder-write-test"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(path)
        return True
    except OSError:
        return False


def log_runtime_environment() -> None:
    logger.info(
        "[START] runtime uid=%s gid=%s home=%s user=%s logname=%s "
        "tmp_writable=%s dsp_cachedir=%s torchinductor_cache_dir=%s "
        "xdg_cache_home=%s",
        os.getuid(),
        os.getgid(),
        os.getenv("HOME", ""),
        os.getenv("USER", ""),
        os.getenv("LOGNAME", ""),
        _tmp_writable(),
        os.getenv("DSP_CACHEDIR", ""),
        os.getenv("TORCHINDUCTOR_CACHE_DIR", ""),
        os.getenv("XDG_CACHE_HOME", ""),
    )


def process_job(payload: JobPayload, lm_configs, openai_client: OpenAI) -> None:
    job = payload.job
    started = time.perf_counter()
    logger.info(
        "[JOB_START] job=%s source_document=%s title=%r",
        job.id,
        job.source_document_id,
        payload.document.title,
    )
    try:
        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=mark_started", job.id)
        with db_writer.transaction() as conn:
            db_writer.mark_job_started(conn, job.id)
        logger.info(
            "[STAGE_DONE] job=%s stage=mark_started elapsed=%.2fs",
            job.id,
            time.perf_counter() - stage_started,
        )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=generate_wiki", job.id)
        wiki = wiki_generator.generate_wiki(payload, lm_configs, openai_client)
        logger.info(
            "[STAGE_DONE] job=%s stage=generate_wiki elapsed=%.2fs",
            job.id,
            time.perf_counter() - stage_started,
        )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=db_write", job.id)
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
        logger.info(
            "[STAGE_DONE] job=%s stage=db_write elapsed=%.2fs",
            job.id,
            time.perf_counter() - stage_started,
        )
        logger.info(
            "[JOB_DONE] job=%s source_document=%s elapsed=%.2fs",
            job.id,
            job.source_document_id,
            time.perf_counter() - started,
        )
    except Exception as e:
        logger.exception(
            "[ERROR] job=%s source_document=%s stage=process_job elapsed=%.2fs",
            job.id,
            job.source_document_id,
            time.perf_counter() - started,
        )
        with db_writer.transaction() as conn:
            db_writer.mark_job_failed(conn, job.id, job.source_document_id, str(e))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log_runtime_environment()
    logger.info("[START] initializing OpenAI client")
    openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
    logger.info("[START] initializing STORM LLM configs")
    lm_configs = setup_llms()

    logger.info("[START] connecting Redis url_configured=%s", bool(settings.REDIS_URL))
    client = redis.Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    )
    client.ping()
    logger.info(
        "[START] consumer_ready queue=%s pop_timeout=%ss",
        settings.REDIS_QUEUE_NAME,
        REDIS_POP_TIMEOUT_SECONDS,
    )

    last_idle_log = 0.0
    while True:
        item = client.blpop(
            settings.REDIS_QUEUE_NAME,
            timeout=REDIS_POP_TIMEOUT_SECONDS,
        )
        if item is None:
            now = time.monotonic()
            if now - last_idle_log >= IDLE_LOG_INTERVAL_SECONDS:
                logger.info(
                    "[WAIT] queue_empty queue=%s timeout=%ss",
                    settings.REDIS_QUEUE_NAME,
                    REDIS_POP_TIMEOUT_SECONDS,
                )
                last_idle_log = now
            continue
        _, raw = item
        try:
            payload = parse_payload(raw)
        except Exception:
            logger.exception("[ERROR] payload_parse_failed raw_prefix=%r", raw[:200])
            continue
        logger.info(
            "[RECEIVED] job=%s source_document=%s title=%r",
            payload.job.id,
            payload.job.source_document_id,
            payload.document.title,
        )
        process_job(payload, lm_configs, openai_client)


if __name__ == "__main__":
    main()
