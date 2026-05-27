import logging
import os
import time

import redis
from openai import OpenAI

from config import pipeline, settings
from data import db_reader, db_writer
from data.payload import JobPayload, parse_payload
from storm_engine import validator, wiki_generator
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
        "xdg_cache_home=%s storm_work_dir=%s",
        os.getuid(),
        os.getgid(),
        os.getenv("HOME", ""),
        os.getenv("USER", ""),
        os.getenv("LOGNAME", ""),
        _tmp_writable(),
        os.getenv("DSP_CACHEDIR", ""),
        os.getenv("TORCHINDUCTOR_CACHE_DIR", ""),
        os.getenv("XDG_CACHE_HOME", ""),
        pipeline.STORM_WORK_DIR,
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
        logger.info("[STAGE_START] job=%s stage=embedding", job.id)
        embedding = wiki_generator.compute_embedding(
            openai_client, payload.document.body_text
        )
        logger.info(
            "[STAGE_DONE] job=%s stage=embedding elapsed=%.2fs dimensions=%s",
            job.id,
            time.perf_counter() - stage_started,
            len(embedding),
        )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=knn_lookup", job.id)
        with db_writer.transaction() as conn:
            neighbors = db_reader.find_similar_chunks(
                conn,
                embedding=embedding,
                k=pipeline.KNN_CLUSTER_K,
                similarity_threshold=pipeline.CLUSTER_THRESHOLD,
                exclude_source_document_id=job.source_document_id,
            )
            neighbor_chunk_ids = [n.chunk_id for n in neighbors]
            target_wiki_id = db_reader.find_target_wiki_for_chunks(
                conn, neighbor_chunk_ids
            )
            exclude_wiki_ids = [target_wiki_id] if target_wiki_id else []
            related_wikis = db_reader.find_related_wikis(
                conn,
                embedding=embedding,
                k=pipeline.RELATED_WIKI_K,
                min_similarity=pipeline.RELATED_WIKI_MIN_SIMILARITY,
                max_similarity=pipeline.RELATED_WIKI_MAX_SIMILARITY,
                exclude_wiki_ids=exclude_wiki_ids,
            )
        logger.info(
            "[STAGE_DONE] job=%s stage=knn_lookup elapsed=%.2fs neighbors=%s "
            "target_wiki=%s related=%s",
            job.id,
            time.perf_counter() - stage_started,
            len(neighbors),
            target_wiki_id,
            len(related_wikis),
        )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=generate_wiki", job.id)
        wiki = wiki_generator.generate_wiki(
            payload, neighbors, related_wikis, lm_configs, openai_client
        )
        logger.info(
            "[STAGE_DONE] job=%s stage=generate_wiki elapsed=%.2fs",
            job.id,
            time.perf_counter() - stage_started,
        )

        if pipeline.ENABLE_VALIDATION:
            stage_started = time.perf_counter()
            logger.info("[STAGE_START] job=%s stage=validation", job.id)
            result = validator.validate(
                wiki["content_markdown"],
                payload.document.body_text,
                openai_client,
            )
            for attempt in range(1, pipeline.MAX_VALIDATION_RETRIES + 1):
                if result.passed:
                    break
                logger.info(
                    "[STAGE_START] job=%s stage=repolish attempt=%s issues=%s",
                    job.id,
                    attempt,
                    len(result.issues),
                )
                issues_text = validator.format_issues_for_prompt(result.issues)
                wiki["content_markdown"] = wiki_generator.repolish_with_feedback(
                    wiki["content_markdown"], issues_text
                )
                result = validator.validate(
                    wiki["content_markdown"],
                    payload.document.body_text,
                    openai_client,
                )
                logger.info(
                    "[STAGE_DONE] job=%s stage=repolish attempt=%s passed=%s "
                    "remaining_issues=%s",
                    job.id,
                    attempt,
                    result.passed,
                    len(result.issues),
                )
            logger.info(
                "[STAGE_DONE] job=%s stage=validation elapsed=%.2fs passed=%s "
                "final_issues=%s",
                job.id,
                time.perf_counter() - stage_started,
                result.passed,
                len(result.issues),
            )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=wiki_embedding", job.id)
        wiki_embedding = wiki_generator.compute_embedding(
            openai_client, wiki["content_markdown"]
        )
        wiki_content_hash = wiki_generator.compute_content_hash(
            wiki["content_markdown"]
        )
        logger.info(
            "[STAGE_DONE] job=%s stage=wiki_embedding elapsed=%.2fs dimensions=%s",
            job.id,
            time.perf_counter() - stage_started,
            len(wiki_embedding),
        )

        stage_started = time.perf_counter()
        logger.info("[STAGE_START] job=%s stage=db_write", job.id)
        with db_writer.transaction() as conn:
            own_chunk_id = db_writer.insert_chunk_with_embedding(
                conn,
                source_document_id=job.source_document_id,
                content_text=payload.document.body_text,
                embedding=embedding,
            )
            all_chunk_ids = [str(own_chunk_id), *neighbor_chunk_ids]
            if target_wiki_id:
                db_writer.insert_wiki_revision(
                    conn,
                    wiki_document_id=target_wiki_id,
                    summary=wiki["summary"],
                    content_markdown=wiki["content_markdown"],
                    generation_model=settings.SYNTHESIS_MODEL,
                    source_chunk_ids=all_chunk_ids,
                    wiki_embedding=wiki_embedding,
                    wiki_content_hash=wiki_content_hash,
                )
            else:
                slug = db_writer.make_unique_slug(conn, wiki["slug_base"])
                db_writer.insert_wiki(
                    conn,
                    title=wiki["title"],
                    slug=slug,
                    summary=wiki["summary"],
                    content_markdown=wiki["content_markdown"],
                    generation_model=settings.SYNTHESIS_MODEL,
                    source_chunk_ids=all_chunk_ids,
                    wiki_embedding=wiki_embedding,
                    wiki_content_hash=wiki_content_hash,
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
