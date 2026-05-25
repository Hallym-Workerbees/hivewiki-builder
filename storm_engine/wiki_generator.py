import logging
import re
import shutil
import time

from openai import OpenAI

from config import pipeline, settings
from data.payload import JobPayload
from storm_engine.wiki_runner import run_storm_for_cluster

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = (
    "다음 위키 문서를 한국어로 2~3문장으로 요약하라. "
    "핵심 정보만 포함하고 군더더기 없이 작성하라.\n\n---\n\n{content}"
)
SUMMARY_MAX_TOKENS = 300
EMBEDDING_INPUT_MAX_CHARS = 8000


def compute_embedding(client: OpenAI, text: str) -> list[float]:
    resp = client.embeddings.create(
        model=settings.EMBEDDING_MODEL,
        input=text[:EMBEDDING_INPUT_MAX_CHARS],
    )
    return resp.data[0].embedding


def generate_summary(client: OpenAI, content: str) -> str:
    resp = client.chat.completions.create(
        model=settings.AGENT_MODEL,
        messages=[
            {
                "role": "user",
                "content": SUMMARY_PROMPT.format(
                    content=content[:EMBEDDING_INPUT_MAX_CHARS]
                ),
            }
        ],
        max_tokens=SUMMARY_MAX_TOKENS,
        temperature=0.3,
    )
    return resp.choices[0].message.content.strip()


def make_slug_base(title: str) -> str:
    s = title.strip()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^\w\-가-힣]", "", s, flags=re.UNICODE)
    s = s[:200]
    return s or "untitled"


def _payload_to_notice(payload: JobPayload) -> dict:
    doc = payload.document
    return {
        "title": doc.title,
        "department": payload.source.name,
        "content": doc.body_text,
        "link": doc.canonical_url,
    }


def _run_storm(notice: dict, lm_configs, job_id: int) -> str:
    topic = f"job-{job_id}"
    pipeline.STORM_WORK_DIR.mkdir(parents=True, exist_ok=True)
    topic_dir = pipeline.STORM_WORK_DIR / topic
    if topic_dir.exists():
        logger.info(
            "[STAGE_START] job=%s stage=storm_cleanup path=%s",
            job_id,
            topic_dir,
        )
        shutil.rmtree(topic_dir)
        logger.info("[STAGE_DONE] job=%s stage=storm_cleanup", job_id)
    return run_storm_for_cluster([notice], topic, lm_configs, pipeline.STORM_WORK_DIR)


def generate_wiki(payload: JobPayload, lm_configs, openai_client: OpenAI) -> dict:
    started = time.perf_counter()
    logger.info(
        "[STAGE_START] job=%s stage=wiki_generation title=%r",
        payload.job.id,
        payload.document.title,
    )
    notice = _payload_to_notice(payload)

    stage_started = time.perf_counter()
    logger.info("[STAGE_START] job=%s stage=storm", payload.job.id)
    content_markdown = _run_storm(notice, lm_configs, payload.job.id)
    logger.info(
        "[STAGE_DONE] job=%s stage=storm elapsed=%.2fs content_chars=%s",
        payload.job.id,
        time.perf_counter() - stage_started,
        len(content_markdown),
    )

    stage_started = time.perf_counter()
    logger.info(
        "[STAGE_START] job=%s stage=summary model=%s content_chars=%s",
        payload.job.id,
        settings.AGENT_MODEL,
        len(content_markdown),
    )
    summary = generate_summary(openai_client, content_markdown)
    logger.info(
        "[STAGE_DONE] job=%s stage=summary elapsed=%.2fs summary_chars=%s",
        payload.job.id,
        time.perf_counter() - stage_started,
        len(summary),
    )

    stage_started = time.perf_counter()
    logger.info(
        "[STAGE_START] job=%s stage=embedding model=%s input_chars=%s",
        payload.job.id,
        settings.EMBEDDING_MODEL,
        min(len(payload.document.body_text), EMBEDDING_INPUT_MAX_CHARS),
    )
    embedding = compute_embedding(openai_client, payload.document.body_text)
    logger.info(
        "[STAGE_DONE] job=%s stage=embedding elapsed=%.2fs dimensions=%s",
        payload.job.id,
        time.perf_counter() - stage_started,
        len(embedding),
    )
    logger.info(
        "[STAGE_DONE] job=%s stage=wiki_generation elapsed=%.2fs",
        payload.job.id,
        time.perf_counter() - started,
    )

    return {
        "title": payload.document.title,
        "slug_base": make_slug_base(payload.document.title),
        "summary": summary,
        "content_markdown": content_markdown,
        "embedding": embedding,
        "chunk_content": payload.document.body_text,
    }
