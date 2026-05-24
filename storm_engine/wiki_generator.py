import logging
import re
import shutil

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
        shutil.rmtree(topic_dir)
    return run_storm_for_cluster([notice], topic, lm_configs, pipeline.STORM_WORK_DIR)


def generate_wiki(payload: JobPayload, lm_configs, openai_client: OpenAI) -> dict:
    logger.info(f"[STORM 시작] job={payload.job.id} title={payload.document.title}")
    notice = _payload_to_notice(payload)
    content_markdown = _run_storm(notice, lm_configs, payload.job.id)

    logger.info(f"[summary 생성] model={settings.AGENT_MODEL}")
    summary = generate_summary(openai_client, content_markdown)

    logger.info(f"[embedding 생성] model={settings.EMBEDDING_MODEL}")
    embedding = compute_embedding(openai_client, payload.document.body_text)

    return {
        "title": payload.document.title,
        "slug_base": make_slug_base(payload.document.title),
        "summary": summary,
        "content_markdown": content_markdown,
        "embedding": embedding,
        "chunk_content": payload.document.body_text,
    }
