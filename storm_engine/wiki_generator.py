import hashlib
import logging
import re
import shutil
import time

import anthropic
from openai import OpenAI

from config import pipeline, settings
from data.db_reader import NeighborChunk, RelatedWiki
from data.payload import JobPayload
from storm_engine.wiki_runner import run_storm_for_cluster

logger = logging.getLogger(__name__)

SUMMARY_PROMPT = (
    "다음 위키 문서를 한국어로 2~3문장으로 요약하라. "
    "핵심 정보만 포함하고 군더더기 없이 작성하라.\n\n---\n\n{content}"
)
SUMMARY_MAX_TOKENS = 300
EMBEDDING_INPUT_MAX_CHARS = 8000
RELATED_SECTION_HEADER = "## 관련 문서"
WIKI_URL_TEMPLATE = "/wiki/{slug}"

REPOLISH_SYSTEM_PROMPT = (
    "당신은 한국어 위키 문서 편집자입니다. "
    "검증 에이전트가 발견한 문제만 정확히 수정하고, 그 외 부분은 그대로 유지하세요. "
    "마크다운 헤딩(#, ##), footnote 표기([^n]), # 참고 문헌 섹션을 보존하세요. "
    "설명·해설·머리말 추가 금지. 수정된 위키 본문 전체만 출력하세요."
)

REPOLISH_USER_PROMPT = """검증에서 발견된 문제:

{issues}

원본 위키 본문:

{wiki}

위 문제들만 수정한 위키 본문 전체를 출력하세요."""

REPOLISH_MAX_TOKENS = 4000


def compute_embedding(client: OpenAI, text: str) -> list[float]:
    resp = client.embeddings.create(
        model=settings.EMBEDDING_MODEL,
        input=text[:EMBEDDING_INPUT_MAX_CHARS],
    )
    return resp.data[0].embedding


def compute_content_hash(text: str) -> str:
    return hashlib.sha256(text[:EMBEDDING_INPUT_MAX_CHARS].encode("utf-8")).hexdigest()


def repolish_with_feedback(wiki_markdown: str, issues_text: str) -> str:
    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=settings.SYNTHESIS_MODEL,
        max_tokens=REPOLISH_MAX_TOKENS,
        system=REPOLISH_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": REPOLISH_USER_PROMPT.format(
                    issues=issues_text, wiki=wiki_markdown
                ),
            }
        ],
    )
    return resp.content[0].text.strip()


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


def _neighbor_to_notice(neighbor: NeighborChunk) -> dict:
    return {
        "title": neighbor.title,
        "department": "",
        "content": neighbor.content_text,
        "link": neighbor.canonical_url,
    }


def _build_cluster(payload: JobPayload, neighbors: list[NeighborChunk]) -> list[dict]:
    return [_payload_to_notice(payload)] + [_neighbor_to_notice(n) for n in neighbors]


def _append_related_section(
    content_markdown: str, related_wikis: list[RelatedWiki]
) -> str:
    if not related_wikis:
        return content_markdown
    lines = [RELATED_SECTION_HEADER, ""]
    for wiki in related_wikis:
        url = WIKI_URL_TEMPLATE.format(slug=wiki.slug)
        lines.append(f"- [{wiki.title}]({url})")
    return content_markdown.rstrip() + "\n\n" + "\n".join(lines) + "\n"


def _run_storm(
    cluster_notices: list[dict],
    topic_slug: str,
    lm_configs,
    job_id: int,
) -> str:
    pipeline.STORM_WORK_DIR.mkdir(parents=True, exist_ok=True)
    topic_dir = pipeline.STORM_WORK_DIR / topic_slug
    if topic_dir.exists():
        logger.info(
            "[STAGE_START] job=%s stage=storm_cleanup path=%s",
            job_id,
            topic_dir,
        )
        shutil.rmtree(topic_dir)
        logger.info("[STAGE_DONE] job=%s stage=storm_cleanup", job_id)
    try:
        return run_storm_for_cluster(
            cluster_notices,
            topic_slug,
            lm_configs,
            pipeline.STORM_WORK_DIR,
        )
    finally:
        if topic_dir.exists():
            logger.info(
                "[STAGE_START] job=%s stage=storm_cleanup_final path=%s",
                job_id,
                topic_dir,
            )
            try:
                shutil.rmtree(topic_dir)
            except OSError:
                logger.exception(
                    "[ERROR] job=%s stage=storm_cleanup_final path=%s",
                    job_id,
                    topic_dir,
                )
            else:
                logger.info("[STAGE_DONE] job=%s stage=storm_cleanup_final", job_id)


def generate_wiki(
    payload: JobPayload,
    neighbors: list[NeighborChunk],
    related_wikis: list[RelatedWiki],
    lm_configs,
    openai_client: OpenAI,
) -> dict:
    started = time.perf_counter()
    cluster = _build_cluster(payload, neighbors)
    logger.info(
        "[STAGE_START] job=%s stage=wiki_generation title=%r cluster_size=%s "
        "neighbors=%s related=%s",
        payload.job.id,
        payload.document.title,
        len(cluster),
        len(neighbors),
        len(related_wikis),
    )

    topic_slug = make_slug_base(payload.document.title)

    stage_started = time.perf_counter()
    logger.info(
        "[STAGE_START] job=%s stage=storm cluster_size=%s",
        payload.job.id,
        len(cluster),
    )
    content_markdown = _run_storm(cluster, topic_slug, lm_configs, payload.job.id)
    logger.info(
        "[STAGE_DONE] job=%s stage=storm elapsed=%.2fs content_chars=%s",
        payload.job.id,
        time.perf_counter() - stage_started,
        len(content_markdown),
    )

    content_markdown = _append_related_section(content_markdown, related_wikis)

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

    logger.info(
        "[STAGE_DONE] job=%s stage=wiki_generation elapsed=%.2fs",
        payload.job.id,
        time.perf_counter() - started,
    )

    return {
        "title": payload.document.title,
        "slug_base": topic_slug,
        "summary": summary,
        "content_markdown": content_markdown,
    }
