import json
import logging
import re
import time
from pathlib import Path

import dspy
from knowledge_storm import STORMWikiRunner, STORMWikiRunnerArguments

from config import pipeline
from storm_engine.outline_generator import generate_outline_from_notices

logger = logging.getLogger(__name__)


class _WikipediaPersonaErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not ("Error occurs when processing" in msg and "wikipedia.org" in msg)


logging.getLogger().addFilter(_WikipediaPersonaErrorFilter())


def clean_title(raw: str) -> str:
    return raw.rstrip("}").strip()


class DBNoticeRetriever(dspy.Retrieve):
    def __init__(self, db_notices: list, k: int = 5):
        super().__init__(k=k)
        self.db_notices = db_notices

    def get_usage_and_reset(self):
        return {"DBNoticeRetriever": 0}

    def forward(
        self,
        query_or_queries: str | list[str],
        exclude_urls: list[str] | None = None,
    ) -> list[dict]:
        queries = (
            [query_or_queries]
            if isinstance(query_or_queries, str)
            else query_or_queries
        )
        started = time.perf_counter()

        results = []
        seen_urls = set(exclude_urls or [])

        for query in queries:
            query_lower = query.lower()
            scored = []
            for notice in self.db_notices:
                score = sum(
                    1
                    for word in query_lower.split()
                    if word in notice["title"].lower()
                    or word in notice["content"].lower()
                    or word in notice["department"].lower()
                )
                scored.append((score, notice))

            scored.sort(key=lambda x: x[0], reverse=True)

            for _, notice in scored[: self.k]:
                url = notice.get("link") or (
                    f"hallym-notice-{notice['title'].replace(' ', '_')}"
                )
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                results.append(
                    {
                        "url": url,
                        "title": notice["title"],
                        "description": f"부서: {notice['department']}",
                        "snippets": [
                            f"부서: {notice['department']}\n내용: {notice['content']}"
                        ],
                    }
                )

        logger.debug(
            "[RETRIEVER] queries=%s notices=%s results=%s elapsed=%.2fs",
            len(queries),
            len(self.db_notices),
            len(results),
            time.perf_counter() - started,
        )
        return results


def load_url_to_info(work_dir: Path, topic: str) -> dict[int, dict]:
    path = work_dir / topic / "url_to_info.json"
    with open(path, encoding="utf-8") as f:
        ref_data = json.load(f)
    index_to_meta: dict[int, dict] = {}
    url_to_index: dict[str, int] = ref_data.get("url_to_unified_index", {})
    url_to_info: dict[str, dict] = ref_data.get("url_to_info", {})
    for url, info in url_to_info.items():
        idx = url_to_index.get(url)
        if idx is not None:
            index_to_meta[idx] = {
                "title": clean_title(info.get("title", "공지사항")),
                "url": url,
            }
    return index_to_meta


def replace_citations(text: str, index_to_meta: dict[int, dict]) -> str:
    cited: set[int] = set()

    def _replacer(m: re.Match) -> str:
        idx = int(m.group(1))
        if idx in index_to_meta:
            cited.add(idx)
            return f"[^{idx}]"
        return m.group(0)

    body = re.sub(r"\[(\d+)\]", _replacer, text)

    if not cited:
        return body

    refs = ["# 참고 문헌", ""]
    for idx in sorted(cited):
        meta = index_to_meta[idx]
        refs.append(f"[^{idx}]: [{meta['title']}]({meta['url']})")

    return body.rstrip() + "\n\n" + "\n".join(refs) + "\n"


def write_single_notice_md(notice: dict, filepath: Path) -> None:
    parts = [f"# {notice['title']}", ""]
    meta = []
    if notice.get("department"):
        meta.append(f"**부서:** {notice['department']}")
    if notice.get("date"):
        meta.append(f"**작성일:** {notice['date']}")
    if meta:
        parts.append("  \n".join(meta))
        parts.append("")
    parts.append(notice.get("content", ""))
    if notice.get("link"):
        parts.append("")
        parts.append("---")
        parts.append("")
        parts.append(f"[출처: {notice['title']}]({notice['link']})")
    filepath.write_text("\n".join(parts) + "\n", encoding="utf-8")


_OUTLINE_BOILERPLATE_HEADINGS = {
    "Academic Sources",
    "Official Documents",
    "Related Literature",
    "Official Website",
    "Related Organizations",
    "Additional Resources",
    "References",
    "Further Reading",
    "See Also",
    "External Links",
}


def _clean_outline_placeholders(outline_path: Path) -> None:
    if not outline_path.exists():
        logger.warning("[WARN] outline_missing path=%s", outline_path)
        return
    lines = outline_path.read_text(encoding="utf-8").splitlines()
    cleaned: list[str] = []
    skip = False
    for line in lines:
        stripped = line.strip()
        is_top_heading = stripped.startswith("# ") and not stripped.startswith("## ")
        is_sub_heading = stripped.startswith("## ") and not stripped.startswith("### ")
        if is_top_heading or is_sub_heading:
            heading_text = stripped.lstrip("#").strip()
            if heading_text in _OUTLINE_BOILERPLATE_HEADINGS:
                skip = True
                continue
            if is_top_heading:
                skip = False
        if not skip:
            cleaned.append(line)
    outline_path.write_text("\n".join(cleaned) + "\n", encoding="utf-8")
    logger.info(
        "[STAGE_DONE] stage=outline_cleanup path=%s original_lines=%s cleaned_lines=%s",
        outline_path,
        len(lines),
        len(cleaned),
    )


def _outline_top_headings(outline_path: Path) -> set[str]:
    if not outline_path.exists():
        return set()
    headings: set[str] = set()
    for line in outline_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("## "):
            headings.add(stripped.lstrip("#").strip())
    return headings


def _strip_unknown_top_sections(article_path: Path, valid_headings: set[str]) -> None:
    if not article_path.exists() or not valid_headings:
        return
    lines = article_path.read_text(encoding="utf-8").splitlines()
    cleaned: list[str] = []
    skip = False
    for line in lines:
        stripped = line.strip()
        is_top = stripped.startswith("# ") and not stripped.startswith("## ")
        if is_top:
            heading_text = stripped.lstrip("#").strip()
            if heading_text not in valid_headings:
                skip = True
                continue
            skip = False
        if not skip:
            cleaned.append(line)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    article_path.write_text("\n".join(cleaned) + "\n", encoding="utf-8")


def _strip_outline_top_as_sub(article_path: Path, top_headings: set[str]) -> None:
    if not article_path.exists() or not top_headings:
        return
    lines = article_path.read_text(encoding="utf-8").splitlines()
    cleaned: list[str] = []
    skip = False
    for line in lines:
        stripped = line.strip()
        is_top = stripped.startswith("# ") and not stripped.startswith("## ")
        is_sub = stripped.startswith("## ") and not stripped.startswith("### ")
        if is_top:
            skip = False
        elif is_sub:
            heading_text = stripped.lstrip("#").strip()
            if heading_text in top_headings:
                skip = True
                continue
            skip = False
        if not skip:
            cleaned.append(line)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    article_path.write_text("\n".join(cleaned) + "\n", encoding="utf-8")


def run_storm_for_cluster(
    cluster_notices: list[dict],
    filename: str,
    lm_configs,
    work_dir: Path = pipeline.STORM_WORK_DIR,
) -> str:
    started = time.perf_counter()
    logger.info(
        "[STAGE_START] stage=storm_runner topic=%s notices=%s work_dir=%s "
        "max_conv_turn=%s max_perspective=%s retriever_k=%s",
        filename,
        len(cluster_notices),
        work_dir,
        pipeline.STORM_MAX_CONV_TURN,
        pipeline.STORM_MAX_PERSPECTIVE,
        min(pipeline.STORM_RETRIEVER_K, len(cluster_notices)),
    )
    rm = DBNoticeRetriever(
        db_notices=cluster_notices,
        k=min(pipeline.STORM_RETRIEVER_K, len(cluster_notices)),
    )
    runner = STORMWikiRunner(
        STORMWikiRunnerArguments(
            output_dir=str(work_dir),
            max_conv_turn=pipeline.STORM_MAX_CONV_TURN,
            max_perspective=pipeline.STORM_MAX_PERSPECTIVE,
        ),
        lm_configs,
        rm,
    )

    topic_dir = work_dir / filename
    topic_dir.mkdir(parents=True, exist_ok=True)
    outline_path = topic_dir / "storm_gen_outline.txt"
    outline_path.write_text(
        generate_outline_from_notices(cluster_notices) + "\n",
        encoding="utf-8",
    )

    run_started = time.perf_counter()
    logger.info("[STAGE_START] stage=storm_runner_run topic=%s", filename)
    runner.run(
        topic=filename,
        do_research=True,
        do_generate_outline=False,
        do_generate_article=True,
        do_polish_article=True,
        remove_duplicate=True,
    )
    logger.info(
        "[STAGE_DONE] stage=storm_runner_run topic=%s elapsed=%.2fs",
        filename,
        time.perf_counter() - run_started,
    )
    _clean_outline_placeholders(outline_path)
    polished = work_dir / filename / "storm_gen_article_polished.txt"
    if not polished.exists():
        logger.error("[ERROR] polished_article_missing path=%s", polished)
    top_headings = _outline_top_headings(outline_path)
    _strip_unknown_top_sections(polished, top_headings)
    _strip_outline_top_as_sub(polished, top_headings)
    raw = polished.read_text(encoding="utf-8")
    index_to_meta = load_url_to_info(work_dir, filename)
    logger.info(
        "[STAGE_DONE] stage=storm_runner topic=%s elapsed=%.2fs article_chars=%s "
        "citations=%s",
        filename,
        time.perf_counter() - started,
        len(raw),
        len(index_to_meta),
    )
    return replace_citations(raw, index_to_meta)
