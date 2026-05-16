import json
import re
from pathlib import Path

import dspy
from knowledge_storm import STORMWikiRunner, STORMWikiRunnerArguments

from config import pipeline


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
    def _replacer(m: re.Match) -> str:
        idx = int(m.group(1))
        if idx in index_to_meta:
            t = index_to_meta[idx]["title"]
            u = index_to_meta[idx]["url"]
            return f"[출처: {t}]({u})"
        return m.group(0)

    return re.sub(r"\[(\d+)\]", _replacer, text)


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


def run_storm_for_cluster(
    cluster_notices: list[dict],
    filename: str,
    lm_configs,
    work_dir: Path = pipeline.STORM_WORK_DIR,
) -> str:
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
    runner.run(
        topic=filename,
        do_research=True,
        do_generate_outline=True,
        do_generate_article=True,
        do_polish_article=True,
    )
    polished = work_dir / filename / "storm_gen_article_polished.txt"
    raw = polished.read_text(encoding="utf-8")
    index_to_meta = load_url_to_info(work_dir, filename)
    return replace_citations(raw, index_to_meta)
