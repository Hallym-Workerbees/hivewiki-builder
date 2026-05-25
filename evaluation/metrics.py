import json
import re
from collections.abc import Callable
from math import sqrt
from pathlib import Path

LM_PRICING_USD_PER_1M_TOKENS: dict[str, dict[str, float]] = {
    "gpt-4o-mini": {"prompt": 0.15, "completion": 0.60},
    "claude-3-5-sonnet-20240620": {"prompt": 3.00, "completion": 15.00},
    "text-embedding-3-small": {"prompt": 0.02, "completion": 0.0},
}

CITATION_PATTERN = re.compile(r"\[(\d+)\]")
HEADING_PATTERN = re.compile(r"^(#+)\s+(.+?)\s*$")
WORD_PATTERN = re.compile(r"\w+", re.UNICODE)

Embedder = Callable[[list[str]], list[list[float]]]


def compute_metrics(
    cluster_output_dir: Path,
    input_notice_count: int,
    lm_usage: dict,
    wall_time_s: float,
    embedder: Embedder | None = None,
) -> dict:
    outline_path = cluster_output_dir / "storm_gen_outline.txt"
    article_path = cluster_output_dir / "storm_gen_article_polished.txt"
    url_info_path = cluster_output_dir / "url_to_info.json"

    outline_text = (
        outline_path.read_text(encoding="utf-8") if outline_path.exists() else ""
    )
    article_text = (
        article_path.read_text(encoding="utf-8") if article_path.exists() else ""
    )
    url_info_data = (
        json.loads(url_info_path.read_text(encoding="utf-8"))
        if url_info_path.exists()
        else {}
    )

    return {
        "A_cost": compute_cost_metrics(lm_usage, wall_time_s),
        "B_structure": compute_structure_metrics(outline_text, article_text, embedder),
        "C_information": compute_information_metrics(
            article_text, url_info_data, input_notice_count
        ),
    }


def compute_cost_metrics(lm_usage: dict, wall_time_s: float) -> dict:
    total_prompt = 0
    total_completion = 0
    total_calls = 0
    total_cost_usd = 0.0
    per_model: dict[str, dict] = {}

    for model_name, usage in lm_usage.items():
        prompt_tok = usage.get("prompt_tokens", 0)
        comp_tok = usage.get("completion_tokens", 0)
        calls = usage.get("call_count", 0)
        pricing = LM_PRICING_USD_PER_1M_TOKENS.get(
            model_name, {"prompt": 0.0, "completion": 0.0}
        )
        cost = (
            prompt_tok * pricing["prompt"] + comp_tok * pricing["completion"]
        ) / 1_000_000
        per_model[model_name] = {
            "prompt_tokens": prompt_tok,
            "completion_tokens": comp_tok,
            "call_count": calls,
            "cost_usd": round(cost, 6),
        }
        total_prompt += prompt_tok
        total_completion += comp_tok
        total_calls += calls
        total_cost_usd += cost

    return {
        "wall_time_s": round(wall_time_s, 2),
        "total_prompt_tokens": total_prompt,
        "total_completion_tokens": total_completion,
        "total_call_count": total_calls,
        "total_cost_usd": round(total_cost_usd, 6),
        "per_model": per_model,
    }


def compute_structure_metrics(
    outline_text: str, article_text: str, embedder: Embedder | None
) -> dict:
    section_levels = [
        len(m.group(1))
        for line in outline_text.splitlines()
        if (m := HEADING_PATTERN.match(line))
    ]
    outline_section_count = len(section_levels)
    outline_max_depth = max(section_levels) if section_levels else 0

    article_len = len(article_text)
    article_word_count = len(WORD_PATTERN.findall(article_text))

    all_citations = CITATION_PATTERN.findall(article_text)
    total_citations = len(all_citations)
    unique_citations = len(set(all_citations))
    citation_density_per_1k = (
        total_citations / article_len * 1000 if article_len else 0.0
    )

    section_texts = split_article_by_headings(article_text)
    cosine_stats = compute_section_cosine_stats(section_texts, embedder)

    trigram_repetition = compute_trigram_repetition_rate(article_text)

    return {
        "outline_section_count": outline_section_count,
        "outline_max_depth": outline_max_depth,
        "article_char_count": article_len,
        "article_word_count": article_word_count,
        "citation_total": total_citations,
        "citation_unique": unique_citations,
        "citation_density_per_1k_char": round(citation_density_per_1k, 3),
        "section_cosine": cosine_stats,
        "trigram_repetition_rate": round(trigram_repetition, 4),
    }


def split_article_by_headings(text: str) -> list[str]:
    sections: list[str] = []
    current: list[str] = []
    started = False
    for line in text.splitlines():
        if HEADING_PATTERN.match(line):
            if started and current:
                body = "\n".join(current).strip()
                if body:
                    sections.append(body)
            current = []
            started = True
            continue
        if started:
            current.append(line)
    if started and current:
        body = "\n".join(current).strip()
        if body:
            sections.append(body)
    return sections


def compute_section_cosine_stats(
    section_texts: list[str], embedder: Embedder | None
) -> dict:
    n = len(section_texts)
    if n < 2 or embedder is None:
        return {"mean": None, "max": None, "section_count": n}
    embeddings = embedder(section_texts)
    sims: list[float] = []
    for i in range(n):
        for j in range(i + 1, n):
            sims.append(cosine_similarity(embeddings[i], embeddings[j]))
    return {
        "mean": round(sum(sims) / len(sims), 4),
        "max": round(max(sims), 4),
        "section_count": n,
    }


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    na = sqrt(sum(x * x for x in a))
    nb = sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def compute_trigram_repetition_rate(text: str) -> float:
    tokens = WORD_PATTERN.findall(text)
    if len(tokens) < 3:
        return 0.0
    trigrams = [tuple(tokens[i : i + 3]) for i in range(len(tokens) - 2)]
    total = len(trigrams)
    unique = len(set(trigrams))
    return (total - unique) / total if total else 0.0


def compute_information_metrics(
    article_text: str, url_info_data: dict, input_notice_count: int
) -> dict:
    cited_indices = {int(x) for x in CITATION_PATTERN.findall(article_text)}

    url_to_index: dict[str, int] = url_info_data.get("url_to_unified_index", {})
    available_indices = set(url_to_index.values())
    available_count = len(available_indices)
    cited_within_available = cited_indices & available_indices

    reference_utilization_rate = (
        len(cited_within_available) / available_count if available_count else 0.0
    )
    source_coverage = (
        len(cited_within_available) / input_notice_count if input_notice_count else 0.0
    )

    return {
        "input_notice_count": input_notice_count,
        "retriever_url_count": available_count,
        "cited_unique_count": len(cited_indices),
        "cited_within_available_count": len(cited_within_available),
        "reference_utilization_rate": round(reference_utilization_rate, 4),
        "source_coverage": round(source_coverage, 4),
    }


def make_openai_embedder(client) -> Embedder:
    def embed(texts: list[str]) -> list[list[float]]:
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=texts,
        )
        return [item.embedding for item in response.data]

    return embed
