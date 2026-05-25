import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from anthropic import Anthropic
from knowledge_storm import STORMWikiRunner, STORMWikiRunnerArguments
from openai import OpenAI

from config import pipeline, settings
from data.dataloader import load_notices_from_json
from evaluation.metrics import Embedder, compute_metrics, make_openai_embedder
from evaluation.rubric import score_rubric
from storm_engine.clusterer import (
    assign_cluster_filenames,
    cluster_by_similarity,
    embed_notices,
)
from storm_engine.llm_config import setup_llms
from storm_engine.wiki_runner import DBNoticeRetriever


@dataclass(frozen=True)
class HyperParams:
    turn: int
    perspective: int
    k: int

    def label(self) -> str:
        return f"turn{self.turn}_persp{self.perspective}_k{self.k}"


CSV_FIELDS: list[str] = [
    "combo_label",
    "turn",
    "perspective",
    "k",
    "topic",
    "input_notice_count",
    "wall_time_s",
    "total_prompt_tokens",
    "total_completion_tokens",
    "total_cost_usd",
    "outline_section_count",
    "outline_max_depth",
    "article_char_count",
    "article_word_count",
    "citation_total",
    "citation_unique",
    "citation_density_per_1k_char",
    "section_count",
    "cosine_mean",
    "cosine_max",
    "trigram_repetition_rate",
    "retriever_url_count",
    "cited_unique_count",
    "cited_within_available_count",
    "reference_utilization_rate",
    "source_coverage",
    "rubric_interest",
    "rubric_relevance",
    "rubric_broad_coverage",
    "rubric_depth",
    "rubric_organization",
]


def prepare_clusters(
    input_json_path: Path, openai_client: OpenAI
) -> list[tuple[list[dict], str]]:
    notices = load_notices_from_json(input_json_path)
    embeddings = embed_notices(notices, openai_client)
    clusters = cluster_by_similarity(embeddings, pipeline.CLUSTER_THRESHOLD)
    filenames = assign_cluster_filenames(clusters, notices, openai_client)
    return [
        ([notices[i] for i in indices], filename)
        for indices, filename in zip(clusters, filenames, strict=True)
        if len(indices) >= 2
    ]


def aggregate_lm_cost(
    lm_cost: dict[str, dict[str, dict[str, int]]],
) -> dict[str, dict[str, int]]:
    total: dict[str, dict[str, int]] = {}
    for module_usage in lm_cost.values():
        for model_name, tokens in module_usage.items():
            slot = total.setdefault(
                model_name, {"prompt_tokens": 0, "completion_tokens": 0}
            )
            slot["prompt_tokens"] += tokens.get("prompt_tokens", 0)
            slot["completion_tokens"] += tokens.get("completion_tokens", 0)
    return total


def run_storm_for_bench(
    cluster_notices: list[dict],
    filename: str,
    params: HyperParams,
    lm_configs,
    output_dir: Path,
) -> tuple[Path, dict, float]:
    rm = DBNoticeRetriever(
        db_notices=cluster_notices,
        k=min(params.k, len(cluster_notices)),
    )
    runner = STORMWikiRunner(
        STORMWikiRunnerArguments(
            output_dir=str(output_dir),
            max_conv_turn=params.turn,
            max_perspective=params.perspective,
        ),
        lm_configs,
        rm,
    )

    start = time.time()
    runner.run(
        topic=filename,
        do_research=True,
        do_generate_outline=True,
        do_generate_article=True,
        do_polish_article=True,
        remove_duplicate=True,
    )
    wall_time_s = time.time() - start

    lm_usage = aggregate_lm_cost(getattr(runner, "lm_cost", {}))
    cluster_dir = output_dir / filename
    return cluster_dir, lm_usage, wall_time_s


def bench_single(
    cluster_notices: list[dict],
    filename: str,
    params: HyperParams,
    lm_configs,
    embedder: Embedder,
    judge_client: Anthropic,
    bench_root: Path,
) -> dict[str, Any]:
    combo_dir = bench_root / params.label()
    combo_dir.mkdir(parents=True, exist_ok=True)

    cluster_dir, lm_usage, wall_time_s = run_storm_for_bench(
        cluster_notices, filename, params, lm_configs, combo_dir
    )

    metrics = compute_metrics(
        cluster_output_dir=cluster_dir,
        input_notice_count=len(cluster_notices),
        lm_usage=lm_usage,
        wall_time_s=wall_time_s,
        embedder=embedder,
    )

    polished_path = cluster_dir / "storm_gen_article_polished.txt"
    article_text = polished_path.read_text(encoding="utf-8")
    rubric_scores = score_rubric(article_text, judge_client)

    return {
        "combo": params,
        "topic": filename,
        "input_notice_count": len(cluster_notices),
        "metrics": metrics,
        "rubric": rubric_scores,
    }


def flatten_row(result: dict[str, Any]) -> dict[str, Any]:
    combo: HyperParams = result["combo"]
    metrics = result["metrics"]
    cost = metrics["A_cost"]
    struct = metrics["B_structure"]
    info = metrics["C_information"]
    cosine = struct["section_cosine"]
    rubric = result["rubric"]

    return {
        "combo_label": combo.label(),
        "turn": combo.turn,
        "perspective": combo.perspective,
        "k": combo.k,
        "topic": result["topic"],
        "input_notice_count": result["input_notice_count"],
        "wall_time_s": cost["wall_time_s"],
        "total_prompt_tokens": cost["total_prompt_tokens"],
        "total_completion_tokens": cost["total_completion_tokens"],
        "total_cost_usd": cost["total_cost_usd"],
        "outline_section_count": struct["outline_section_count"],
        "outline_max_depth": struct["outline_max_depth"],
        "article_char_count": struct["article_char_count"],
        "article_word_count": struct["article_word_count"],
        "citation_total": struct["citation_total"],
        "citation_unique": struct["citation_unique"],
        "citation_density_per_1k_char": struct["citation_density_per_1k_char"],
        "section_count": cosine["section_count"],
        "cosine_mean": cosine["mean"],
        "cosine_max": cosine["max"],
        "trigram_repetition_rate": struct["trigram_repetition_rate"],
        "retriever_url_count": info["retriever_url_count"],
        "cited_unique_count": info["cited_unique_count"],
        "cited_within_available_count": info["cited_within_available_count"],
        "reference_utilization_rate": info["reference_utilization_rate"],
        "source_coverage": info["source_coverage"],
        "rubric_interest": rubric["interest"],
        "rubric_relevance": rubric["relevance"],
        "rubric_broad_coverage": rubric["broad_coverage"],
        "rubric_depth": rubric["depth"],
        "rubric_organization": rubric["organization"],
    }


def append_csv(csv_path: Path, row: dict[str, Any]) -> None:
    is_new = not csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def run_sweep(
    cluster_groups: list[tuple[list[dict], str]],
    combos: list[HyperParams],
    lm_configs,
    embedder: Embedder,
    judge_client: Anthropic,
    bench_root: Path,
    csv_path: Path,
) -> None:
    total_runs = len(combos) * len(cluster_groups)
    run_idx = 0
    for combo in combos:
        for cluster_notices, filename in cluster_groups:
            run_idx += 1
            print(
                f"[sweep {run_idx}/{total_runs}] combo={combo.label()} "
                f"topic={filename} size={len(cluster_notices)}"
            )
            result = bench_single(
                cluster_notices,
                filename,
                combo,
                lm_configs,
                embedder,
                judge_client,
                bench_root,
            )
            row = flatten_row(result)
            append_csv(csv_path, row)
            print(
                f"  wall={row['wall_time_s']:.1f}s "
                f"cost=${row['total_cost_usd']:.4f} "
                f"cos_mean={row['cosine_mean']} "
                f"trigram={row['trigram_repetition_rate']} "
                f"org={row['rubric_organization']}"
            )


def main() -> None:
    bench_root = Path("output_bench")
    csv_path = Path("evaluation/results.csv")

    openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
    judge_client = Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    embedder = make_openai_embedder(openai_client)
    lm_configs = setup_llms()

    cluster_groups = prepare_clusters(pipeline.INPUT_JSON_PATH, openai_client)
    print(f"prepared {len(cluster_groups)} multi-clusters")
    for i, (cluster_notices, fname) in enumerate(cluster_groups):
        print(f"  [{i}] {fname} (size={len(cluster_notices)})")

    combos = [
        HyperParams(turn=t, perspective=p, k=k)
        for t in (1, 2, 3)
        for p in (2, 3, 4)
        for k in (3, 5, 7)
    ]
    print(f"combos: {len(combos)}")

    run_sweep(
        cluster_groups=cluster_groups[:2],
        combos=combos,
        lm_configs=lm_configs,
        embedder=embedder,
        judge_client=judge_client,
        bench_root=bench_root,
        csv_path=csv_path,
    )


if __name__ == "__main__":
    main()
