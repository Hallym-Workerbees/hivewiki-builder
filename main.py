import logging
from datetime import datetime

from openai import OpenAI

from config import pipeline, settings
from data.dataloader import load_notices
from storm_engine.clusterer import (
    assign_cluster_filenames,
    cluster_by_similarity,
    embed_notices,
)
from storm_engine.llm_config import setup_llms
from storm_engine.wiki_runner import (
    run_storm_for_cluster,
    write_single_notice_md,
)

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("=== 한림대 학사공지 클러스터링 위키 파이프라인 ===")
    logger.info(f"[설정] DATA_SOURCE={pipeline.DATA_SOURCE}")

    pipeline.FINAL_WIKI_DIR.mkdir(parents=True, exist_ok=True)
    pipeline.STORM_WORK_DIR.mkdir(parents=True, exist_ok=True)

    notices = load_notices()
    if not notices:
        logger.error("[에러] 불러올 수 있는 데이터 없음. 파이프라인 종료.")
        return

    client = OpenAI(api_key=settings.OPENAI_API_KEY)

    embeddings = embed_notices(notices, client)
    clusters = cluster_by_similarity(embeddings, pipeline.CLUSTER_THRESHOLD)
    filenames = assign_cluster_filenames(clusters, notices, client)

    lm_configs = setup_llms()

    results: list[tuple[int, str, str, int]] = []
    for cid, (indices, filename) in enumerate(zip(clusters, filenames, strict=True)):
        final_path = pipeline.FINAL_WIKI_DIR / f"{filename}.md"
        cluster_notices = [notices[i] for i in indices]

        if len(indices) == 1:
            logger.info(f"[Cluster {cid:02d}] 단독 → {filename}.md")
            write_single_notice_md(cluster_notices[0], final_path)
            results.append((cid, filename, "single", final_path.stat().st_size))
        else:
            logger.info(
                f"[Cluster {cid:02d}] 다중(size={len(indices)}) → STORM ({filename})"
            )
            attributed = run_storm_for_cluster(
                cluster_notices, filename, lm_configs, pipeline.STORM_WORK_DIR
            )
            header = (
                f"# {filename.replace('_', ' ')}\n\n"
                f"> **생성일:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  \n"
                f"> **포함 공지 수:** {len(indices)}건  \n"
                f"> **출처:** 한림대학교 학사지원팀  \n\n---\n\n"
            )
            final_path.write_text(header + attributed, encoding="utf-8")
            results.append((cid, filename, "storm", final_path.stat().st_size))

    logger.info(f"[완료] {len(results)}개 파일 → {pipeline.FINAL_WIKI_DIR}")


if __name__ == "__main__":
    main()
