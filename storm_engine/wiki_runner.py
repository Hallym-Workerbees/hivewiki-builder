from knowledge_storm import STORMWikiRunner, STORMWikiRunnerArguments
from knowledge_storm.rm import RM

from .llm_config import setup_llms


class DBNoticeRetriever(RM):
    def __init__(self, db_notices):
        super().__init__()
        self.db_notices = db_notices

    def search(self, query: str, top_k: int = 5):
        results = []
        for notice in self.db_notices[:top_k]:
            results.append(
                {
                    "url": f"hallym-notice-{notice['title']}",
                    "title": notice["title"],
                    "snippets": [
                        f"부서: {notice['department']}\n내용: {notice['content']}"
                    ],
                }
            )
        return results


def run_storm_pipeline(topic: str, db_notices: list, output_dir: str = "output"):
    print(f"\n[STORM] '{topic}' 주제로 위키 문서 생성 시작..")

    agent_lm, synthesis_lm = setup_llms()
    rm = DBNoticeRetriever(db_notices)

    engine_args = STORMWikiRunnerArguments(
        output_dir=output_dir, max_conv_turn=3, max_perspective=3
    )

    runner = STORMWikiRunner(engine_args, agent_lm, synthesis_lm, rm)

    runner.run(
        topic=topic,
        do_research=True,
        do_generate_outline=True,
        do_generate_article=True,
        do_polish_article=True,
    )

    print(f"\n[STORM 완료] '{output_dir}/{topic}' 폴더에 문서 생성 완료")
