from knowledge_storm.lm import ClaudeModel, OpenAIModel
from knowledge_storm.storm_wiki.engine import STORMWikiLMConfigs

from config import settings


def setup_llms() -> STORMWikiLMConfigs:
    agent_lm = OpenAIModel(
        model=settings.AGENT_MODEL,
        api_key=settings.OPENAI_API_KEY,
        max_tokens=2000,
    )

    if "claude" in settings.SYNTHESIS_MODEL.lower():
        synthesis_lm = ClaudeModel(
            model=settings.SYNTHESIS_MODEL,
            api_key=settings.ANTHROPIC_API_KEY,
            max_tokens=4000,
            system=(
                "당신은 한국어 위키 문서 작성자입니다. "
                "지시사항이 영어로 작성되어 있더라도, "
                "모든 출력은 반드시 한국어로 작성하세요. "
                "마크다운 헤딩(#, ##, ###)과 인용 표기([1], [2] 등)는 그대로 사용하되 "
                "헤딩 제목과 본문은 모두 한국어여야 합니다."
            ),
        )
        synthesis_lm.kwargs.pop("top_p", None)
    else:
        synthesis_lm = OpenAIModel(
            model=settings.SYNTHESIS_MODEL,
            api_key=settings.OPENAI_API_KEY,
            max_tokens=4000,
        )

    lm_configs = STORMWikiLMConfigs()
    lm_configs.set_conv_simulator_lm(agent_lm)
    lm_configs.set_question_asker_lm(agent_lm)
    lm_configs.set_outline_gen_lm(agent_lm)
    lm_configs.set_article_gen_lm(synthesis_lm)
    lm_configs.set_article_polish_lm(synthesis_lm)

    return lm_configs
