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
