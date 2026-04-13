from knowledge_storm.lm import AnthropicModel, OpenAIModel

from config import settings


def setup_llms():
    agent_lm = OpenAIModel(
        model=settings.AGENT_MODEL, api_key=settings.OPENAI_API_KEY, max_tokens=2000
    )

    if "claude" in settings.SYNTHESIS_MODEL.lower():
        synthesis_lm = AnthropicModel(
            model=settings.SYNTHESIS_MODEL,
            api_key=settings.ANTHROPIC_API_KEY,
            max_tokens=4000,
        )
    else:
        synthesis_lm = OpenAIModel(
            model=settings.SYNTHESIS_MODEL,
            api_key=settings.OPENAI_API_KEY,
            max_tokens=4000,
        )

    return agent_lm, synthesis_lm
