import json
import re

from anthropic import Anthropic

from config import settings

JUDGE_MODEL = settings.SYNTHESIS_MODEL
JUDGE_MAX_TOKENS = 200

RUBRIC_DEFINITIONS: dict[str, str] = {
    "interest": (
        "Interest Level — Does the article engage the reader and pique their "
        "interest? Higher scores reflect engaging, vivid, compelling content."
    ),
    "relevance": (
        "Relevance and Focus — Does the article stay on the stated topic "
        "without drifting into unrelated material? Higher scores reflect "
        "tight topical focus."
    ),
    "broad_coverage": (
        "Broad Coverage — Does the article cover the topic from multiple "
        "angles and include all major aspects? Higher scores reflect "
        "comprehensive scope."
    ),
    "depth": (
        "Depth — Does the article provide detailed, substantive analysis "
        "rather than only surface-level facts? Higher scores reflect rich, "
        "thorough exploration of each subtopic."
    ),
    "organization": (
        "Organization — Is the article well-structured with logical section "
        "flow and clear hierarchy? Higher scores reflect coherent ordering "
        "and minimal redundancy across sections."
    ),
}

JUDGE_SYSTEM = (
    "You are an evaluator for Korean wiki articles. "
    "Score the article on five rubrics defined in the user message, "
    "using an integer scale from 1 (worst) to 5 (best). "
    "Respond ONLY with a single JSON object containing the five rubric keys "
    "and integer scores. No prose, no explanation, no markdown fences. "
    'Example: {"interest": 3, "relevance": 4, "broad_coverage": 3, '
    '"depth": 2, "organization": 4}'
)


def score_rubric(article_text: str, judge_client: Anthropic) -> dict[str, int]:
    rubric_block = "\n".join(
        f"- **{name}**: {definition}" for name, definition in RUBRIC_DEFINITIONS.items()
    )
    user_message = (
        "Rubrics (each scored 1-5, integer):\n"
        f"{rubric_block}\n\n"
        "Article (Korean, between fences):\n"
        "---\n"
        f"{article_text}\n"
        "---\n\n"
        "Respond with the JSON object only."
    )

    response = judge_client.messages.create(
        model=JUDGE_MODEL,
        max_tokens=JUDGE_MAX_TOKENS,
        system=JUDGE_SYSTEM,
        messages=[{"role": "user", "content": user_message}],
    )
    raw = response.content[0].text
    return _parse_rubric_json(raw)


def _parse_rubric_json(raw: str) -> dict[str, int]:
    match = re.search(r"\{.*?\}", raw, re.DOTALL)
    if match is None:
        raise ValueError(f"No JSON object found in judge response: {raw[:200]!r}")
    data = json.loads(match.group(0))
    expected = set(RUBRIC_DEFINITIONS.keys())
    missing = expected - data.keys()
    if missing:
        raise ValueError(f"Missing rubric keys in response: {missing}. Got: {data!r}")
    scores = {}
    for key in expected:
        value = int(data[key])
        if not 1 <= value <= 5:
            raise ValueError(f"Rubric '{key}' score out of range [1,5]: {value}")
        scores[key] = value
    return scores


def make_claude_judge(api_key: str) -> Anthropic:
    return Anthropic(api_key=api_key)
