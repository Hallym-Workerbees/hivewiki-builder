import json
import logging
import re
from dataclasses import dataclass, field

from openai import OpenAI

from config import settings

logger = logging.getLogger(__name__)

VALIDATOR_MODEL = settings.AGENT_MODEL
VALIDATOR_MAX_TOKENS = 800
VALIDATOR_TEMPERATURE = 0.0
SOURCE_MAX_CHARS_PER_DOC = 3000
MAX_SOURCES = 8
WIKI_MAX_CHARS = 8000

NUMERIC_ACCURACY_PROMPT = """다음은 원본 공지(들)과 그로부터 생성된 위키입니다.
위키에 등장하는 다음 항목들을 모든 원본 공지와 대조하세요:
- 날짜, 시간, 기간
- 전화번호, 이메일, 팩스, URL
- 호실, 건물명, 부서명
- 인명, 학번, 직책
- 인원수, 점수, 학년 범위

원본 공지들 중 어디에도 없거나 모든 원본과 다른 값으로 등장하는 항목만 추출하세요.
하나의 원본에라도 일치하는 값이 있으면 출력하지 마세요.

{sources}

[위키 본문]
{wiki}

출력 규칙:
- JSON 배열만 출력 (코드블록·설명·머리말 금지)
- 문제 없으면 [] 만 출력
- 형식:
[
  {{
    "entity": "항목명",
    "wiki_says": "위키 값",
    "source_says": "원본들이 말하는 값 또는 '없음'"
  }}
]"""


@dataclass
class ValidationIssue:
    category: str
    description: str
    evidence: str | None = None


@dataclass
class ValidationResult:
    passed: bool
    issues: list[ValidationIssue] = field(default_factory=list)


def _check_citations(wiki_markdown: str) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    body_refs = set(re.findall(r"\[\^(\d+)\](?!:)", wiki_markdown))
    defined_refs = set(re.findall(r"^\[\^(\d+)\]:", wiki_markdown, flags=re.MULTILINE))

    for ref in sorted(body_refs - defined_refs, key=int):
        issues.append(
            ValidationIssue(
                category="citation_missing_definition",
                description=f"본문에서 [^{ref}] 참조하지만 # 참고 문헌에 정의 없음",
                evidence=f"[^{ref}]",
            )
        )
    for ref in sorted(defined_refs - body_refs, key=int):
        issues.append(
            ValidationIssue(
                category="citation_unused_definition",
                description=f"[^{ref}] 정의가 있지만 본문에서 참조 안 함",
                evidence=f"[^{ref}]:",
            )
        )
    return issues


def _check_empty_sections(wiki_markdown: str) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    lines = wiki_markdown.splitlines()
    for idx, line in enumerate(lines):
        stripped = line.strip()
        is_heading = stripped.startswith("# ") or stripped.startswith("## ")
        if not is_heading:
            continue
        has_content = False
        for next_line in lines[idx + 1 :]:
            next_stripped = next_line.strip()
            if next_stripped.startswith("# ") or next_stripped.startswith("## "):
                break
            if next_stripped:
                has_content = True
                break
        if not has_content:
            issues.append(
                ValidationIssue(
                    category="empty_section",
                    description=f"섹션 '{stripped}'에 본문 없음",
                    evidence=stripped,
                )
            )
    return issues


def _format_sources(source_texts: list[str]) -> str:
    parts = []
    for i, text in enumerate(source_texts[:MAX_SOURCES], 1):
        parts.append(f"[원본 공지 {i}]\n{text[:SOURCE_MAX_CHARS_PER_DOC]}")
    return "\n\n".join(parts)


def _check_numeric_accuracy(
    wiki_markdown: str, source_texts: list[str], openai_client: OpenAI
) -> list[ValidationIssue]:
    prompt = NUMERIC_ACCURACY_PROMPT.format(
        sources=_format_sources(source_texts),
        wiki=wiki_markdown[:WIKI_MAX_CHARS],
    )
    try:
        resp = openai_client.chat.completions.create(
            model=VALIDATOR_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=VALIDATOR_MAX_TOKENS,
            temperature=VALIDATOR_TEMPERATURE,
        )
        raw = resp.choices[0].message.content.strip()
    except Exception:
        logger.exception("[VALIDATOR] llm_call_failed")
        return []

    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("[VALIDATOR] json_parse_failed raw_prefix=%r", raw[:200])
        return []

    if not isinstance(parsed, list):
        return []

    issues: list[ValidationIssue] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("entity", ""))
        wiki_says = str(item.get("wiki_says", ""))
        source_says = str(item.get("source_says", ""))
        issues.append(
            ValidationIssue(
                category="fabricated_entity",
                description=(f"{entity}: 위키='{wiki_says}' / 원본='{source_says}'"),
                evidence=wiki_says,
            )
        )
    return issues


def validate(
    wiki_markdown: str,
    source_texts: list[str],
    openai_client: OpenAI,
) -> ValidationResult:
    issues: list[ValidationIssue] = []
    issues.extend(_check_citations(wiki_markdown))
    issues.extend(_check_empty_sections(wiki_markdown))
    issues.extend(_check_numeric_accuracy(wiki_markdown, source_texts, openai_client))

    by_category: dict[str, int] = {}
    for issue in issues:
        by_category[issue.category] = by_category.get(issue.category, 0) + 1
    logger.info(
        "[VALIDATOR] passed=%s total=%s breakdown=%s sources=%s",
        not issues,
        len(issues),
        by_category,
        len(source_texts),
    )
    return ValidationResult(passed=not issues, issues=issues)


def format_issues_for_prompt(issues: list[ValidationIssue]) -> str:
    lines = []
    for i, issue in enumerate(issues, 1):
        lines.append(f"{i}. [{issue.category}] {issue.description}")
    return "\n".join(lines)
