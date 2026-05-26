import anthropic

from config import settings

SYSTEM_PROMPT = """당신은 한국 대학교 학사 공지를 위한 위키 페이지 outline 작성자입니다.

[원칙]
1. 본문 충실성 — 공지 본문에 실제로 담긴 정보 영역만 섹션으로 만든다.
   본문에 없는 주제(취업 전망, 진로 영향, 산업 동향, 기술 사양,
   미래 발전 방향, 도전 과제 일반론, 감정적 고려사항 등)는 절대 섹션으로 만들지 않는다.
2. 도메인 맥락 — 이 문서는 행정 공지문이다. 일반 백과사전 article의
   표준 템플릿(직무 설명·진로 개발·취업 기회·도전 과제 등)을 적용하지 않는다.
3. 섹션 수 적정성 — 본문 정보량에 비례. 짧은 공지는 3~4개 섹션으로 충분.

[형식]
- 모든 섹션 제목은 한국어
- "# 제목"으로 섹션, 필요 시 "## 제목"으로 부섹션
- 첫 줄에 토픽 제목을 포함하지 않는다
- outline만 출력 (설명·본문 금지)"""


def _format_notices(notices: list[dict]) -> str:
    parts = []
    for i, n in enumerate(notices, 1):
        parts.append(
            f"[공지 {i}]\n"
            f"제목: {n['title']}\n"
            f"부서: {n.get('department', '')}\n"
            f"본문:\n{n.get('content', '')}"
        )
    return "\n\n---\n\n".join(parts)


def generate_outline_from_notices(notices: list[dict]) -> str:
    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    user_msg = f"공지 본문:\n\n{_format_notices(notices)}\n\noutline을 작성하라."
    resp = client.messages.create(
        model=settings.SYNTHESIS_MODEL,
        max_tokens=600,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    return resp.content[0].text.strip()
