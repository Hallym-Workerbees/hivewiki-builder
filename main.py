import sys

DEPRECATION_MESSAGE = (
    "[deprecated] main.py는 더 이상 진입점이 아닙니다.\n"
    "builder는 Redis 큐 컨슈머로 교체되었습니다.\n"
    "실행: uv run python consumer.py\n"
)


if __name__ == "__main__":
    sys.stderr.write(DEPRECATION_MESSAGE)
    sys.exit(1)
