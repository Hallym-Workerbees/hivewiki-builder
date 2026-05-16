import json
import logging
from pathlib import Path

import psycopg2

from config import pipeline, settings
from storm_engine.wiki_runner import clean_title

logger = logging.getLogger(__name__)


def fetch_notices_from_db(limit: int = 10) -> list[dict]:
    conn = None
    cursor = None
    notices: list[dict] = []

    try:
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )
        cursor = conn.cursor()

        query = "SELECT data FROM notices ORDER BY created_at DESC LIMIT %s"
        cursor.execute(query, (limit,))

        for row in cursor.fetchall():
            data = row[0]

            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.warning(f"JSON 파싱 에러 발생 데이터: {data[:50]}...")
                    continue

            notices.append(
                {
                    "title": clean_title(data.get("제목", "제목 없음")),
                    "department": data.get("부서", "부서 미상"),
                    "content": data.get("본문내용", "내용 없음"),
                    "date": data.get("작성일", ""),
                    "link": data.get("링크", ""),
                }
            )

        logger.info(f"[DB] {len(notices)}개 데이터 불러옴")
        return notices

    except Exception as e:
        logger.error(f"[DB 오류] 데이터 불러오는 중 문제 발생: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_notices_from_json(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    notices = [
        {
            "title": clean_title(item.get("제목", "제목 없음")),
            "department": item.get("부서", "학사지원팀"),
            "content": item.get("본문내용", ""),
            "date": item.get("작성일", ""),
            "link": item.get("링크", ""),
        }
        for item in raw
    ]

    logger.info(f"[JSON] {path.name}에서 {len(notices)}건 로드")
    return notices


def load_notices() -> list[dict]:
    if pipeline.DATA_SOURCE == "db":
        return fetch_notices_from_db(limit=pipeline.DB_FETCH_LIMIT)
    if pipeline.DATA_SOURCE == "json":
        return load_notices_from_json(pipeline.INPUT_JSON_PATH)
    raise ValueError(
        f"Unknown DATA_SOURCE: {pipeline.DATA_SOURCE!r} (expected 'json' or 'db')"
    )
