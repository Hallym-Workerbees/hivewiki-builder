import json

import psycopg2

from config import settings


def fetch_notices_from_db(limit=10):
    conn = None
    notices = []

    try:
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )
        cursor = conn.cursor()

        query = ""
        cursor.execute(query)

        for row in cursor.fetchall():
            data = row[0]

            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    print(f"JSON 파싱 에러 발생 데이터: {data[:50]}...")
                    continue

            notices.append(
                {
                    "title": data.get("제목", "제목 없음"),
                    "department": data.get("부서", "부서 미상"),
                    "content": data.get("본문내용", "내용 없음"),
                }
            )

        print(f"[DB] {len(notices)}개 데이터 불러옴")
        return notices

    except Exception as e:
        print(f"[DB 오류] 데이터 불러오는 중 문제 발생: {e}")
        return []

    finally:
        if conn:
            cursor.close()
            conn.close()
