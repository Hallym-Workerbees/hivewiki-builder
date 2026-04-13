from data.dataloader import fetch_notices_from_db
from storm_engine.wiki_runner import run_storm_pipeline


def main():
    print("=== 한림대 학사공지 위키 파이프라인 ===")

    notices = fetch_notices_from_db(limit=10)

    if not notices:
        print("[에러] 불러올 수 있는 데이터 없음. 파이프라인 종료.")
        return

    print(f"[성공] 총 {len(notices)}건의 공지 데이터 확인")

    wiki_topic = "최근_학사_공지사항_종합"  # e.g.

    run_storm_pipeline(topic=wiki_topic, db_notices=notices, output_dir="output")


if __name__ == "__main__":
    main()
