import logging
from contextlib import contextmanager

import psycopg2

from config import settings

logger = logging.getLogger(__name__)

WIKI_STATUS_DONE = "DONE"
WIKI_STATUS_FAILED = "FAILED"


@contextmanager
def transaction():
    conn = psycopg2.connect(settings.DATABASE_DSN)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def insert_wiki(
    conn,
    *,
    title: str,
    slug: str,
    summary: str,
    content_markdown: str,
    generation_model: str,
) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO wiki_documents "
            "(id, title, slug, summary, status, created_at, updated_at) "
            "VALUES (gen_random_uuid(), %s, %s, %s, 'published', NOW(), NOW()) "
            "RETURNING id",
            (title, slug, summary),
        )
        wiki_document_id = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO wiki_revisions "
            "(id, wiki_document_id, revision_number, content_markdown, "
            "generation_type, generation_model, created_at) "
            "VALUES (gen_random_uuid(), %s, 1, %s, 'ai', %s, NOW()) "
            "RETURNING id",
            (wiki_document_id, content_markdown, generation_model),
        )
        revision_id = cur.fetchone()[0]

        cur.execute(
            "UPDATE wiki_documents SET current_revision_id = %s WHERE id = %s",
            (revision_id, wiki_document_id),
        )

    logger.info(f"[DB] wiki_document 생성 id={wiki_document_id}")
    return wiki_document_id


def mark_job_completed(conn, job_id: int, source_document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ingestion_jobs "
            "SET status = 'COMPLETED', completed_at = NOW(), error_message = NULL "
            "WHERE id = %s",
            (job_id,),
        )
        cur.execute(
            "UPDATE source_documents SET wiki_status = %s, body_text = NULL "
            "WHERE id = %s",
            (WIKI_STATUS_DONE, source_document_id),
        )
    logger.info(f"[DB] job {job_id} COMPLETED")


def mark_job_failed(
    conn, job_id: int, source_document_id: int, error_message: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ingestion_jobs "
            "SET status = 'FAILED', retry_count = retry_count + 1, "
            "error_message = %s WHERE id = %s",
            (error_message, job_id),
        )
        cur.execute(
            "UPDATE source_documents SET wiki_status = %s WHERE id = %s",
            (WIKI_STATUS_FAILED, source_document_id),
        )
    logger.warning(f"[DB] job {job_id} FAILED: {error_message}")
