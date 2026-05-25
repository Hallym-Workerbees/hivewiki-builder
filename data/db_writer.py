import logging
import time
from contextlib import contextmanager

import psycopg2

from config import settings

logger = logging.getLogger(__name__)

WIKI_STATUS_DONE = "DONE"
WIKI_STATUS_FAILED = "FAILED"


@contextmanager
def transaction():
    started = time.perf_counter()
    conn = psycopg2.connect(settings.DATABASE_DSN)
    try:
        with conn:
            yield conn
    finally:
        conn.close()
        logger.debug(
            "[DB] transaction_closed elapsed=%.2fs",
            time.perf_counter() - started,
        )


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

    logger.info(
        "[DB] action=insert_wiki wiki_document=%s slug=%s revision=%s",
        wiki_document_id,
        slug,
        revision_id,
    )
    return wiki_document_id


def mark_job_started(conn, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ingestion_jobs SET started_at = NOW() WHERE id = %s",
            (job_id,),
        )
        rowcount = cur.rowcount
    if rowcount == 0:
        logger.warning("[WARN] action=mark_job_started job=%s rows=0", job_id)
    else:
        logger.info("[DB] action=mark_job_started job=%s rows=%s", job_id, rowcount)


def slug_exists(conn, slug: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM wiki_documents WHERE slug = %s LIMIT 1", (slug,))
        return cur.fetchone() is not None


def make_unique_slug(conn, base: str) -> str:
    slug = base
    suffix = 1
    while slug_exists(conn, slug):
        suffix += 1
        slug = f"{base}-{suffix}"
    return slug


def insert_chunk_with_embedding(
    conn,
    *,
    source_document_id: int,
    content_text: str,
    embedding: list[float],
) -> str:
    embedding_dim = len(embedding)
    embedding_literal = "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO source_chunks "
            "(id, source_document_id, content_text, chunk_index, created_at) "
            "VALUES (gen_random_uuid(), %s, %s, 0, NOW()) "
            "RETURNING id",
            (source_document_id, content_text),
        )
        chunk_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO chunk_embeddings "
            "(id, source_chunk_id, embedding, embedding_model, "
            "embedding_dim, created_at) "
            "VALUES (gen_random_uuid(), %s, %s::vector, %s, %s, NOW())",
            (chunk_id, embedding_literal, settings.EMBEDDING_MODEL, embedding_dim),
        )
    logger.info(
        "[DB] action=insert_chunk_embedding source_document=%s chunk=%s dimensions=%s",
        source_document_id,
        chunk_id,
        embedding_dim,
    )
    return chunk_id


def mark_job_completed(conn, job_id: int, source_document_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ingestion_jobs "
            "SET status = 'COMPLETED', completed_at = NOW(), error_message = NULL "
            "WHERE id = %s",
            (job_id,),
        )
        job_rows = cur.rowcount
        cur.execute(
            "UPDATE source_documents SET wiki_status = %s, body_text = NULL "
            "WHERE id = %s",
            (WIKI_STATUS_DONE, source_document_id),
        )
        source_rows = cur.rowcount
    logger.info(
        "[DB] action=mark_job_completed job=%s source_document=%s job_rows=%s "
        "source_rows=%s",
        job_id,
        source_document_id,
        job_rows,
        source_rows,
    )


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
    logger.warning(
        "[DB] action=mark_job_failed job=%s source_document=%s error=%r",
        job_id,
        source_document_id,
        error_message[:500],
    )
