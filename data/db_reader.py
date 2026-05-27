import logging
from dataclasses import dataclass

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class NeighborChunk:
    chunk_id: str
    source_document_id: int
    title: str
    content_text: str
    canonical_url: str
    similarity: float


@dataclass
class RelatedWiki:
    wiki_document_id: str
    title: str
    slug: str
    similarity: float


def _format_vector(embedding: list[float]) -> str:
    return "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"


def find_similar_chunks(
    conn,
    *,
    embedding: list[float],
    k: int,
    similarity_threshold: float,
    exclude_source_document_id: int | None = None,
) -> list[NeighborChunk]:
    vector = _format_vector(embedding)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sc.id, sc.source_document_id, sd.title, sc.content_text, "
            "sd.canonical_url, 1 - (ce.embedding <=> %s::vector) AS similarity "
            "FROM chunk_embeddings ce "
            "JOIN source_chunks sc ON ce.source_chunk_id = sc.id "
            "JOIN source_documents sd ON sc.source_document_id = sd.id "
            "WHERE ce.embedding_model = %s "
            "AND (%s::bigint IS NULL OR sc.source_document_id <> %s::bigint) "
            "ORDER BY ce.embedding <=> %s::vector "
            "LIMIT %s",
            (
                vector,
                settings.EMBEDDING_MODEL,
                exclude_source_document_id,
                exclude_source_document_id,
                vector,
                k,
            ),
        )
        rows = cur.fetchall()
    neighbors = [
        NeighborChunk(
            chunk_id=str(r[0]),
            source_document_id=r[1],
            title=r[2],
            content_text=r[3],
            canonical_url=r[4],
            similarity=float(r[5]),
        )
        for r in rows
        if float(r[5]) >= similarity_threshold
    ]
    logger.info(
        "[DB] action=find_similar_chunks k=%s threshold=%.2f scanned=%s matched=%s",
        k,
        similarity_threshold,
        len(rows),
        len(neighbors),
    )
    return neighbors


def find_target_wiki_for_chunks(conn, chunk_ids: list[str]) -> str | None:
    if not chunk_ids:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wr.wiki_document_id, COUNT(DISTINCT wrs.source_chunk_id) AS cnt "
            "FROM wiki_revision_sources wrs "
            "JOIN wiki_revisions wr ON wrs.wiki_revision_id = wr.id "
            "JOIN wiki_documents wd ON wr.wiki_document_id = wd.id "
            "WHERE wrs.source_chunk_id = ANY(%s::uuid[]) "
            "AND wd.status = 'published' "
            "GROUP BY wr.wiki_document_id "
            "ORDER BY cnt DESC, wr.wiki_document_id ASC "
            "LIMIT 1",
            (chunk_ids,),
        )
        row = cur.fetchone()
    wiki_id = str(row[0]) if row else None
    logger.info(
        "[DB] action=find_target_wiki_for_chunks chunks=%s matched_wiki=%s",
        len(chunk_ids),
        wiki_id,
    )
    return wiki_id


def find_related_wikis(
    conn,
    *,
    embedding: list[float],
    k: int,
    min_similarity: float,
    max_similarity: float,
    exclude_wiki_ids: list[str],
) -> list[RelatedWiki]:
    vector = _format_vector(embedding)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT wd.id, wd.title, wd.slug, "
            "1 - (wde.embedding <=> %s::vector) AS similarity "
            "FROM wiki_document_embeddings wde "
            "JOIN wiki_documents wd ON wde.wiki_document_id = wd.id "
            "WHERE wde.embedding_model = %s "
            "AND wd.status = 'published' "
            "AND wd.id <> ALL(%s::uuid[]) "
            "AND 1 - (wde.embedding <=> %s::vector) BETWEEN %s AND %s "
            "ORDER BY wde.embedding <=> %s::vector "
            "LIMIT %s",
            (
                vector,
                settings.EMBEDDING_MODEL,
                exclude_wiki_ids,
                vector,
                min_similarity,
                max_similarity,
                vector,
                k,
            ),
        )
        rows = cur.fetchall()
    related = [
        RelatedWiki(
            wiki_document_id=str(r[0]),
            title=r[1],
            slug=r[2],
            similarity=float(r[3]),
        )
        for r in rows
    ]
    logger.info(
        "[DB] action=find_related_wikis k=%s band=[%.2f,%.2f] matched=%s",
        k,
        min_similarity,
        max_similarity,
        len(related),
    )
    return related
