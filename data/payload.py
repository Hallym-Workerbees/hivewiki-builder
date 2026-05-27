import json
from dataclasses import dataclass


@dataclass
class Job:
    id: int
    source_document_id: int
    status: str
    retry_count: int
    queued_at: str


@dataclass
class Source:
    id: int
    name: str


@dataclass
class Document:
    source_id: int
    canonical_url: str
    title: str
    body_text: str
    published_at: str | None


@dataclass
class JobPayload:
    job: Job
    source: Source
    document: Document


def parse_payload(raw: str) -> JobPayload:
    data = json.loads(raw)
    job = data["job"]
    source = data["source"]
    document = data["document"]

    return JobPayload(
        job=Job(
            id=job["id"],
            source_document_id=job["source_document_id"],
            status=job.get("status", ""),
            retry_count=job.get("retry_count", 0),
            queued_at=job.get("queued_at", ""),
        ),
        source=Source(
            id=source["id"],
            name=source.get("name", ""),
        ),
        document=Document(
            source_id=document["source_id"],
            canonical_url=document["canonical_url"],
            title=document["title"],
            body_text=document["body_text"],
            published_at=document.get("published_at"),
        ),
    )
