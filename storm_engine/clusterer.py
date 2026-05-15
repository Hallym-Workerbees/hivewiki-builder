import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
from numpy.linalg import norm

from config import pipeline, settings

FILENAME_SYSTEM = (
    "You generate concise Korean filenames for academic notice clusters. "
    "Output ONLY the filename, no extension, no path, no quotes, no explanation."
)

FILENAME_USER_TMPL = """다음 한림대학교 공지 묶음을 대표하는 한국어 파일명을 만드세요.

규칙:
- 30자 이하
- 단어 사이는 언더스코어(_)
- 한글, 숫자, 언더스코어만 사용 (영문/특수문자/공백 금지)
- 파일 확장자 포함 금지
- 핵심 주제만 압축

공지 제목 목록:
{titles}

파일명:"""


class UnionFind:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def embed_notices(notices: list[dict], client) -> np.ndarray:
    texts = [
        f"{n['title']}\n{n['content'][: pipeline.EMBED_CONTENT_CHARS]}" for n in notices
    ]
    print(f"[임베딩 요청] {len(texts)}건 일괄...")
    resp = client.embeddings.create(model=settings.EMBEDDING_MODEL, input=texts)
    embeddings = np.array([d.embedding for d in resp.data])
    print(f"[임베딩 완료] shape={embeddings.shape}, tokens={resp.usage.total_tokens:,}")
    return embeddings


def cluster_by_similarity(
    embeddings: np.ndarray, threshold: float = pipeline.CLUSTER_THRESHOLD
) -> list[list[int]]:
    e = embeddings / norm(embeddings, axis=1, keepdims=True)
    sim = e @ e.T

    n = embeddings.shape[0]
    uf = UnionFind(n)
    for i in range(n):
        for j in range(i + 1, n):
            if sim[i, j] >= threshold:
                uf.union(i, j)

    groups: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        groups[uf.find(i)].append(i)
    cluster_list = sorted(groups.values(), key=lambda c: (-len(c), c[0]))

    print(f"[클러스터링 완료] threshold={threshold}, 총 그룹 수: {len(cluster_list)}")
    size_counts: dict[int, int] = defaultdict(int)
    for c in cluster_list:
        size_counts[len(c)] += 1
    for size in sorted(size_counts.keys(), reverse=True):
        print(f"  크기 {size:>2}: {size_counts[size]}개")
    return cluster_list


def _sanitize_filename(name: str) -> str:
    name = name.strip().strip('"').strip("'")
    if name.endswith(".md"):
        name = name[:-3]
    name = re.sub(r"[^\w가-힣]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"_+", "_", name).strip("_")
    return name[: pipeline.FILENAME_MAX_LEN] or "공지"


def _generate_cluster_filename(titles: list[str], client) -> str:
    titles_str = "\n".join(f"- {t}" for t in titles)
    resp = client.with_options(
        timeout=pipeline.FILENAME_LLM_TIMEOUT
    ).chat.completions.create(
        model=settings.AGENT_MODEL,
        messages=[
            {"role": "system", "content": FILENAME_SYSTEM},
            {"role": "user", "content": FILENAME_USER_TMPL.format(titles=titles_str)},
        ],
        max_tokens=50,
        temperature=pipeline.FILENAME_LLM_TEMPERATURE,
    )
    return _sanitize_filename(resp.choices[0].message.content)


def assign_cluster_filenames(
    clusters: list[list[int]], notices: list[dict], client
) -> list[str]:
    multi_indices = [cid for cid, c in enumerate(clusters) if len(c) >= 2]
    print(
        f"[파일명 LLM] 다중 클러스터 {len(multi_indices)}개 → "
        f"max_workers={pipeline.FILENAME_LLM_MAX_WORKERS}"
    )

    llm_names: dict[int, str] = {}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=pipeline.FILENAME_LLM_MAX_WORKERS) as ex:
        future_to_cid = {
            ex.submit(
                _generate_cluster_filename,
                [notices[i]["title"] for i in clusters[cid]],
                client,
            ): cid
            for cid in multi_indices
        }
        for fut in as_completed(future_to_cid):
            cid = future_to_cid[fut]
            try:
                name = fut.result()
                llm_names[cid] = name
                print(f"  [{time.time() - t0:5.1f}s] Cluster {cid:02d} → {name}")
            except Exception as e:
                print(f"  [실패] Cluster {cid:02d}: {e}")
                llm_names[cid] = _sanitize_filename(notices[clusters[cid][0]]["title"])
    print(f"[파일명 LLM 완료] {time.time() - t0:.1f}초")

    used_names: set[str] = set()
    filenames: list[str] = []
    for cid, indices in enumerate(clusters):
        base = (
            _sanitize_filename(notices[indices[0]]["title"])
            if len(indices) == 1
            else llm_names[cid]
        )
        name = base
        counter = 2
        while name in used_names:
            suffix = f"_{counter}"
            name = f"{base[: pipeline.FILENAME_MAX_LEN - len(suffix)]}{suffix}"
            counter += 1
        used_names.add(name)
        filenames.append(name)

    return filenames
