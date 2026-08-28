from __future__ import annotations

import hashlib
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Content, ContentCluster, ContentClusterMember
from ..time import now_utc


def simhash(text: str, *, bits: int = 64) -> int:
    """Return a deterministic SimHash without a vector database."""
    normalized = (text or "").casefold()
    tokens = re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]", normalized)
    shingles = tokens or [normalized]
    scores = [0] * bits
    for token in shingles:
        digest = hashlib.blake2b(token.encode(), digest_size=bits // 8).digest()
        value = int.from_bytes(digest, "big")
        for bit in range(bits):
            scores[bit] += 1 if (value >> bit) & 1 else -1
    result = 0
    for bit, score in enumerate(scores):
        if score >= 0:
            result |= 1 << bit
    return result


def hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def cluster_contents(session: Session, *, limit: int | None = None, max_distance: int = 3) -> int:
    contents = session.scalars(select(Content).order_by(Content.id).limit(limit)).all()
    representatives: list[tuple[ContentCluster, int]] = [
        (cluster, int(cluster.fingerprint, 16))
        for cluster in session.scalars(select(ContentCluster)).all()
    ]
    for content in contents:
        fingerprint = simhash(f"{content.title or ''} {content.body}")
        membership = session.scalar(
            select(ContentClusterMember).where(ContentClusterMember.content_id == content.id)
        )
        if membership is not None:
            continue
        chosen = None
        distance = 0
        for cluster, representative in representatives:
            candidate_distance = hamming_distance(fingerprint, representative)
            if candidate_distance <= max_distance:
                chosen, distance = cluster, candidate_distance
                break
        if chosen is None:
            chosen = ContentCluster(fingerprint=f"{fingerprint:016x}", created_at=now_utc())
            session.add(chosen)
            session.flush()
            representatives.append((chosen, fingerprint))
        session.add(
            ContentClusterMember(
                content_id=content.id,
                cluster_id=chosen.id,
                hamming_distance=distance,
                created_at=now_utc(),
            )
        )
    session.flush()
    return len(contents)
