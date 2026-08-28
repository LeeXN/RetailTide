from __future__ import annotations

import base64
from datetime import datetime, timedelta

from ..schemas import CollectResult, RawObservation
from ..time import as_utc, now_utc, shanghai_datetime
from .base import ProbeResult


def _offset(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception as exc:
        raise ValueError("invalid pagination cursor") from exc


def _cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode()


class FixtureSource:
    """Deterministic local source used for development and acceptance tests.

    It models the adapter boundary and can be replaced with a permitted remote
    transport later. It intentionally has stable IDs and cursor pagination.
    """

    source_type = "content"
    page_size = 12

    def __init__(self, name: str, *, clock=now_utc, days: int = 45):
        self.name = name
        self.clock = clock
        self.days = days

    def _records(
        self, query: str, since: datetime, until: datetime | None = None
    ) -> list[RawObservation]:
        now = as_utc(self.clock())
        assert now is not None
        since = as_utc(since) or (now - timedelta(days=self.days))
        until = as_utc(until) or now
        start = max(since.date(), (now - timedelta(days=self.days)).date())
        records: list[RawObservation] = []
        phrases = self._phrases()
        for day_offset in range(self.days + 1):
            day = start + timedelta(days=day_offset)
            for slot in range(3):
                published = shanghai_datetime(
                    day, 9 + (day_offset % 7), (day_offset * 11 + slot * 13) % 60
                )
                if published > until or published < since:
                    continue
                # A couple of deterministic crowd-surges make the local demo
                # exercise the discovery rule without hard-coding an event row.
                phrase_index = (
                    1 if day_offset % 19 in (15, 16) else (day_offset * 3 + slot) % len(phrases)
                )
                phrase = phrases[phrase_index]
                body = f"{query} {phrase}"
                item_id = f"{self.name}-{day.isoformat()}-{slot}"
                records.append(
                    RawObservation(
                        source=self.name,
                        source_item_id=item_id,
                        observation_kind="forum_post",
                        published_at=published,
                        observed_at=now,
                        payload={
                            "id": item_id,
                            "title": f"{query} 讨论 {day.isoformat()} #{slot}",
                            "body": body,
                            "author_id": f"{self.name}-author-{(day_offset * 3 + slot) % 23}",
                            "kind": "post" if (day_offset + slot) % 4 else "comment",
                            "url": f"https://example.invalid/{self.name}/{item_id}",
                            "likes": 5 + day_offset * 3 + slot,
                            "comments": (day_offset + slot) % 8,
                            "shares": (day_offset + slot) % 5,
                            "views": 100 + day_offset * 17 + slot,
                        },
                    )
                )
        return records

    def _phrases(self) -> list[str]:
        return [
            "新手第一次看黄金ETF，想问怎么买，风险是不是很大",
            "感觉大家都在冲，怕错过这一波，准备追涨，黄金会不会继续涨",
            "我已经持有黄金，先观察，按计划分批配置，不想一次梭哈",
            "价格突然回落有点慌，担心被套，大家别盲目跟风",
            "分享一个长期研究思路，欢迎理性讨论，注意仓位和风险",
            "黄金热度上来了，朋友群都在问，求一个入门解释",
        ]

    async def collect(
        self,
        query: str,
        since: datetime,
        cursor: str | None = None,
        *,
        until: datetime | None = None,
    ) -> CollectResult:
        records = self._records(query, since, until)
        offset = _offset(cursor)
        page = records[offset : offset + self.page_size]
        next_offset = offset + len(page)
        exhausted = next_offset >= len(records)
        return CollectResult(
            items=page,
            next_cursor=None if exhausted else _cursor(next_offset),
            exhausted=exhausted,
        )

    def probe(self) -> ProbeResult:
        return ProbeResult(
            source=self.name,
            source_type=self.source_type,
            checks={
                "stable_source_item_id": True,
                "published_at": True,
                "body": True,
                "author_id": True,
                "pagination": "cursor",
                "historical_depth_days": self.days,
                "failure_detection": True,
            },
            notes=[
                "Local deterministic fixture; configure a permitted transport for production collection."
            ],
        )
