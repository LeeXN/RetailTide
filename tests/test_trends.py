from datetime import date, datetime, timedelta

from sqlalchemy import select

from retail_tide.api.overview import topic_overview
from retail_tide.models import Source, Topic, TrendSignal
from retail_tide.pipeline.normalize import (
    insert_raw_observation,
    link_raw_observation_topic,
    normalize_raw_observation,
)
from retail_tide.pipeline.trends import aggregate_trend_signals, trend_snapshot
from retail_tide.schemas import RawObservation
from retail_tide.time import UTC


def test_wikimedia_attention_is_stored_as_independent_trend_signal(session, settings):
    source = session.scalar(
        select(Source).where(Source.name == "wikimedia-pageviews")
    )
    topic = session.scalar(select(Topic).where(Topic.slug == "gold"))
    start = datetime(2026, 7, 1, tzinfo=UTC)
    for index in range(16):
        bucket = start + timedelta(days=index)
        for keyword, value in (("黄金", 100 + index * 10), ("金", 50 + index)):
            raw_schema = RawObservation(
                source="wikimedia-pageviews",
                source_item_id=f"{keyword}:{bucket.date().isoformat()}",
                observation_kind="pageviews",
                published_at=bucket,
                observed_at=bucket + timedelta(hours=12),
                payload={
                    "keyword": keyword,
                    "project": "zh.wikipedia.org",
                    "article": keyword,
                    "date": bucket.date().isoformat(),
                    "value": value,
                    "unit": "views",
                },
            )
            raw, _ = insert_raw_observation(session, source.id, raw_schema)
            link_raw_observation_topic(
                session, raw, topic_id=topic.id, collection_query=keyword
            )
            normalize_raw_observation(session, raw)
    session.commit()

    assert aggregate_trend_signals(session, settings=settings) == 32
    signals = session.scalars(select(TrendSignal)).all()
    assert len(signals) == 32
    assert signals[-1].percentile is not None
    rows = trend_snapshot(session, topic_id=topic.id)
    assert len(rows) == 32
    assert rows[0]["source"] == "wikimedia-pageviews"
    assert rows[0]["unit"] == "views"
    assert len(trend_snapshot(session, all_topics=True)) == 32
    overview = topic_overview(session, selected_date=date(2026, 7, 16))
    gold = next(row for row in overview["topics"] if row["slug"] == "gold")
    assert len(gold["wikimedia_history"]) == 16
    assert gold["wikimedia_history"][-1]["value"] == 315
    assert {
        item["keyword"] for item in gold["wikimedia_history"][-1]["keywords"]
    } == {"黄金", "金"}
    assert overview["market"]["wikimedia_history"][-1]["topic_count"] == 1
    assert overview["market"]["wikimedia_history"][-1]["keyword_count"] == 2
