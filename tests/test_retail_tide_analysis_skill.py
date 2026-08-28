from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest


def _load_skill_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "skills"
        / "retail-tide-analysis"
        / "scripts"
        / "retail_tide_query.py"
    )
    spec = importlib.util.spec_from_file_location("retail_tide_analysis_skill", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_skill_bundle_is_bounded_and_preserves_evidence(monkeypatch):
    skill = _load_skill_module()

    def fake_request(_base_url, path, params, _timeout):
        if path == "/topics/overview":
            return {
                "selected_date": "2026-08-30",
                "coverage": {
                    "collection_status": "complete",
                    "analysis_pending_count": 0,
                },
                "topics": [{"id": 6, "slug": "semiconductor", "history": []}],
            }
        if path == "/topics":
            return [{"id": 6, "slug": "semiconductor", "name": "半导体", "status": "active"}]
        if path == "/topics/6/contents":
            assert params["period"] == "custom"
            return {
                "total": 2,
                "facets": {"all": 2, "buy": 1},
                "source_facets": {"guba": 2},
                "items": [
                    {
                        "id": 1,
                        "source_name": "guba",
                        "source_item_id": "post-1",
                        "published_at": "2026-08-30T04:00:00+00:00",
                        "title": "芯片帖子",
                        "body": "很长的正文" * 100,
                        "analysis": {
                            "model": "gpt-5.6-sol-via-codex-cli",
                            "intent": "buy",
                        },
                    }
                ],
            }
        if path == "/trends/attention":
            return [
                {"topic_id": 6, "observed_at": "2026-08-29T16:00:00+00:00", "value": 9},
                {"topic_id": 7, "observed_at": "2026-08-29T16:00:00+00:00", "value": 8},
                {"topic_id": 6, "observed_at": "2026-08-20T16:00:00+00:00", "value": 7},
            ]
        if path == "/events":
            return [
                {"id": 10, "topic_id": 6, "started_at": "2026-08-30T01:00:00+00:00"},
                {"id": 11, "topic_id": 6, "started_at": "2026-08-20T01:00:00+00:00"},
            ]
        if path == "/sources/status":
            return [
                {
                    "name": "guba",
                    "enabled": True,
                    "health_status": "healthy",
                    "evidence": {"content_count": 2},
                    "quality": {},
                    "collector_version": "collector-v2",
                }
            ]
        raise AssertionError(path)

    monkeypatch.setattr(skill, "request_json", fake_request)
    args = argparse.Namespace(
        base_url="http://127.0.0.1:8000",
        timeout=10,
        from_date=date(2026, 8, 28),
        to_date=date(2026, 8, 30),
        topic="semiconductor",
        source="all",
        content_filter="all",
        post_limit=1,
        max_body_chars=120,
        attention_limit=100,
        event_limit=10,
    )

    result = skill.command_bundle(args)

    assert result["meta"]["timezone"] == "Asia/Shanghai"
    assert result["meta"]["topic"]["slug"] == "semiconductor"
    assert result["posts"]["total"] == 2
    assert result["posts"]["returned"] == 1
    assert result["posts"]["items"][0]["body_truncated"] is True
    assert result["posts"]["items"][0]["analysis"]["model"].endswith("codex-cli")
    assert len(result["attention"]) == 1
    assert [row["id"] for row in result["events"]] == [10]
    assert "post evidence is truncated to 1 of 2 items" in result["warnings"]


def test_skill_requires_complete_date_pair():
    skill = _load_skill_module()

    with pytest.raises(ValueError, match="must be provided together"):
        skill.range_values(
            "http://127.0.0.1:8000",
            date(2026, 8, 1),
            None,
            10,
        )
