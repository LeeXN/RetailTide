from __future__ import annotations

import hashlib
import inspect
import json
import logging
from collections import Counter
from datetime import datetime, timedelta
from threading import Lock
from time import monotonic
from typing import Any, Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import LLMProviderConfig, Settings, get_settings, llm_config_status
from ..models import (
    AnalysisTask,
    Content,
    ContentAnalysis,
    ContentAnalysisReview,
    ContentEntity,
    Topic,
)
from ..observability import increment, set_gauge
from ..schemas import AnalysisContract, EmotionSignals
from ..time import as_utc, now_utc

_LLM_RATE_LOCK = Lock()
_LLM_NEXT_REQUEST_AT = 0.0
logger = logging.getLogger(__name__)


class AnalysisProvider(Protocol):
    model: str

    def analyze(
        self, *, title: str | None, body: str, topic: Topic | None = None
    ) -> AnalysisContract | dict[str, Any]: ...


def _contains(text: str, *terms: str) -> bool:
    return any(term.casefold() in text.casefold() for term in terms)


class RuleBasedAnalysisProvider:
    """Transparent local provider used by V0 and as a safe fallback.

    It produces the same strict JSON contract as an LLM provider, so re-running
    with a real provider only changes model/version rows rather than the schema.
    """

    model = "rule-based-v0"

    def analyze(
        self, *, title: str | None, body: str, topic: Topic | None = None
    ) -> AnalysisContract:
        text = f"{title or ''} {body}".strip()
        commercial = _contains(
            text,
            "广告",
            "推广",
            "宣传",
            "开户链接",
            "课程",
            "营销",
            "扫码",
            "加微信",
            "加v",
            "私信领取",
            "免费领取",
            "会员群",
        )
        media = _contains(text, "快讯", "媒体", "报道", "财联社", "新华社")
        kol = _contains(text, "老师", "大V", "跟我", "直播", "粉丝")
        spam = _contains(text, "微信群", "内幕消息", "机器人账号", "自动发帖", "刷屏", "批量注册")
        actor = "commercial" if commercial else "media" if media else "kol" if kol else "retail"

        novice_signals: list[str] = []
        novice_terms = {
            "第一次": "first_time",
            "新手": "first_time",
            "怎么买": "how_to_buy",
            "如何买": "how_to_buy",
            "入门": "basic_question",
            "想问": "basic_question",
            "求带": "influencer_following",
            "上车": "seeking_entry",
            "追涨": "chasing_price",
            "梭哈": "all_in_language",
            "满仓": "all_in_language",
            "风险": "lack_risk_awareness",
            "不懂": "product_confusion",
            "被套": "position_uncertainty",
        }
        for term, signal in novice_terms.items():
            if term in text and signal not in novice_signals:
                novice_signals.append(signal)
        investor = (
            "novice"
            if novice_signals
            else "experienced"
            if _contains(text, "仓位", "纪律", "研究")
            else "unknown"
        )

        bullish = _contains(text, "上涨", "看涨", "继续涨", "冲", "上车", "涨停", "bullish")
        bearish = _contains(text, "下跌", "看跌", "回落", "恐慌", "被套", "崩", "bearish")
        direction = (
            "bullish"
            if bullish and not bearish
            else "bearish"
            if bearish and not bullish
            else "neutral"
        )
        intent = (
            "buy"
            if _contains(text, "买", "上车", "冲", "满仓", "梭哈")
            else "sell"
            if _contains(text, "卖", "止损", "减仓")
            else "hold"
            if _contains(text, "持有", "底仓")
            else "unknown"
        )
        position = (
            "owned"
            if _contains(text, "持有", "底仓", "仓位")
            else "not_owned"
            if novice_signals
            else "unknown"
        )

        emotion = EmotionSignals(
            urgency=_contains(text, "现在", "赶紧", "马上", "来不及", "上车"),
            fear_of_missing=_contains(text, "错过", "踏空", "怕没", "怕错过"),
            social_proof=_contains(text, "大家都", "朋友群", "群里", "都在", "一致"),
            price_chasing=_contains(text, "追涨", "冲", "上车", "涨停", "继续涨"),
            regret=_contains(text, "后悔", "早知道", "踏空"),
            panic=_contains(text, "恐慌", "慌", "崩", "被套", "亏损扩大"),
        )
        return AnalysisContract(
            actor={"type": actor, "confidence": 0.84 if actor != "retail" else 0.78},
            investor={"level": investor, "confidence": 0.87 if investor != "unknown" else 0.55},
            novice_signals=novice_signals,
            direction={"value": direction, "confidence": 0.82},
            intent={"value": intent, "confidence": 0.78},
            position={"value": position, "confidence": 0.76},
            emotion=emotion,
            spam={"value": spam, "confidence": 0.9 if spam else 0.82},
            promotion={
                "value": commercial,
                "confidence": 0.94 if commercial else 0.82,
            },
        )


class OpenAICompatibleAnalysisProvider:
    """Strict JSON analysis over an OpenAI-compatible chat-completions API.

    The adapter intentionally speaks the small common HTTP contract instead of
    importing a provider SDK. This supports OpenAI and compatible gateways
    while keeping credentials in runtime settings and out of stored rows.
    """

    _system_prompt = """You are a financial-community content annotator.
Return JSON only. The JSON must contain exactly these top-level fields:
actor, investor, novice_signals, direction, intent, position, emotion, spam, promotion.
Use the allowed values from the schema and confidence numbers between 0 and 1.
Extract observable language; do not infer demographics from words such as 宝妈,
姐妹, 老公, or 孩子. Keep retail, KOL, media, commercial, bot/spam, and unknown
separate. Do not make a price prediction or a BUY/SELL recommendation. Promotion
is separate from spam: it means commercial advertising, paid placement, courses,
or lead generation. Promotional content must not count as retail emotion.

Schema:
{
  "actor": {"type": "retail|kol|media|institutional|commercial|bot_or_spam|unknown", "confidence": 0.0},
  "investor": {"level": "novice|intermediate|experienced|unknown", "confidence": 0.0},
  "novice_signals": ["observable_signal"],
  "direction": {"value": "bullish|neutral|bearish|unknown", "confidence": 0.0},
  "intent": {"value": "buy|sell|hold|wait|unknown", "confidence": 0.0},
  "position": {"value": "not_owned|owned|sold|unknown", "confidence": 0.0},
  "emotion": {"urgency": false, "fear_of_missing": false, "social_proof": false, "price_chasing": false, "regret": false, "panic": false},
  "spam": {"value": false, "confidence": 0.0},
  "promotion": {"value": false, "confidence": 0.0}
}
"""

    self_paced = True
    batch_size = 4

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        model: str,
        timeout: float = 45.0,
        min_interval: float = 0.0,
        role: str = "primary",
    ):
        self.endpoint = endpoint.rstrip("/") + "/chat/completions"
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.min_interval = max(0.0, min_interval)
        self.role = role
        self._rate_lock = Lock()
        self._next_request_at = 0.0

    def _pace(self) -> None:
        if self.min_interval <= 0:
            return
        with self._rate_lock:
            delay = max(0.0, self._next_request_at - monotonic())
            if delay:
                from time import sleep

                sleep(delay)
            self._next_request_at = monotonic() + self.min_interval

    @staticmethod
    def _content_from_response(payload: Any) -> str:
        try:
            content = payload["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("LLM response has no choices[0].message.content") from exc
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = [item.get("text", "") for item in content if isinstance(item, dict)]
            return "".join(str(part) for part in parts)
        raise ValueError("LLM response content is not text")

    @staticmethod
    def _parse_json(content: str) -> dict[str, Any]:
        cleaned = content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.removeprefix("```").strip()
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].lstrip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].rstrip()
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError("LLM response is not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise TypeError("LLM response JSON must be an object")
        return parsed

    def analyze(
        self, *, title: str | None, body: str, topic: Topic | None = None
    ) -> AnalysisContract:
        self._pace()
        topic_context = ""
        if topic is not None:
            aliases = ", ".join(alias.alias for alias in topic.aliases)
            topic_context = f"\nTopic: {topic.name} ({topic.slug})"
            if aliases:
                topic_context += f"; aliases: {aliases}"
        user_text = f"Title: {title or ''}{topic_context}\nBody:\n{body[:16000]}"
        payload = {
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": self._system_prompt},
                {"role": "user", "content": user_text},
            ],
        }
        try:
            with httpx.Client(
                timeout=self.timeout,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
            ) as client:
                response = client.post(self.endpoint, json=payload)
                response.raise_for_status()
                response_payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ValueError(f"LLM analysis request failed: {exc}") from exc
        return validate_analysis(self._parse_json(self._content_from_response(response_payload)))

    def analyze_batch(
        self, *, items: list[dict[str, Any]]
    ) -> dict[int, AnalysisContract]:
        """Analyze several contents in one request while retaining strict per-item validation."""

        if not items:
            return {}
        self._pace()
        input_rows = [
            {
                "id": int(item["id"]),
                "title": str(item.get("title") or ""),
                "body": str(item.get("body") or "")[:2400],
                "topics": list(item.get("topics") or []),
            }
            for item in items
        ]
        system_prompt = self._system_prompt + """
Analyze every input item. Return one JSON object shaped as {"items":[...]};
each output item must copy its integer id and contain all analysis schema fields.
Do not omit, merge, or reorder input ids.
"""
        payload = {
            "model": self.model,
            "max_tokens": min(16000, 1600 + len(input_rows) * 900),
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(input_rows, ensure_ascii=False, separators=(",", ":")),
                },
            ],
        }
        try:
            with httpx.Client(
                timeout=self.timeout,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
            ) as client:
                response = client.post(self.endpoint, json=payload)
                response.raise_for_status()
                response_payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ValueError(f"LLM batch analysis request failed: {exc}") from exc
        try:
            parsed = self._parse_json(self._content_from_response(response_payload))
            output_items = parsed.get("items")
            if not isinstance(output_items, list) or not all(
                isinstance(item, dict) for item in output_items
            ):
                raise TypeError("LLM batch response has no valid items array")
            expected_ids = [int(item["id"]) for item in input_rows]
            returned_ids = [int(item.get("id", -1)) for item in output_items]
            if returned_ids != expected_ids:
                raise ValueError(
                    f"LLM batch returned ids {returned_ids}; expected {expected_ids}"
                )
            return {
                int(item["id"]): validate_analysis(
                    {key: value for key, value in item.items() if key != "id"}
                )
                for item in output_items
            }
        except (TypeError, ValueError) as exc:
            # Some free/shared models occasionally collapse a valid multi-item
            # request into one object. Split only that malformed response; HTTP
            # and rate-limit failures remain bounded by the outer task retry.
            if len(items) > 1:
                middle = len(items) // 2
                logger.warning(
                    "event=analysis_batch_splitting model=%s size=%d left=%d right=%d "
                    "error_type=%s error=%r",
                    self.model,
                    len(items),
                    middle,
                    len(items) - middle,
                    type(exc).__name__,
                    str(exc),
                )
                return {
                    **self.analyze_batch(items=items[:middle]),
                    **self.analyze_batch(items=items[middle:]),
                }
            item = items[0]
            topic_names = ", ".join(str(value) for value in item.get("topics") or [])
            body = str(item.get("body") or "")
            if topic_names:
                body = f"Topics: {topic_names}\n{body}"
            return {
                int(item["id"]): self.analyze(
                    title=str(item.get("title") or ""),
                    body=body,
                )
            }


class FailoverAnalysisProvider:
    """Use a secondary configured model only when the primary call fails."""

    self_paced = True

    def __init__(self, primary: AnalysisProvider, fallback: AnalysisProvider):
        self.primary = primary
        self.fallback = fallback
        self.model = primary.model
        self.models = tuple(dict.fromkeys((primary.model, fallback.model)))
        self.last_model = primary.model
        self.last_role = "primary"

    def analyze(
        self, *, title: str | None, body: str, topic: Topic | None = None
    ) -> AnalysisContract | dict[str, Any]:
        try:
            result = self.primary.analyze(title=title, body=body, topic=topic)
        except Exception as primary_error:  # noqa: BLE001 - provider boundary
            increment("analysis_failover_total")
            logger.warning(
                "event=analysis_failover_started primary_model=%s fallback_model=%s "
                "error_type=%s error=%r",
                self.primary.model,
                self.fallback.model,
                type(primary_error).__name__,
                str(primary_error),
            )
            try:
                result = self.fallback.analyze(title=title, body=body, topic=topic)
            except Exception as fallback_error:
                logger.error(
                    "event=analysis_failover_failed primary_model=%s fallback_model=%s "
                    "primary_error=%r fallback_error=%r",
                    self.primary.model,
                    self.fallback.model,
                    str(primary_error),
                    str(fallback_error),
                )
                raise RuntimeError(
                    "both configured LLM providers failed; "
                    f"primary={type(primary_error).__name__}, "
                    f"fallback={type(fallback_error).__name__}"
                ) from fallback_error
            self.last_model = self.fallback.model
            self.last_role = "fallback"
            logger.info(
                "event=analysis_failover_completed primary_model=%s fallback_model=%s",
                self.primary.model,
                self.fallback.model,
            )
            return result
        self.last_model = self.primary.model
        self.last_role = "primary"
        return result

    def analyze_batch(
        self, *, items: list[dict[str, Any]]
    ) -> dict[int, AnalysisContract]:
        primary_batch = getattr(self.primary, "analyze_batch", None)
        fallback_batch = getattr(self.fallback, "analyze_batch", None)
        if not callable(primary_batch) or not callable(fallback_batch):
            raise TypeError("both failover providers must support batch analysis")
        try:
            result = primary_batch(items=items)
        except Exception as primary_error:  # noqa: BLE001 - provider boundary
            increment("analysis_failover_total")
            logger.warning(
                "event=analysis_batch_failover_started primary_model=%s "
                "fallback_model=%s size=%d error_type=%s error=%r",
                self.primary.model,
                self.fallback.model,
                len(items),
                type(primary_error).__name__,
                str(primary_error),
            )
            try:
                result = fallback_batch(items=items)
            except Exception as fallback_error:
                logger.error(
                    "event=analysis_batch_failover_failed primary_model=%s "
                    "fallback_model=%s size=%d primary_error=%r fallback_error=%r",
                    self.primary.model,
                    self.fallback.model,
                    len(items),
                    str(primary_error),
                    str(fallback_error),
                )
                raise RuntimeError(
                    "both configured LLM batch providers failed; "
                    f"primary={type(primary_error).__name__}, "
                    f"fallback={type(fallback_error).__name__}"
                ) from fallback_error
            self.last_model = self.fallback.model
            self.last_role = "fallback"
            return result
        self.last_model = self.primary.model
        self.last_role = "primary"
        return result

    @property
    def batch_size(self) -> int:
        return min(
            int(getattr(self.primary, "batch_size", 1)),
            int(getattr(self.fallback, "batch_size", 1)),
        )


def _external_analysis_provider(
    config: LLMProviderConfig,
) -> OpenAICompatibleAnalysisProvider:
    if config.provider not in {"openai", "openai-compatible"}:
        raise ValueError(f"unsupported LLM provider: {config.provider}")
    return OpenAICompatibleAnalysisProvider(
        endpoint=config.base_url or "",
        api_key=config.api_key or "",
        model=config.model,
        timeout=config.timeout_seconds,
        min_interval=config.min_interval,
        role=config.role,
    )


def analysis_provider_for_settings(settings: Settings) -> AnalysisProvider:
    """Build the configured provider and fail explicitly when it is incomplete."""

    provider = settings.llm_provider or "rule-based"
    if provider in {"rule-based", "local", "none", "disabled"}:
        return RuleBasedAnalysisProvider()
    if provider not in {"openai", "openai-compatible"}:
        raise ValueError(f"unsupported LLM provider: {provider}")
    status = llm_config_status(settings)
    if not status["configured"]:
        raise ValueError("LLM configuration is incomplete: " + ", ".join(status["missing"]))
    primary = _external_analysis_provider(settings.primary_llm())
    fallback_config = settings.fallback_llm()
    if fallback_config is None:
        return primary
    fallback_status = status["providers"][1]
    if not fallback_status["configured"]:
        logger.warning(
            "event=analysis_fallback_disabled reason=incomplete missing=%s",
            fallback_status["missing"],
        )
        return primary
    fallback = _external_analysis_provider(fallback_config)
    return FailoverAnalysisProvider(primary, fallback)


def coerce_analysis_aliases(value: dict[str, Any]) -> dict[str, Any]:
    """Correct narrow provider aliases while retaining strict final validation."""

    corrected = dict(value)
    actor = corrected.get("actor")
    if isinstance(actor, dict) and "type" not in actor and "value" in actor:
        corrected["actor"] = {**actor, "type": actor["value"]}
        corrected["actor"].pop("value", None)
    investor = corrected.get("investor")
    if isinstance(investor, dict) and "level" not in investor and "value" in investor:
        corrected["investor"] = {**investor, "level": investor["value"]}
        corrected["investor"].pop("value", None)
    return corrected


def validate_analysis(value: AnalysisContract | dict[str, Any]) -> AnalysisContract:
    if isinstance(value, AnalysisContract):
        return value
    return AnalysisContract.model_validate(coerce_analysis_aliases(value))


def save_content_analysis(
    session: Session,
    content: Content,
    result: AnalysisContract | dict[str, Any],
    *,
    model: str,
    prompt_version: str,
    schema_version: str,
    topic_id: int | None = None,
    input_hash: str | None = None,
) -> ContentAnalysis:
    contract = validate_analysis(result)
    input_hash = input_hash or analysis_input_hash(content, topic_id=topic_id)
    topic_filter = (
        ContentAnalysis.topic_id.is_(None)
        if topic_id is None
        else ContentAnalysis.topic_id == topic_id
    )
    existing = session.scalar(
        select(ContentAnalysis).where(
            ContentAnalysis.content_id == content.id,
            topic_filter,
            ContentAnalysis.input_hash == input_hash,
            ContentAnalysis.model == model,
            ContentAnalysis.prompt_version == prompt_version,
            ContentAnalysis.schema_version == schema_version,
        )
    )
    if existing:
        return existing
    row = ContentAnalysis(
        content_id=content.id,
        topic_id=topic_id,
        input_hash=input_hash,
        model=model,
        prompt_version=prompt_version,
        schema_version=schema_version,
        actor_type=contract.actor.type,
        actor_confidence=contract.actor.confidence,
        investor_level=contract.investor.level,
        investor_confidence=contract.investor.confidence,
        direction=contract.direction.value,
        direction_confidence=contract.direction.confidence,
        intent=contract.intent.value,
        intent_confidence=contract.intent.confidence,
        position=contract.position.value,
        position_confidence=contract.position.confidence,
        novice_signals=contract.novice_signals,
        emotion_signals=contract.emotion.model_dump(),
        spam=contract.spam.value,
        spam_confidence=contract.spam.confidence,
        promotion=contract.promotion.value,
        promotion_confidence=contract.promotion.confidence,
        created_at=now_utc(),
    )
    session.add(row)
    session.flush()
    return row


def save_content_analysis_review(
    session: Session,
    analysis: ContentAnalysis,
    *,
    reviewer: str,
    input_hash: str,
    intent_basis: str,
    intent_evidence: str,
    rationale: str,
    payload: dict[str, Any],
) -> ContentAnalysisReview:
    """Attach inspectable evidence to a versioned semantic result."""

    existing = session.scalar(
        select(ContentAnalysisReview).where(
            ContentAnalysisReview.content_analysis_id == analysis.id
        )
    )
    if existing is not None:
        return existing
    row = ContentAnalysisReview(
        content_analysis_id=analysis.id,
        reviewer=reviewer,
        input_hash=input_hash,
        intent_basis=intent_basis,
        intent_evidence=intent_evidence,
        rationale=rationale,
        payload=payload,
        created_at=now_utc(),
    )
    session.add(row)
    session.flush()
    return row


def _has_current_evidence_review(session: Session, content: Content) -> bool:
    """Return whether this exact content version already has an audited LLM review."""

    return (
        session.scalar(
            select(ContentAnalysis.id)
            .join(
                ContentAnalysisReview,
                ContentAnalysisReview.content_analysis_id == ContentAnalysis.id,
            )
            .where(
                ContentAnalysis.content_id == content.id,
                ContentAnalysis.input_hash
                == analysis_input_hash(content, topic_id=None),
            )
            .limit(1)
        )
        is not None
    )


def _has_current_external_analysis(session: Session, content: Content) -> bool:
    """Return whether any non-rule LLM row matches this exact content version."""

    rows = session.scalars(
        select(ContentAnalysis).where(
            ContentAnalysis.content_id == content.id,
            ~ContentAnalysis.model.like("rule-based%"),
        )
    ).all()
    for row in rows:
        topic = session.get(Topic, row.topic_id) if row.topic_id is not None else None
        if row.input_hash == analysis_input_hash(
            content,
            topic=topic,
            topic_id=row.topic_id,
        ):
            return True
    return False


def _analysis_task_for_candidate(
    session: Session,
    *,
    content: Content,
    topic: Topic | None,
    input_hash: str,
    provider: AnalysisProvider,
    settings: Settings,
) -> AnalysisTask:
    task = session.scalar(
        select(AnalysisTask).where(
            AnalysisTask.content_id == content.id,
            (
                AnalysisTask.topic_id.is_(None)
                if topic is None
                else AnalysisTask.topic_id == topic.id
            ),
            AnalysisTask.input_hash == input_hash,
            AnalysisTask.model == provider.model,
            AnalysisTask.prompt_version == settings.prompt_version,
            AnalysisTask.schema_version == settings.analysis_schema_version,
        )
    )
    if task is None:
        task = AnalysisTask(
            content_id=content.id,
            topic_id=topic.id if topic else None,
            input_hash=input_hash,
            model=provider.model,
            prompt_version=settings.prompt_version,
            schema_version=settings.analysis_schema_version,
            status="pending",
            attempts=0,
            created_at=now_utc(),
            updated_at=now_utc(),
        )
        session.add(task)
        session.flush()
    return task


def _analyze_pending_in_batches(
    session: Session,
    *,
    candidates: list[tuple[Content, Topic | None, str]],
    provider: AnalysisProvider,
    settings: Settings,
) -> tuple[int, int]:
    batch_analyzer = getattr(provider, "analyze_batch", None)
    if not callable(batch_analyzer):
        raise TypeError("provider does not support batch analysis")
    batch_size = max(1, int(getattr(provider, "batch_size", 1)))
    grouped: dict[int, tuple[Content, list[tuple[Topic | None, str]]]] = {}
    for content, topic, input_hash in candidates:
        group = grouped.setdefault(content.id, (content, []))
        group[1].append((topic, input_hash))

    count = 0
    failed = 0
    groups = list(grouped.values())
    for offset in range(0, len(groups), batch_size):
        batch = groups[offset : offset + batch_size]
        tasks: list[tuple[Content, Topic | None, str, AnalysisTask]] = []
        input_rows: list[dict[str, Any]] = []
        for content, targets in batch:
            topic_labels: list[str] = []
            for topic, input_hash in targets:
                task = _analysis_task_for_candidate(
                    session,
                    content=content,
                    topic=topic,
                    input_hash=input_hash,
                    provider=provider,
                    settings=settings,
                )
                task.status = "running"
                task.attempts = int(task.attempts or 0) + 1
                task.updated_at = now_utc()
                tasks.append((content, topic, input_hash, task))
                if topic is not None:
                    topic_labels.append(f"{topic.name} ({topic.slug})")
            input_rows.append(
                {
                    "id": content.id,
                    "title": content.title or "",
                    "body": content.body,
                    "topics": topic_labels,
                }
            )

        started = monotonic()
        try:
            _pace_llm(provider, settings)
            results = batch_analyzer(items=input_rows)
            expected_ids = {content.id for content, _targets in batch}
            if set(results) != expected_ids:
                raise ValueError(
                    f"LLM batch returned ids {sorted(results)}; expected {sorted(expected_ids)}"
                )
        except Exception as exc:  # noqa: BLE001 - keep batch durable and retryable
            increment("analysis_errors_total")
            for _content, _topic, _input_hash, task in tasks:
                failed += 1
                task.status = "failed"
                task.last_error = str(exc)[:2000]
                task.next_retry_at = now_utc() + timedelta(
                    seconds=min(6 * 3600, 30 * (2 ** min(task.attempts - 1, 8)))
                )
                task.updated_at = now_utc()
            session.commit()
            logger.warning(
                "event=analysis_batch_failed route_model=%s contents=%d targets=%d "
                "elapsed_seconds=%.3f error_type=%s error=%r",
                provider.model,
                len(batch),
                len(tasks),
                monotonic() - started,
                type(exc).__name__,
                str(exc)[:2000],
            )
            continue

        elapsed = monotonic() - started
        set_gauge("analysis_latency_seconds", elapsed)
        result_model = str(getattr(provider, "last_model", provider.model))
        for content, topic, input_hash, task in tasks:
            analysis = save_content_analysis(
                session,
                content,
                results[content.id],
                model=result_model,
                prompt_version=settings.prompt_version,
                schema_version=settings.analysis_schema_version,
                topic_id=topic.id if topic else None,
                input_hash=input_hash,
            )
            task.status = "completed"
            task.next_retry_at = None
            task.last_error = None
            task.updated_at = now_utc()
            increment("analysis_total")
            count += 1
            logger.debug(
                "event=analysis_completed task_id=%s analysis_id=%s content_id=%d "
                "topic=%s route_model=%s result_model=%s provider_role=%s "
                "batch_contents=%d elapsed_seconds=%.3f intent=%s",
                task.id,
                analysis.id,
                content.id,
                topic.slug if topic else None,
                provider.model,
                result_model,
                getattr(provider, "last_role", "primary"),
                len(batch),
                elapsed,
                analysis.intent,
            )
        # One provider response can fan out to multiple topic-scoped rows. Commit
        # the entire response atomically so retries never duplicate a partial batch.
        session.commit()
        logger.info(
            "event=analysis_batch_completed route_model=%s result_model=%s "
            "contents=%d targets=%d elapsed_seconds=%.3f",
            provider.model,
            result_model,
            len(batch),
            len(tasks),
            elapsed,
        )
    return count, failed


def analyze_pending(
    session: Session,
    *,
    limit: int = 500,
    provider: AnalysisProvider | None = None,
    settings: Settings | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> int:
    settings = settings or get_settings()
    if provider is None:
        try:
            provider = analysis_provider_for_settings(settings)
        except ValueError as exc:
            if "LLM configuration is incomplete" not in str(exc):
                raise
            queued = enqueue_pending_analysis_tasks(
                session,
                settings=settings,
                limit=limit,
                since=since,
                until=until,
            )
            logger.warning(
                "event=analysis_deferred model=%s queued=%d reason=%r",
                settings.analysis_model,
                queued,
                str(exc),
            )
            return 0
    if limit < 1:
        raise ValueError("limit must be positive")
    content_query = select(Content)
    if since is not None:
        content_query = content_query.where(Content.published_at >= as_utc(since))
    if until is not None:
        content_query = content_query.where(Content.published_at < as_utc(until))
    contents = session.scalars(content_query.order_by(Content.id)).all()
    provider_models = tuple(
        dict.fromkeys(getattr(provider, "models", (provider.model,)))
    )
    logger.info(
        "event=analysis_scan_started model=%s contents=%d limit=%d since=%s until=%s",
        provider.model,
        len(contents),
        limit,
        since.isoformat() if since else None,
        until.isoformat() if until else None,
    )
    now = now_utc()
    candidates: list[tuple[Content, Topic | None, str]] = []
    already_analyzed = 0
    retry_deferred = 0
    for content in contents:
        topics = session.scalars(
            select(Topic)
            .join(ContentEntity, ContentEntity.entity_id == Topic.id)
            .where(
                ContentEntity.content_id == content.id,
                ContentEntity.entity_type == "topic",
                Topic.status == "active",
            )
            .order_by(Topic.id)
        ).all()
        if _has_current_evidence_review(session, content):
            already_analyzed += len(topics or [None])
            continue
        for topic in topics or [None]:
            input_hash = analysis_input_hash(content, topic=topic)
            topic_filter = (
                ContentAnalysis.topic_id.is_(None)
                if topic is None
                else ContentAnalysis.topic_id == topic.id
            )
            exists = session.scalar(
                select(ContentAnalysis.id).where(
                    ContentAnalysis.content_id == content.id,
                    topic_filter,
                    ContentAnalysis.input_hash == input_hash,
                    ContentAnalysis.model.in_(provider_models),
                    ContentAnalysis.prompt_version == settings.prompt_version,
                    ContentAnalysis.schema_version == settings.analysis_schema_version,
                )
            )
            if exists is not None:
                already_analyzed += 1
                continue
            task = session.scalar(
                select(AnalysisTask).where(
                    AnalysisTask.content_id == content.id,
                    (
                        AnalysisTask.topic_id.is_(None)
                        if topic is None
                        else AnalysisTask.topic_id == topic.id
                    ),
                    AnalysisTask.input_hash == input_hash,
                    AnalysisTask.model == provider.model,
                    AnalysisTask.prompt_version == settings.prompt_version,
                    AnalysisTask.schema_version == settings.analysis_schema_version,
                )
            )
            if task is not None and task.next_retry_at is not None and task.next_retry_at > now:
                retry_deferred += 1
                continue
            candidates.append((content, topic, input_hash))
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    logger.info(
        "event=analysis_batch_selected model=%s candidates=%d already_analyzed=%d "
        "retry_deferred=%d",
        provider.model,
        len(candidates),
        already_analyzed,
        retry_deferred,
    )
    if callable(getattr(provider, "analyze_batch", None)) and int(
        getattr(provider, "batch_size", 1)
    ) > 1:
        count, failed = _analyze_pending_in_batches(
            session,
            candidates=candidates,
            provider=provider,
            settings=settings,
        )
        logger.info(
            "event=analysis_batch_finished model=%s selected=%d completed=%d failed=%d "
            "retry_deferred=%d",
            provider.model,
            len(candidates),
            count,
            failed,
            retry_deferred,
        )
        return count
    count = 0
    failed = 0
    for content, topic, input_hash in candidates:
        task = session.scalar(
            select(AnalysisTask).where(
                AnalysisTask.content_id == content.id,
                (
                    AnalysisTask.topic_id.is_(None)
                    if topic is None
                    else AnalysisTask.topic_id == topic.id
                ),
                AnalysisTask.input_hash == input_hash,
                AnalysisTask.model == provider.model,
                AnalysisTask.prompt_version == settings.prompt_version,
                AnalysisTask.schema_version == settings.analysis_schema_version,
            )
        )
        if task is None:
            task = AnalysisTask(
                content_id=content.id,
                topic_id=topic.id if topic else None,
                input_hash=input_hash,
                model=provider.model,
                prompt_version=settings.prompt_version,
                schema_version=settings.analysis_schema_version,
                status="pending",
                attempts=0,
                created_at=now_utc(),
                updated_at=now_utc(),
            )
            session.add(task)
            session.flush()
        task.status = "running"
        task.attempts = int(task.attempts or 0) + 1
        task.updated_at = now_utc()
        started = monotonic()
        topic_slug = topic.slug if topic else None
        logger.debug(
            "event=analysis_started task_id=%s content_id=%d topic=%s model=%s attempt=%d",
            task.id,
            content.id,
            topic_slug,
            provider.model,
            task.attempts,
        )
        try:
            _pace_llm(provider, settings)
            result = _provider_analyze(provider, content, topic)
        except Exception as exc:  # noqa: BLE001 - keep collection durable and retryable
            increment("analysis_errors_total")
            failed += 1
            task.status = "failed"
            task.last_error = str(exc)[:2000]
            task.next_retry_at = now_utc() + timedelta(
                seconds=min(6 * 3600, 30 * (2 ** min(task.attempts - 1, 8)))
            )
            task.updated_at = now_utc()
            session.commit()
            logger.warning(
                "event=analysis_failed task_id=%s content_id=%d topic=%s model=%s "
                "attempt=%d elapsed_seconds=%.3f next_retry_at=%s error=%r",
                task.id,
                content.id,
                topic_slug,
                provider.model,
                task.attempts,
                monotonic() - started,
                task.next_retry_at.isoformat() if task.next_retry_at else None,
                task.last_error,
            )
            continue
        elapsed = monotonic() - started
        set_gauge("analysis_latency_seconds", elapsed)
        result_model = str(getattr(provider, "last_model", provider.model))
        analysis = save_content_analysis(
            session,
            content,
            result,
            model=result_model,
            prompt_version=settings.prompt_version,
            schema_version=settings.analysis_schema_version,
            topic_id=topic.id if topic else None,
            input_hash=input_hash,
        )
        task.status = "completed"
        task.next_retry_at = None
        task.last_error = None
        task.updated_at = now_utc()
        increment("analysis_total")
        count += 1
        # A long free-tier batch may take minutes. Persist each completed task
        # so a process interruption does not discard earlier LLM results.
        session.commit()
        logger.info(
            "event=analysis_completed task_id=%s analysis_id=%s content_id=%d topic=%s "
            "route_model=%s result_model=%s provider_role=%s elapsed_seconds=%.3f "
            "actor=%s direction=%s intent=%s "
            "promotion=%s spam=%s",
            task.id,
            analysis.id,
            content.id,
            topic_slug,
            provider.model,
            result_model,
            getattr(provider, "last_role", "primary"),
            elapsed,
            analysis.actor_type,
            analysis.direction,
            analysis.intent,
            analysis.promotion,
            analysis.spam,
        )
    logger.info(
        "event=analysis_batch_finished model=%s selected=%d completed=%d failed=%d "
        "retry_deferred=%d",
        provider.model,
        len(candidates),
        count,
        failed,
        retry_deferred,
    )
    return count


def analysis_task_summary(
    session: Session,
    *,
    model: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> dict[str, Any]:
    """Return safe task counters for CLI summaries and operational checks."""

    filters = [AnalysisTask.model == model] if model else []
    task_query = select(AnalysisTask)
    if since is not None or until is not None:
        task_query = task_query.join(Content, Content.id == AnalysisTask.content_id)
    if since is not None:
        task_query = task_query.where(Content.published_at >= as_utc(since))
    if until is not None:
        task_query = task_query.where(Content.published_at < as_utc(until))
    tasks = session.scalars(task_query.where(*filters)).all()
    current_keys: set[tuple[int, int | None, str]] = set()
    content_query = select(Content)
    if since is not None:
        content_query = content_query.where(Content.published_at >= as_utc(since))
    if until is not None:
        content_query = content_query.where(Content.published_at < as_utc(until))
    for content in session.scalars(content_query.order_by(Content.id)).all():
        if _has_current_evidence_review(session, content):
            continue
        topics = session.scalars(
            select(Topic)
            .join(ContentEntity, ContentEntity.entity_id == Topic.id)
            .where(
                ContentEntity.content_id == content.id,
                ContentEntity.entity_type == "topic",
                Topic.status == "active",
            )
        ).all()
        for topic in topics or [None]:
            current_keys.add(
                (
                    content.id,
                    topic.id if topic else None,
                    analysis_input_hash(content, topic=topic),
                )
            )
    current_tasks = [
        task for task in tasks if (task.content_id, task.topic_id, task.input_hash) in current_keys
    ]
    tracked_keys = {(task.content_id, task.topic_id, task.input_hash) for task in current_tasks}
    statuses = Counter(str(task.status) for task in current_tasks)
    now = now_utc()
    retryable = [task for task in current_tasks if task.status in {"pending", "failed"}]
    retry_ready = sum(task.next_retry_at is None or task.next_retry_at <= now for task in retryable)
    retry_deferred = sum(
        task.next_retry_at is not None and task.next_retry_at > now for task in retryable
    )
    return {
        "model": model,
        "targets": len(current_keys),
        "total": len(current_tasks),
        "historical_total": len(tasks),
        "superseded": len(tasks) - len(current_tasks),
        "untracked": len(current_keys - tracked_keys),
        "pending": statuses.get("pending", 0),
        "running": statuses.get("running", 0),
        "completed": statuses.get("completed", 0),
        "failed": statuses.get("failed", 0),
        "retry_ready": retry_ready,
        "retry_deferred": retry_deferred,
    }


def enqueue_pending_analysis_tasks(
    session: Session,
    *,
    settings: Settings,
    limit: int = 500,
    since: datetime | None = None,
    until: datetime | None = None,
) -> int:
    """Persist retryable targets when the configured LLM is temporarily unavailable."""

    if limit < 1:
        raise ValueError("limit must be positive")
    created = 0
    content_query = select(Content)
    if since is not None:
        content_query = content_query.where(Content.published_at >= as_utc(since))
    if until is not None:
        content_query = content_query.where(Content.published_at < as_utc(until))
    for content in session.scalars(content_query.order_by(Content.id)).all():
        if _has_current_evidence_review(session, content):
            continue
        topics = session.scalars(
            select(Topic)
            .join(ContentEntity, ContentEntity.entity_id == Topic.id)
            .where(
                ContentEntity.content_id == content.id,
                ContentEntity.entity_type == "topic",
                Topic.status == "active",
            )
            .order_by(Topic.id)
        ).all()
        for topic in topics or [None]:
            input_hash = analysis_input_hash(content, topic=topic)
            topic_filter = (
                AnalysisTask.topic_id.is_(None)
                if topic is None
                else AnalysisTask.topic_id == topic.id
            )
            existing = session.scalar(
                select(AnalysisTask).where(
                    AnalysisTask.content_id == content.id,
                    topic_filter,
                    AnalysisTask.input_hash == input_hash,
                    AnalysisTask.model == settings.analysis_model,
                    AnalysisTask.prompt_version == settings.prompt_version,
                    AnalysisTask.schema_version == settings.analysis_schema_version,
                )
            )
            if existing is not None:
                continue
            session.add(
                AnalysisTask(
                    content_id=content.id,
                    topic_id=topic.id if topic else None,
                    input_hash=input_hash,
                    model=settings.analysis_model,
                    prompt_version=settings.prompt_version,
                    schema_version=settings.analysis_schema_version,
                    status="pending",
                    attempts=0,
                    created_at=now_utc(),
                    updated_at=now_utc(),
                )
            )
            created += 1
            if created >= limit:
                session.commit()
                return created
    session.commit()
    return created


def analysis_input_hash(
    content: Content, *, topic: Topic | None = None, topic_id: int | None = None
) -> str:
    """Hash the exact content and topic context sent to an analysis provider."""

    payload = {
        "content_hash": content.content_hash,
        "title": content.title or "",
        "body": content.body or "",
        "topic_id": topic.id if topic is not None else topic_id,
        "topic_slug": topic.slug if topic is not None else None,
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _provider_analyze(
    provider: AnalysisProvider, content: Content, topic: Topic | None
) -> AnalysisContract | dict[str, Any]:
    """Call topic-aware providers while retaining small legacy adapters."""

    kwargs: dict[str, Any] = {"title": content.title, "body": content.body}
    try:
        parameters = inspect.signature(provider.analyze).parameters
        accepts_topic = "topic" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
        )
    except (TypeError, ValueError):
        accepts_topic = True
    if accepts_topic:
        kwargs["topic"] = topic
    return provider.analyze(**kwargs)


def _pace_llm(provider: AnalysisProvider, settings: Settings) -> None:
    """Serialize synchronous LLM calls so free gateways are not burst-loaded."""

    global _LLM_NEXT_REQUEST_AT
    if (
        provider.model.startswith("rule-based")
        or getattr(provider, "self_paced", False)
        or settings.llm_min_interval <= 0
    ):
        return
    with _LLM_RATE_LOCK:
        delay = max(0.0, _LLM_NEXT_REQUEST_AT - monotonic())
        if delay:
            from time import sleep

            sleep(delay)
        _LLM_NEXT_REQUEST_AT = monotonic() + settings.llm_min_interval


def fomo_score(analysis: ContentAnalysis) -> float:
    values = analysis.emotion_signals or {}
    return (
        sum(
            bool(values.get(key, False))
            for key in ("urgency", "fear_of_missing", "social_proof", "price_chasing", "regret")
        )
        / 5.0
    )


def analysis_precedence_key(analysis: ContentAnalysis) -> tuple[int, int]:
    """Prefer the newest reviewed semantic contract, then reviewer tier."""

    model = (analysis.model or "").casefold()
    prompt_version = (analysis.prompt_version or "").casefold()
    if "gpt-5.6-sol-via-codex-cli" in model and prompt_version.startswith(
        "codex-content-review-v4:"
    ):
        priority = 475
    elif "gpt-5.6-sol-via-codex-cli" in model and prompt_version.startswith(
        "codex-content-review-v3:"
    ):
        priority = 450
    elif "gpt-5.6-sol-via-codex-cli" in model and prompt_version.startswith(
        "codex-content-review-v2:"
    ):
        priority = 400
    elif "via-openai-compatible" in model and prompt_version.startswith(
        "evidence-content-review-v3:"
    ):
        priority = 460
    elif "via-openai-compatible" in model and prompt_version.startswith(
        "evidence-content-review-v2:"
    ):
        priority = 350
    elif "gpt-5.6-terra-via-codex-cli" in model:
        priority = 300
    elif "via-codex-cli" in model:
        priority = 250
    elif model.startswith("rule-based"):
        priority = 0
    else:
        priority = 200
    return priority, analysis.id
