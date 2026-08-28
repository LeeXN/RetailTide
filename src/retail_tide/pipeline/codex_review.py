from __future__ import annotations

import hashlib
import json
import logging
import re
import subprocess
import tempfile
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from threading import Lock
from time import monotonic, sleep
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Content, ContentAnalysis, Source
from ..schemas import AnalysisContract
from ..time import as_utc
from .analysis import (
    _has_current_external_analysis,
    coerce_analysis_aliases,
    save_content_analysis,
    save_content_analysis_review,
)

CODEX_REVIEWER = "codex-cli"
CODEX_MODEL = "gpt-5.6-sol"
ANALYSIS_MODEL = "gpt-5.6-sol-via-codex-cli"
PROMPT_VERSION = "codex-content-review-v4"
COMPATIBLE_REVIEWER = "openai-compatible"
COMPATIBLE_PROMPT_VERSION = "evidence-content-review-v3"
SCHEMA_VERSION = "content-analysis-v1"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CODEX_PROMPT_PATH = _PROJECT_ROOT / "prompts" / "codex-content-review-v4.txt"
_COMPATIBLE_PROMPT_PATH = _PROJECT_ROOT / "prompts" / "evidence-content-review-v3.txt"
_SCHEMA_PATH = _PROJECT_ROOT / "prompts" / "codex-content-review-batch.schema.json"
_ACTION_BASES = {"explicit_self_executed", "explicit_self_planned"}
_TENDENCY_BASES = {
    "advice_or_recommendation",
    "market_directional_view",
    "risk_warning",
}
_NON_ACTION_BASES = {
    "question_or_advice",
    "negated",
    "reported_or_quoted",
    "market_description",
    "insufficient",
}
logger = logging.getLogger(__name__)

BatchReviewResult = list[dict[str, Any]] | tuple[list[dict[str, Any]], str]


class _RequestPacer:
    """Keep one compatible endpoint inside its configured free-tier cadence."""

    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, min_interval)
        self._lock = Lock()
        self._next_request_at = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            delay = max(0.0, self._next_request_at - monotonic())
            if delay:
                sleep(delay)
            self._next_request_at = monotonic() + self.min_interval


class CompatibleLLMTransportError(RuntimeError):
    """A retryable network, rate-limit, or upstream availability failure."""


def _review_text(content: Content, *, max_chars: int) -> str:
    body = " ".join((content.body or "").split())
    if len(body) <= max_chars:
        return body
    tail_size = min(700, max_chars // 3)
    return f"{body[: max_chars - tail_size]}\n…[中间省略]…\n{body[-tail_size:]}"


def _input_row(content: Content, *, max_chars: int) -> dict[str, Any]:
    return {
        "id": content.id,
        "source": content.source.name,
        "published_at": content.published_at.isoformat(),
        "title": content.title or "",
        "body": _review_text(content, max_chars=max_chars),
    }


def _input_hash(row: dict[str, Any]) -> str:
    encoded = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _analysis_contract(item: dict[str, Any]) -> AnalysisContract:
    try:
        payload = {
            key: item[key]
            for key in (
                "actor",
                "investor",
                "novice_signals",
                "direction",
                "intent",
                "position",
                "emotion",
                "spam",
            )
        }
    except KeyError as exc:
        raise ValueError(f"review item is missing {exc.args[0]}") from exc
    payload["promotion"] = item.get("promotion", {"value": False, "confidence": 0.0})
    return AnalysisContract.model_validate(payload)


def _normalized_evidence(value: str) -> str:
    return "".join(str(value or "").split())


def _validate_review_item(item: dict[str, Any], row: dict[str, Any]) -> AnalysisContract:
    contract = _analysis_contract(item)
    evidence = _normalized_evidence(item.get("intent_evidence", ""))
    source_text = _normalized_evidence(f"{row['title']} {row['body']}")
    basis = item.get("intent_basis")
    rationale = item.get("rationale")
    if basis not in _ACTION_BASES | _TENDENCY_BASES | _NON_ACTION_BASES | {
        "explicit_self_position",
        "explicit_wait",
    }:
        raise ValueError(f"content {row['id']} returned an invalid intent basis")
    if not isinstance(rationale, str) or not rationale.strip():
        raise ValueError(f"content {row['id']} returned no intent rationale")
    normalized_rationale = _normalized_evidence(rationale)
    contradiction_terms = ("非作者本人", "不是作者本人", "无本人", "给读者建议", "操作建议")
    explicit_basis = basis in _ACTION_BASES | {"explicit_self_position", "explicit_wait"}
    if (
        contract.intent.value != "unknown"
        and explicit_basis
        and any(term in normalized_rationale for term in contradiction_terms)
    ):
        raise ValueError(f"content {row['id']} returned an intent contradicting its rationale")
    if evidence and evidence not in source_text:
        raise ValueError(f"content {row['id']} returned intent evidence that is not in the input")
    if contract.intent.value in {"buy", "sell"} and (
        basis not in _ACTION_BASES | _TENDENCY_BASES or not evidence
    ):
        raise ValueError(
            f"content {row['id']} has {contract.intent.value} without action or tendency evidence"
        )
    if contract.intent.value == "hold" and (
        basis not in {"explicit_self_position", "advice_or_recommendation"} or not evidence
    ):
        raise ValueError(f"content {row['id']} has hold without explicit position evidence")
    if contract.intent.value == "wait" and (
        basis not in {"explicit_wait", "advice_or_recommendation"} or not evidence
    ):
        raise ValueError(f"content {row['id']} has wait without explicit waiting evidence")
    if contract.intent.value == "unknown" and basis not in _NON_ACTION_BASES:
        raise ValueError(f"content {row['id']} has unknown intent with an action basis")
    if basis == "market_directional_view" and (
        (contract.intent.value == "buy" and contract.direction.value != "bullish")
        or (contract.intent.value == "sell" and contract.direction.value != "bearish")
        or contract.intent.value not in {"buy", "sell"}
    ):
        raise ValueError(f"content {row['id']} has a directional tendency that does not align")
    if basis == "risk_warning" and (
        contract.intent.value != "sell" or contract.direction.value != "bearish"
    ):
        raise ValueError(f"content {row['id']} has a risk warning that is not bearish/sell")
    return contract


def _run_codex_batch(
    rows: list[dict[str, Any]],
    *,
    model: str,
    timeout: float,
) -> list[dict[str, Any]]:
    instructions = _CODEX_PROMPT_PATH.read_text(encoding="utf-8")
    payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    prompt = (
        f"{instructions}\n\n"
        "不要读取工作区文件，不要调用任何工具；只审查下面的输入 JSON。\n"
        f"输入 JSON：{payload}"
    )
    with tempfile.NamedTemporaryFile(prefix="retail-tide-codex-", suffix=".json") as output:
        command = [
            "codex",
            "exec",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--color",
            "never",
            "-s",
            "read-only",
            "-m",
            model,
            "-c",
            'model_reasoning_effort="low"',
            "-C",
            str(_PROJECT_ROOT),
            "--output-schema",
            str(_SCHEMA_PATH),
            "-o",
            output.name,
            "-",
        ]
        try:
            completed = subprocess.run(
                command,
                input=prompt,
                text=True,
                cwd=_PROJECT_ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"Codex review timed out after {timeout:g}s") from exc
        if completed.returncode != 0:
            diagnostic = "\n".join((completed.stderr or "").splitlines()[-5:])
            raise RuntimeError(
                f"Codex review exited with {completed.returncode}: {diagnostic or 'no diagnostic'}"
            )
        try:
            response = json.loads(Path(output.name).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("Codex review did not return valid JSON") from exc
    items = response.get("items") if isinstance(response, dict) else None
    if not isinstance(items, list):
        raise TypeError("Codex review JSON has no items array")
    expected = {int(row["id"]) for row in rows}
    returned = {int(item.get("id", -1)) for item in items if isinstance(item, dict)}
    if returned != expected or len(items) != len(rows):
        raise RuntimeError(
            f"Codex review returned ids {sorted(returned)}; expected {sorted(expected)}"
        )
    by_id = {int(item["id"]): item for item in items}
    return [by_id[int(row["id"])] for row in rows]


def _run_codex_batch_resilient(
    rows: list[dict[str, Any]],
    *,
    model: str,
    timeout: float,
    attempts: int = 3,
) -> list[dict[str, Any]]:
    """Retry structural failures while preserving valid items in a semantic batch."""

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            items = _run_codex_batch(rows, model=model, timeout=timeout)
            repaired = list(items)
            for index, (row, item) in enumerate(zip(rows, items, strict=True)):
                try:
                    _validate_review_item(item, row)
                except (TypeError, ValueError) as validation_error:
                    # The output schema already proved the rest of this item is
                    # structurally complete. A non-verifiable intent must not
                    # force another LLM call for every otherwise-valid peer.
                    repaired[index] = _degrade_unverifiable_intent(
                        item, row, validation_error
                    )
                    logger.warning(
                        "event=codex_review_intent_degraded model=%s content_id=%s "
                        "reason=%r",
                        model,
                        row["id"],
                        str(validation_error),
                    )
            return repaired
        except (RuntimeError, TypeError, ValueError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleep(min(2**attempt, 4))
    content_id = rows[0]["id"] if rows else "unknown"
    raise RuntimeError(
        f"Codex could not produce a valid review for batch starting at content {content_id}: "
        f"{last_error or 'unknown error'}"
    ) from last_error


def _parse_json_object(content: str) -> dict[str, Any]:
    cleaned = content.strip()
    cleaned = re.sub(
        r"<think\b[^>]*>.*?</think>", "", cleaned, flags=re.IGNORECASE | re.DOTALL
    ).strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.removeprefix("```").strip()
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].lstrip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].rstrip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        decoder = json.JSONDecoder()
        candidates: list[tuple[int, Any]] = []
        for index, character in enumerate(cleaned):
            if character not in "[{":
                continue
            try:
                candidate, _end = decoder.raw_decode(cleaned[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and "items" in candidate:
                candidates.append((3, candidate))
            elif isinstance(candidate, list):
                candidates.append((2, candidate))
            elif isinstance(candidate, dict) and "id" in candidate:
                candidates.append((1, candidate))
        if not candidates:
            raise ValueError("compatible LLM response is not valid JSON") from exc
        best_priority = max(priority for priority, _candidate in candidates)
        parsed = [candidate for priority, candidate in candidates if priority == best_priority][-1]
    if isinstance(parsed, list):
        return {"items": parsed}
    if not isinstance(parsed, dict):
        raise TypeError("compatible LLM response JSON must be an object")
    return parsed


def _response_text(payload: Any) -> str:
    try:
        content = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("compatible LLM response has no choices[0].message.content") from exc
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(str(item.get("text", "")) for item in content if isinstance(item, dict))
    raise ValueError("compatible LLM response content is not text")


def _coerce_compatible_item(item: dict[str, Any]) -> dict[str, Any]:
    """Correct narrow, unambiguous schema aliases without inventing semantics."""

    corrected = coerce_analysis_aliases(item)
    intent = corrected.get("intent")
    intent_value = intent.get("value") if isinstance(intent, dict) else None
    if intent_value == "unknown" and corrected.get("intent_basis") in _TENDENCY_BASES:
        corrected = {**corrected, "intent_basis": "insufficient"}
    return corrected


def _degrade_unverifiable_intent(
    item: dict[str, Any], row: dict[str, Any], error: Exception
) -> dict[str, Any]:
    """Keep a schema-valid LLM review while refusing an untraceable intent.

    This fallback is used only after an isolated retry also failed.  It never
    invents evidence or a directional action: the intent becomes unknown and
    the audit rationale records why the stricter claim was discarded.
    """

    _analysis_contract(item)
    intent = item.get("intent")
    if not isinstance(intent, dict):
        raise error
    rationale = str(item.get("rationale") or "").strip()
    degraded = {
        **item,
        "intent": {**intent, "value": "unknown", "confidence": 0.0},
        "intent_basis": "insufficient",
        "intent_evidence": "",
        "rationale": (
            f"{rationale}；意图证据未通过原文逐字校验，保守降级为 unknown。"
            if rationale
            else "意图证据未通过原文逐字校验，保守降级为 unknown。"
        ),
    }
    _validate_review_item(degraded, row)
    return degraded


def _ordered_review_items(
    response: dict[str, Any], rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    items = response.get("items")
    if items is None and len(rows) == 1 and "id" in response:
        items = [response]
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise TypeError("compatible LLM review JSON has no valid items array")
    corrected = [_coerce_compatible_item(item) for item in items]
    expected = {int(row["id"]) for row in rows}
    returned = {int(item.get("id", -1)) for item in corrected}
    if returned != expected or len(corrected) != len(rows):
        raise RuntimeError(
            f"compatible LLM returned ids {sorted(returned)}; expected {sorted(expected)}"
        )
    by_id = {int(item["id"]): item for item in corrected}
    return [by_id[int(row["id"])] for row in rows]


def _run_compatible_batch(
    rows: list[dict[str, Any]],
    *,
    endpoint: str,
    api_key: str,
    model: str,
    timeout: float,
) -> list[dict[str, Any]]:
    instructions = _COMPATIBLE_PROMPT_PATH.read_text(encoding="utf-8")
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    system_prompt = (
        f"{instructions}\n\n"
        "只返回一个 JSON 对象，不要使用 Markdown。输出必须严格符合下面的 JSON Schema：\n"
        f"{json.dumps(schema, ensure_ascii=False, separators=(',', ':'))}"
    )
    payload = {
        "model": model,
        # Reasoning-capable compatible models can consume the gateway's small
        # default completion budget before finishing a multi-item JSON array.
        # Reserve enough output room per item while keeping a bounded ceiling
        # for free/shared endpoints.
        "max_tokens": min(16000, 1600 + len(rows) * 900),
        "temperature": 0,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "content_analysis_review_batch",
                "strict": True,
                "schema": schema,
            },
        },
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": "逐条审查下面的输入 JSON：\n"
                + json.dumps(rows, ensure_ascii=False, separators=(",", ":")),
            },
        ],
    }
    url = endpoint.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    try:
        with httpx.Client(
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        ) as client:
            response = client.post(url, json=payload)
            if response.status_code in {400, 404, 422}:
                fallback_payload = {**payload, "response_format": {"type": "json_object"}}
                response = client.post(url, json=fallback_payload)
            response.raise_for_status()
            response_payload = response.json()
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        error = f"compatible LLM request failed with HTTP {status_code}"
        if status_code == 429 or status_code >= 500:
            raise CompatibleLLMTransportError(error) from exc
        raise RuntimeError(error) from exc
    except httpx.HTTPError as exc:
        raise CompatibleLLMTransportError(
            f"compatible LLM request failed: {type(exc).__name__}"
        ) from exc
    except ValueError as exc:
        raise RuntimeError("compatible LLM response body is not JSON") from exc
    parsed = _parse_json_object(_response_text(response_payload))
    return _ordered_review_items(parsed, rows)


def _run_compatible_batch_resilient(
    rows: list[dict[str, Any]],
    *,
    endpoint: str,
    api_key: str,
    model: str,
    timeout: float,
    attempts: int = 2,
    transport_attempts: int = 12,
    pacer: _RequestPacer | None = None,
) -> list[dict[str, Any]]:
    """Retry transient/schema failures, then isolate a bad item by splitting."""

    started = monotonic()
    logger.info(
        "event=compatible_llm_batch_started model=%s size=%d timeout_seconds=%.1f",
        model,
        len(rows),
        timeout,
    )
    last_error: Exception | None = None
    schema_attempt = 0
    transport_attempt = 0
    while schema_attempt < attempts:
        try:
            if pacer is not None:
                pacer.wait()
            items = _run_compatible_batch(
                rows,
                endpoint=endpoint,
                api_key=api_key,
                model=model,
                timeout=timeout,
            )
            invalid: list[tuple[int, Exception]] = []
            for index, (row, item) in enumerate(zip(rows, items, strict=True)):
                try:
                    _validate_review_item(item, row)
                except (TypeError, ValueError) as exc:
                    invalid.append((index, exc))
            if invalid and len(rows) > 1:
                logger.warning(
                    "event=compatible_llm_batch_repairing_items model=%s size=%d "
                    "invalid=%d content_ids=%s",
                    model,
                    len(rows),
                    len(invalid),
                    [rows[index]["id"] for index, _error in invalid],
                )
                repaired = list(items)
                for index, validation_error in invalid:
                    try:
                        repaired[index] = _run_compatible_batch_resilient(
                            [rows[index]],
                            endpoint=endpoint,
                            api_key=api_key,
                            model=model,
                            timeout=timeout,
                            attempts=attempts,
                            transport_attempts=transport_attempts,
                            pacer=pacer,
                        )[0]
                    except RuntimeError as repair_error:
                        repaired[index] = _degrade_unverifiable_intent(
                            repaired[index], rows[index], validation_error
                        )
                        logger.warning(
                            "event=compatible_llm_intent_degraded model=%s content_id=%s "
                            "reason=%r",
                            model,
                            rows[index]["id"],
                            str(repair_error),
                        )
                items = repaired
            for row, item in zip(rows, items, strict=True):
                _validate_review_item(item, row)
            logger.info(
                "event=compatible_llm_batch_completed model=%s size=%d schema_attempts=%d "
                "transport_attempts=%d elapsed_seconds=%.3f",
                model,
                len(rows),
                schema_attempt + 1,
                transport_attempt + 1,
                monotonic() - started,
            )
            return items
        except CompatibleLLMTransportError as exc:
            last_error = exc
            transport_attempt += 1
            if transport_attempt >= transport_attempts:
                logger.error(
                    "event=compatible_llm_batch_failed model=%s size=%d reason=transport "
                    "attempt=%d elapsed_seconds=%.3f error=%r",
                    model,
                    len(rows),
                    transport_attempt,
                    monotonic() - started,
                    str(exc),
                )
                raise RuntimeError(
                    "compatible LLM transport remained unavailable after bounded retries"
                ) from exc
            delay = min(2 ** min(transport_attempt - 1, 5), 30)
            logger.warning(
                "event=compatible_llm_batch_retry model=%s size=%d reason=transport "
                "attempt=%d max_attempts=%d delay_seconds=%d error=%r",
                model,
                len(rows),
                transport_attempt,
                transport_attempts,
                delay,
                str(exc),
            )
            sleep(delay)
        except (RuntimeError, TypeError, ValueError) as exc:
            last_error = exc
            schema_attempt += 1
            if schema_attempt < attempts:
                delay = min(2 ** (schema_attempt - 1), 4)
                logger.warning(
                    "event=compatible_llm_batch_retry model=%s size=%d reason=schema "
                    "attempt=%d max_attempts=%d delay_seconds=%d error=%r",
                    model,
                    len(rows),
                    schema_attempt,
                    attempts,
                    delay,
                    str(exc),
                )
                sleep(delay)
    content_id = rows[0]["id"] if rows else "unknown"
    logger.error(
        "event=compatible_llm_item_failed model=%s content_id=%s elapsed_seconds=%.3f error=%r",
        model,
        content_id,
        monotonic() - started,
        str(last_error or "unknown error"),
    )
    raise RuntimeError(
        "compatible LLM could not produce a valid review for batch starting at "
        f"content {content_id}: "
        f"{last_error or 'unknown error'}"
    ) from last_error


def _review_contents(
    session: Session,
    *,
    batch_runner: Callable[[list[dict[str, Any]]], BatchReviewResult],
    analysis_model: str,
    analysis_models: tuple[str, ...] | None = None,
    prompt_version_prefix: str,
    reviewer: str,
    batch_size: int,
    limit: int | None,
    since: datetime | None,
    until: datetime | None,
    source_names: set[str] | None,
    candidate_model: str | None,
    candidate_intents: set[str] | None,
    only_unscanned: bool,
    min_content_chars: int,
    max_chars: int,
    shard_count: int,
    shard_index: int,
    progress: Callable[[dict[str, Any]], None] | None,
) -> dict[str, Any]:
    if batch_size < 1 or batch_size > 100:
        raise ValueError("batch_size must be between 1 and 100")
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    if min_content_chars < 0:
        raise ValueError("min_content_chars cannot be negative")
    if max_chars < 400 or max_chars > 20000:
        raise ValueError("max_chars must be between 400 and 20000")
    if shard_count < 1 or shard_count > 32:
        raise ValueError("shard_count must be between 1 and 32")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be between 0 and shard_count - 1")
    if candidate_intents and not candidate_model:
        raise ValueError("candidate_model is required when candidate_intents are used")

    query = select(Content).where(Content.is_deleted.is_(False)).order_by(Content.id)
    if shard_count > 1:
        query = query.where(Content.id % shard_count == shard_index)
    if since is not None:
        query = query.where(Content.published_at >= as_utc(since))
    if until is not None:
        query = query.where(Content.published_at <= as_utc(until))
    if source_names:
        query = query.join(Source, Source.id == Content.source_id).where(
            Source.name.in_(source_names)
        )
    if min_content_chars:
        query = query.where(
            func.length(func.coalesce(Content.title, "")) + func.length(Content.body)
            >= min_content_chars
        )
    if candidate_model and candidate_intents:
        latest_candidate_ids = (
            select(func.max(ContentAnalysis.id))
            .where(ContentAnalysis.model == candidate_model)
            .group_by(ContentAnalysis.content_id)
        )
        query = query.join(
            ContentAnalysis,
            ContentAnalysis.content_id == Content.id,
        ).where(
            ContentAnalysis.id.in_(latest_candidate_ids),
            ContentAnalysis.intent.in_(candidate_intents),
        )
    contents = session.scalars(query).unique().all()
    if only_unscanned:
        eligible_contents = [
            content
            for content in contents
            if not _has_current_external_analysis(session, content)
        ]
    else:
        eligible_contents = contents

    resume_models = tuple(dict.fromkeys(analysis_models or (analysis_model,)))
    existing_versions = {
        (row.content_id, row.prompt_version)
        for row in session.scalars(
            select(ContentAnalysis).where(ContentAnalysis.model.in_(resume_models))
        ).all()
    }
    all_pending = [
        content
        for content in eligible_contents
        if (
            content.id,
            f"{prompt_version_prefix}:{content.content_hash[:12]}",
        )
        not in existing_versions
    ]
    pending = all_pending[:limit] if limit is not None else all_pending

    reviewed = 0
    failed_items: list[dict[str, Any]] = []
    intent_counts = {name: 0 for name in ("buy", "sell", "hold", "wait", "unknown")}
    provider_counts = {model: 0 for model in resume_models}
    for start in range(0, len(pending), batch_size):
        batch = pending[start : start + batch_size]
        logger.info(
            "event=review_batch_started reviewer=%s model=%s offset=%d size=%d total=%d",
            reviewer,
            analysis_model,
            start,
            len(batch),
            len(pending),
        )
        batch_started = monotonic()
        rows = [_input_row(content, max_chars=max_chars) for content in batch]

        def review_isolated(
            isolated_batch: list[Content], isolated_rows: list[dict[str, Any]]
        ) -> list[
            tuple[Content, dict[str, Any], dict[str, Any], AnalysisContract, str]
        ]:
            try:
                runner_result = batch_runner(isolated_rows)
                if isinstance(runner_result, tuple):
                    isolated_items, result_model = runner_result
                else:
                    isolated_items = runner_result
                    result_model = analysis_model
                return [
                    (
                        content,
                        row,
                        item,
                        _validate_review_item(item, row),
                        result_model,
                    )
                    for content, row, item in zip(
                        isolated_batch, isolated_rows, isolated_items, strict=True
                    )
                ]
            except CompatibleLLMTransportError:
                raise
            except (RuntimeError, TypeError, ValueError) as item_error:
                if "transport remained unavailable" in str(item_error):
                    raise
                if len(isolated_batch) > 1:
                    middle = len(isolated_batch) // 2
                    logger.warning(
                        "event=review_batch_isolating_failed_items reviewer=%s model=%s "
                        "size=%d left=%d right=%d error=%r",
                        reviewer,
                        analysis_model,
                        len(isolated_batch),
                        middle,
                        len(isolated_batch) - middle,
                        str(item_error),
                    )
                    return review_isolated(
                        isolated_batch[:middle], isolated_rows[:middle]
                    ) + review_isolated(isolated_batch[middle:], isolated_rows[middle:])
                content = isolated_batch[0]
                failed_items.append({"content_id": content.id, "error": str(item_error)})
                logger.error(
                    "event=review_item_deferred reviewer=%s model=%s content_id=%d error=%r",
                    reviewer,
                    analysis_model,
                    content.id,
                    str(item_error),
                )
                return []

        accepted = review_isolated(batch, rows)
        for content, row, item, contract, result_model in accepted:
            prompt_version = f"{prompt_version_prefix}:{content.content_hash[:12]}"
            analysis = save_content_analysis(
                session,
                content,
                contract,
                model=result_model,
                prompt_version=prompt_version,
                schema_version=SCHEMA_VERSION,
            )
            save_content_analysis_review(
                session,
                analysis,
                reviewer=reviewer,
                input_hash=_input_hash(row),
                intent_basis=str(item["intent_basis"]),
                intent_evidence=str(item["intent_evidence"]),
                rationale=str(item["rationale"]),
                payload=item,
            )
            intent_counts[contract.intent.value] += 1
            provider_counts[result_model] = provider_counts.get(result_model, 0) + 1
            reviewed += 1
        session.commit()
        logger.info(
            "event=review_batch_completed reviewer=%s model=%s reviewed=%d total=%d "
            "elapsed_seconds=%.3f",
            reviewer,
            analysis_model,
            reviewed,
            len(pending),
            monotonic() - batch_started,
        )
        if progress:
            progress(
                {
                    "reviewed": reviewed,
                    "pending_at_start": len(pending),
                    "batch": len(batch),
                    "failed": len(failed_items),
                    "intent_counts": dict(intent_counts),
                    "provider_counts": dict(provider_counts),
                }
            )
    return {
        "model": analysis_model,
        "reviewed": reviewed,
        "already_reviewed": len(contents) - len(all_pending),
        "remaining": max(0, len(all_pending) - reviewed),
        "failed": len(failed_items),
        "failed_items": failed_items,
        "intent_counts": intent_counts,
        "provider_counts": provider_counts,
    }


def review_contents_with_codex(
    session: Session,
    *,
    batch_size: int = 50,
    limit: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    source_names: set[str] | None = None,
    candidate_model: str | None = None,
    candidate_intents: set[str] | None = None,
    only_unscanned: bool = False,
    min_content_chars: int = 0,
    max_chars: int = 2400,
    model: str = CODEX_MODEL,
    timeout: float = 600,
    shard_count: int = 1,
    shard_index: int = 0,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Review contents with the locally authenticated Codex CLI, resumably.

    Database rows are the checkpoint: every content hash gets its own immutable
    prompt version, and each committed batch can be resumed safely.
    """

    analysis_model = f"{model}-via-codex-cli"
    result = _review_contents(
        session,
        batch_runner=lambda rows: _run_codex_batch_resilient(rows, model=model, timeout=timeout),
        analysis_model=analysis_model,
        prompt_version_prefix=PROMPT_VERSION,
        reviewer=CODEX_REVIEWER,
        batch_size=batch_size,
        limit=limit,
        since=since,
        until=until,
        source_names=source_names,
        candidate_model=candidate_model,
        candidate_intents=candidate_intents,
        only_unscanned=only_unscanned,
        min_content_chars=min_content_chars,
        max_chars=max_chars,
        shard_count=shard_count,
        shard_index=shard_index,
        progress=progress,
    )
    return {
        **result,
        "codex_model": model,
        "shard_count": shard_count,
        "shard_index": shard_index,
    }


def review_contents_with_compatible_llm(
    session: Session,
    *,
    endpoint: str,
    api_key: str,
    model: str,
    batch_size: int = 12,
    limit: int | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    source_names: set[str] | None = None,
    candidate_model: str | None = None,
    candidate_intents: set[str] | None = None,
    only_unscanned: bool = False,
    min_content_chars: int = 0,
    max_chars: int = 2400,
    timeout: float = 120,
    min_interval: float = 0.0,
    fallback_endpoint: str | None = None,
    fallback_api_key: str | None = None,
    fallback_model: str | None = None,
    fallback_timeout: float | None = None,
    fallback_min_interval: float = 0.0,
    shard_count: int = 1,
    shard_index: int = 0,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run evidence-backed review through a configured compatible endpoint."""

    if not endpoint or not api_key or not model:
        raise ValueError("compatible LLM endpoint, API key and model are required")
    fallback_values = (fallback_endpoint, fallback_api_key, fallback_model)
    if any(fallback_values) and not all(fallback_values):
        raise ValueError(
            "fallback compatible LLM endpoint, API key and model must be configured together"
        )
    analysis_model = f"{model}-via-openai-compatible"
    fallback_analysis_model = (
        f"{fallback_model}-via-openai-compatible" if all(fallback_values) else None
    )
    primary_pacer = _RequestPacer(min_interval)
    fallback_pacer = _RequestPacer(fallback_min_interval)

    def run_configured_batch(rows: list[dict[str, Any]]) -> BatchReviewResult:
        try:
            items = _run_compatible_batch_resilient(
                rows,
                endpoint=endpoint,
                api_key=api_key,
                model=model,
                timeout=timeout,
                transport_attempts=3 if fallback_analysis_model else 12,
                pacer=primary_pacer,
            )
            return items, analysis_model
        except (CompatibleLLMTransportError, RuntimeError, TypeError, ValueError) as primary_error:
            if fallback_analysis_model is None:
                raise
            logger.warning(
                "event=compatible_llm_failover_started primary_model=%s "
                "fallback_model=%s size=%d error_type=%s error=%r",
                model,
                fallback_model,
                len(rows),
                type(primary_error).__name__,
                str(primary_error),
            )
            try:
                items = _run_compatible_batch_resilient(
                    rows,
                    endpoint=fallback_endpoint or "",
                    api_key=fallback_api_key or "",
                    model=fallback_model or "",
                    timeout=fallback_timeout or timeout,
                    pacer=fallback_pacer,
                )
            except (
                CompatibleLLMTransportError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as fallback_error:
                logger.error(
                    "event=compatible_llm_failover_failed primary_model=%s "
                    "fallback_model=%s size=%d primary_error=%r fallback_error=%r",
                    model,
                    fallback_model,
                    len(rows),
                    str(primary_error),
                    str(fallback_error),
                )
                raise RuntimeError(
                    "both configured compatible LLM providers failed; "
                    f"primary={type(primary_error).__name__}, "
                    f"fallback={type(fallback_error).__name__}"
                ) from fallback_error
            logger.info(
                "event=compatible_llm_failover_completed primary_model=%s "
                "fallback_model=%s size=%d",
                model,
                fallback_model,
                len(rows),
            )
            return items, fallback_analysis_model

    result = _review_contents(
        session,
        batch_runner=run_configured_batch,
        analysis_model=analysis_model,
        analysis_models=tuple(
            item
            for item in (analysis_model, fallback_analysis_model)
            if item is not None
        ),
        prompt_version_prefix=COMPATIBLE_PROMPT_VERSION,
        reviewer=COMPATIBLE_REVIEWER,
        batch_size=batch_size,
        limit=limit,
        since=since,
        until=until,
        source_names=source_names,
        candidate_model=candidate_model,
        candidate_intents=candidate_intents,
        only_unscanned=only_unscanned,
        min_content_chars=min_content_chars,
        max_chars=max_chars,
        shard_count=shard_count,
        shard_index=shard_index,
        progress=progress,
    )
    return {
        **result,
        "provider_model": model,
        "fallback_provider_model": fallback_model,
        "failover_enabled": fallback_analysis_model is not None,
        "shard_count": shard_count,
        "shard_index": shard_index,
    }
