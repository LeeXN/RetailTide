from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import secrets
import shlex
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import select

from .config import (
    DEFAULT_ENABLED_SOURCES,
    DEFAULT_PUBLIC_SOURCES,
    PUBLIC_SOURCES,
    SOURCE_ENV_PREFIXES,
    SUPPLEMENT_SOURCES,
    compliant_http_user_agent,
    get_settings,
    llm_config_status,
    market_config_status,
    source_config_status,
)
from .db import init_db, make_engine, session_factory
from .jobs import (
    backfill_active_topics,
    collect_active_topics,
    collect_source,
    enrich_common_crawl,
    resolve_incremental_window,
    run_core_pipeline,
)
from .market import provider_for_name, sync_market
from .models import Asset, AssetTopic, Source, Topic
from .observability import configure_logging
from .pipeline import (
    EntityResolver,
    aggregate_trend_signals,
    analysis_task_summary,
    analyze_pending,
    normalize_pending,
    resolve_pending_entities,
)
from .pipeline.codex_review import (
    CODEX_MODEL,
    review_contents_with_codex,
    review_contents_with_compatible_llm,
)
from .pipeline.events import detect_events
from .pipeline.metrics import aggregate_metrics
from .pipeline.rebuild import (
    import_verified_raw_history,
    remove_misnormalized_zhihu_snapshots,
    reset_metric_event_derivatives,
)
from .pipeline.returns import evaluate_events
from .pipeline.timestamps import publication_time_audit
from .registry import sync_registry
from .research import event_study, quantile_study
from .source_sessions import (
    SourceSessionError,
    delete_source_session,
    import_source_session,
    source_session_status,
)
from .sources import source_for_name
from .time import (
    SHANGHAI,
    as_utc,
    now_utc,
    parse_collection_bound,
    parse_datetime,
    resolve_collection_window,
    scheduled_post_window,
)

app = typer.Typer(help="RetailTide: retail-investor observation and event study CLI.")
registry_app = typer.Typer(help="Topic and asset registry")
source_app = typer.Typer(help="Observation source operations")
source_auth_app = typer.Typer(help="Authorized browser-session operations")
market_app = typer.Typer(help="Market data and calendar operations")
llm_app = typer.Typer(help="LLM analysis configuration")
event_app = typer.Typer(help="Signal event operations")
research_app = typer.Typer(help="Reproducible research studies")
demo_app = typer.Typer(help="Local deterministic end-to-end demo")
app.add_typer(registry_app, name="registry", hidden=True)
app.add_typer(source_app, name="source", hidden=True)
source_app.add_typer(source_auth_app, name="auth")
app.add_typer(market_app, name="market", hidden=True)
app.add_typer(llm_app, name="llm", hidden=True)
app.add_typer(event_app, name="event", hidden=True)
app.add_typer(research_app, name="research", hidden=True)
app.add_typer(demo_app, name="demo", hidden=True)


def _json(value):
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2, default=str))


@app.callback()
def main(
    log_level: str | None = typer.Option(
        None,
        "--log-level",
        help="Log level: DEBUG, INFO, WARNING, ERROR or CRITICAL.",
    ),
):
    """Configure process-wide logs before running a command."""

    settings = get_settings()
    try:
        configure_logging(log_level or settings.log_level)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--log-level") from exc


logger = logging.getLogger(__name__)


def _backfill_days(
    since: str | None,
    until: str | None,
    days: int | None,
) -> int | None:
    """Return an explicit history duration; automatic collection owns defaults."""

    return days


def _apply_date_range(
    date_range: str | None,
    since: str | None,
    until: str | None,
    days: int | None,
) -> tuple[str | None, str | None, int | None]:
    if not date_range:
        return since, until, days
    if since is not None or until is not None or days is not None:
        raise ValueError("--range cannot be combined with --since, --until or --days")
    match = re.fullmatch(r"\s*(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\s*", date_range)
    if match is None:
        raise ValueError("--range must use YYYY-MM-DD-YYYY-MM-DD")
    return match.group(1), match.group(2), None


def _apply_single_date(
    single_date: str | None,
    since: str | None,
    until: str | None,
    date_range: str | None,
    days: int | None,
) -> tuple[str | None, str | None, str | None, int | None]:
    """Expand ``--date`` into one inclusive Shanghai calendar day."""

    if not single_date:
        return since, until, date_range, days
    if since is not None or until is not None or date_range is not None or days is not None:
        raise ValueError("--date cannot be combined with --since, --until, --range or --days")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", single_date.strip()) is None:
        raise ValueError("--date must use YYYY-MM-DD")
    return single_date.strip(), single_date.strip(), None, None


def _cli_collection_bounds(
    since: str | None,
    until: str | None,
    days: int | None,
    *,
    current: datetime | None = None,
) -> tuple[datetime | None, datetime | None, int | None]:
    """Parse date-only CLI bounds; today's inclusive end is clamped to now."""

    start = parse_collection_bound(since)
    end = parse_collection_bound(until, end=True)
    current = current or now_utc()
    if days is not None:
        if since is not None or until is not None:
            raise ValueError("days cannot be combined with since or until")
        local_today = current.astimezone(SHANGHAI).date()
        first_day = local_today - timedelta(days=days - 1)
        start = datetime.combine(first_day, datetime.min.time(), tzinfo=SHANGHAI).astimezone(
            current.tzinfo
        )
        end = current
        return start, end, None
    if end is not None and end > current:
        if until and len(until.strip()) == 10:
            local_today = current.astimezone(SHANGHAI).date().isoformat()
            if until.strip() == local_today:
                end = current
            else:
                raise ValueError("until cannot be in the future")
        else:
            raise ValueError("until cannot be in the future")
    return start, end, days


@contextmanager
def _exclusive_refresh_lock(settings):
    """Prevent manual and scheduled runs from multiplying source/LLM traffic."""

    path = Path(settings.run_lock_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another RetailTide refresh is already running (lock: {path})"
            ) from exc
        yield path
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _session():
    settings = get_settings()
    engine = init_db(make_engine(settings))
    return settings, engine, session_factory(engine)()


def _collection_summary(results: list[dict]) -> dict:
    degraded = [
        {
            "topic_slug": row.get("topic_slug"),
            "source": row.get("source"),
            "error": row.get("error"),
        }
        for row in results
        if row.get("source_degraded")
    ]
    partial = [
        {
            "topic_slug": row.get("topic_slug"),
            "source": row.get("source"),
            "warnings": row.get("warnings") or [],
        }
        for row in results
        if row.get("source_partial")
    ]
    deferred = [
        {
            "topic_slug": row.get("topic_slug"),
            "source": row.get("source"),
            "reason": row.get("skip_reason"),
            "resume": row.get("resume"),
        }
        for row in results
        if row.get("collection_skipped") and row.get("skip_reason") != "checkpoint_complete"
    ]
    return {
        "topic_count": len({row.get("topic_id") for row in results if row.get("topic_id")}),
        "source_count": len({row.get("source") for row in results if row.get("source")}),
        "attempt_count": len(results),
        "items_collected": sum(int(row.get("items_collected") or 0) for row in results),
        "duplicates": sum(int(row.get("duplicates") or 0) for row in results),
        "topic_links_added": sum(int(row.get("topic_links_added") or 0) for row in results),
        "degraded": degraded,
        "partial": partial,
        "skipped_completed": sum(
            row.get("skip_reason") == "checkpoint_complete" for row in results
        ),
        "deferred": deferred,
        "results": results,
    }


def _collection_resume_key(
    source_names: list[str],
    *,
    since: str | None,
    until: str | None,
    date_range: str | None,
    days: int | None,
    single_date: str | None = None,
    topic_slugs: list[str] | None = None,
    current: datetime | None = None,
) -> str | None:
    """Return a stable key for retries of one explicit CLI collection window."""

    if (
        since is None
        and until is None
        and date_range is None
        and days is None
        and single_date is None
    ):
        return None
    current = current or now_utc()
    local_today = current.astimezone(SHANGHAI).date().isoformat()
    relative_window = (
        days is not None
        or single_date == local_today
    )
    payload = {
        "sources": sorted(name.lower().replace("_", "-") for name in source_names),
        "since": since,
        "until": until,
        "range": date_range,
        "days": days,
        "date": single_date,
        "topics": sorted(topic_slugs or []),
        # Rolling day windows and a request for "today" get a daily identity.
        # A since-only historical backfill intentionally does not: its end is
        # frozen in the checkpoint created by the first invocation, so a long
        # low-frequency import can resume across dates instead of starting
        # over every midnight. Daily refresh owns newly closed dates.
        "relative_anchor": local_today if relative_window else None,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _backfill_collection_summary(collection: dict) -> dict:
    rows = list(collection.get("jobs") or [])
    return {
        "mode": "bounded-history",
        "complete": bool(collection.get("completed"))
        and not collection.get("terminal_jobs"),
        "state_file": collection.get("state_file"),
        "job_counts": {
            "total": len(rows),
            "done": sum(bool(row.get("done")) for row in rows),
            "terminal": len(collection.get("terminal_jobs") or []),
            "deferred": len(collection.get("deferred_jobs") or []),
            "pending": int(collection.get("pending_jobs") or 0),
        },
        "items_collected": sum(int(row.get("items_collected") or 0) for row in rows),
        "duplicates": sum(int(row.get("duplicates") or 0) for row in rows),
        "topic_links_added": sum(int(row.get("topic_links_added") or 0) for row in rows),
        "degraded": list(collection.get("degraded") or []),
        "partial": list(collection.get("partial") or []),
        "terminal": list(collection.get("terminal_jobs") or []),
        "deferred": list(collection.get("deferred_jobs") or []),
    }


def _bounded_collection_for_sources(
    collection: dict,
    source_names: list[str] | tuple[str, ...],
) -> dict:
    """Project one shared checkpoint result onto required or supplemental sources."""

    selected = {source.lower().replace("_", "-") for source in source_names}
    rows = [row for row in collection.get("jobs") or [] if row.get("source") in selected]
    terminal = [row for row in rows if row.get("terminal")]
    deferred = [
        row
        for row in rows
        if row.get("deferred_reason") and not (row.get("done") or row.get("terminal"))
    ]
    return {
        **collection,
        "completed": all(row.get("done") or row.get("terminal") for row in rows),
        "pending_jobs": sum(not (row.get("done") or row.get("terminal")) for row in rows),
        "terminal_jobs": terminal,
        "deferred_jobs": deferred,
        "degraded": [row for row in rows if row.get("error") and not row.get("partial")],
        "partial": [row for row in rows if row.get("partial")],
        "jobs": rows,
    }


def _bounded_collection_blockers(collection: dict) -> list[dict]:
    return [
        row
        for row in collection.get("jobs") or []
        if not row.get("done") and not row.get("terminal")
    ]


def _bounded_collection_has_immediate_work(collection: dict) -> bool:
    current = now_utc()
    for row in collection.get("jobs") or []:
        if row.get("done") or row.get("terminal") or row.get("deferred_reason"):
            continue
        retry_value = row.get("next_retry_at")
        if retry_value:
            try:
                retry_at = as_utc(datetime.fromisoformat(str(retry_value)))
            except ValueError:
                retry_at = None
            if retry_at is not None and retry_at > current:
                continue
        return True
    return False


def _collect_bounded_until_blocked(
    session,
    *,
    source_names: list[str],
    since: datetime,
    until: datetime,
    settings,
    topic_slugs: set[str],
    state_path: Path,
) -> dict:
    """Run fair, checkpointed batches until complete or a cooldown blocks progress."""

    latest: dict | None = None
    while True:
        latest = backfill_active_topics(
            session,
            source_names=source_names,
            since=since,
            until=until,
            settings=settings,
            topic_slugs=topic_slugs,
            state_path=state_path,
            batch_pages=5,
            default_page_limit=1500,
            cooldown_seconds=0,
            max_retries=6,
            one_batch_per_job=True,
            # A checkpoint can resume in the middle of the flattened strategy
            # list. Without a per-source bound, hundreds of healthy XHS
            # strategies after that cursor can starve pending Guba/Taoguba
            # jobs for hours before the circular scan wraps around.
            max_jobs_per_source=1,
            source_concurrency=max(1, int(getattr(settings, "source_concurrency", 1))),
        )
        blockers = _bounded_collection_blockers(latest)
        if not blockers:
            return latest
        # Keep rotating while at least one source can make immediate progress.
        # A degraded source must not force healthy sources to stop, while its
        # durable retry timestamp still prevents another request in this run.
        if not latest.get("attempted_jobs") or not _bounded_collection_has_immediate_work(
            latest
        ):
            return latest


def _analysis_is_complete(pipeline: dict) -> bool:
    if pipeline.get("analysis_deferred"):
        return False
    tasks = pipeline.get("analysis_tasks") or {}
    return not any(
        int(tasks.get(key) or 0)
        for key in ("failed", "pending", "untracked", "retry_ready", "retry_deferred")
    )


def _sync_topic_market(session, settings, *, end: date, days: int = 120) -> dict:
    """Best-effort representative market sync used by the one-click refresh."""

    status = market_config_status(settings)
    if not status["configured"]:
        return {**status, "assets": [], "errors": []}
    provider = provider_for_name(
        settings.market_provider,
        endpoint=settings.market_endpoint,
        headers=settings.market_headers(),
    )
    assets = session.scalars(
        select(Asset)
        .join(AssetTopic, AssetTopic.asset_id == Asset.id)
        .distinct()
        .order_by(Asset.market, Asset.symbol)
    ).all()
    start = end - timedelta(days=days)
    synced = []
    errors = []
    for asset in assets:
        try:
            inserted = sync_market(session, asset, start, end, provider=provider)
            synced.append(
                {
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "inserted": inserted,
                }
            )
        except (TypeError, ValueError) as exc:
            session.rollback()
            errors.append({"symbol": asset.symbol, "name": asset.name, "error": str(exc)})
    return {
        **status,
        "from": start,
        "to": end,
        "assets": synced,
        "errors": errors,
    }


SOURCE_SETUP_GUIDANCE = {
    "guba": ("使用东方财富公开只读页面，无需 API key。"),
    "taoguba": ("使用淘股吧公开只读关键词搜索，无需 API key。"),
    "zhihu": (
        "官方搜索 endpoint 已内置；打开 https://developer.zhihu.com/ → 鉴权 → "
        "个人中心，仅复制 Access Secret。"
    ),
    "xiaohongshu": (
        "连接项目自有的 xpzouying/xiaohongshu-mcp；Cookie 和扫码登录留在 MCP 内，"
        "RetailTide 只调用搜索、列表和详情读取接口。"
    ),
}

LLM_SETUP_GUIDANCE = (
    "主模型和备用模型均支持 OpenAI-compatible chat-completions 接口；"
    "分别填写所选服务商提供的 base URL、API key 和 model，不绑定具体厂商或模型。"
)

SOURCE_LABELS = {
    "guba": "东方财富股吧",
    "taoguba": "淘股吧",
    "zhihu": "知乎",
    "xiaohongshu": "小红书",
    "common-crawl": "Common Crawl 归档",
    "wikimedia-pageviews": "Wikimedia Pageviews",
}

# Guba and Taoguba are built-in, no-key P0 sources. Setup only asks about the
# optional sources whose official transports require credentials.
SETUP_CONFIGURABLE_SOURCES = ("zhihu", "xiaohongshu")

# Setup only visits the optional adapters, both of which require an authorized
# endpoint and credential. The built-in P0 collectors bypass this prompt.
SOURCE_REQUIRES_KEY = {name: name in {"zhihu"} for name in SOURCE_ENV_PREFIXES}


def _read_env_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        if not separator or not key.isidentifier():
            continue
        try:
            tokens = shlex.split(raw_value, comments=False)
            values[key] = tokens[0] if tokens else ""
        except ValueError:
            values[key] = raw_value.strip().strip('"').strip("'")
    return values


def _env_current(values: dict[str, str], key: str, default: str = "") -> str:
    return values.get(key, os.getenv(key, default))


def _secret_prompt(label: str, current: str) -> str:
    answer = typer.prompt(
        f"{label}（隐藏输入，回车保留现值）", default="", show_default=False, hide_input=True
    )
    return answer or current


def _text_prompt(label: str, current: str = "") -> str:
    if current:
        return typer.prompt(label, default=current, show_default=True).strip()
    return typer.prompt(label, default="", show_default=False).strip()


def _source_has_existing_config(values: dict[str, str], name: str) -> bool:
    prefix = SOURCE_ENV_PREFIXES[name]
    if name == "zhihu":
        return bool(
            _env_current(values, "RETAIL_TIDE_ZHIHU_ACCESS_TOKEN")
            or os.getenv("ZHIHU_ACCESS_SECRET")
        )
    return any(
        _env_current(values, f"RETAIL_TIDE_{prefix}_{suffix}")
        for suffix in ("ENDPOINT", "API_KEY", "ACCESS_TOKEN")
    )


def _clear_llm_fallback(values: dict[str, str]) -> None:
    for key in (
        "RETAIL_TIDE_LLM_FALLBACK_PROVIDER",
        "RETAIL_TIDE_LLM_FALLBACK_BASE_URL",
        "RETAIL_TIDE_LLM_FALLBACK_API_KEY",
        "RETAIL_TIDE_LLM_FALLBACK_MODEL",
    ):
        values[key] = ""


def _configure_llm_fallback(values: dict[str, str]) -> bool:
    provider_key = "RETAIL_TIDE_LLM_FALLBACK_PROVIDER"
    base_url_key = "RETAIL_TIDE_LLM_FALLBACK_BASE_URL"
    api_key_key = "RETAIL_TIDE_LLM_FALLBACK_API_KEY"
    model_key = "RETAIL_TIDE_LLM_FALLBACK_MODEL"
    has_existing = any(_env_current(values, key) for key in (
        provider_key,
        base_url_key,
        api_key_key,
        model_key,
    ))
    if not typer.confirm("是否配置备用 LLM？", default=has_existing):
        _clear_llm_fallback(values)
        return False

    existing_provider = _env_current(values, provider_key, "openai-compatible").lower()
    while True:
        provider = _text_prompt(
            "备用 LLM provider（openai 或 openai-compatible）",
            existing_provider,
        ).lower()
        if provider in {"openai", "openai-compatible"}:
            break
        typer.echo("请输入 openai 或 openai-compatible。")
    values[provider_key] = provider
    default_base_url = "https://api.openai.com/v1" if provider == "openai" else ""
    values[base_url_key] = _text_prompt(
        "备用 LLM base URL",
        _env_current(values, base_url_key, default_base_url),
    )
    values[api_key_key] = _secret_prompt(
        "备用 LLM API key", _env_current(values, api_key_key)
    )
    values[model_key] = _text_prompt(
        "备用 LLM model", _env_current(values, model_key)
    )
    return bool(values[base_url_key] and values[api_key_key] and values[model_key])


def _configure_llm(values: dict[str, str]) -> bool:
    typer.echo("\n[llm] 内容分析（默认配置）")
    typer.echo(LLM_SETUP_GUIDANCE)
    provider_key = "RETAIL_TIDE_LLM_PROVIDER"
    base_url_key = "RETAIL_TIDE_LLM_BASE_URL"
    api_key_key = "RETAIL_TIDE_LLM_API_KEY"
    model_key = "RETAIL_TIDE_ANALYSIS_MODEL"
    existing_provider = _env_current(values, provider_key, "openai").lower()
    if not typer.confirm("现在配置 LLM 吗？", default=True):
        values[provider_key] = "rule-based"
        values[model_key] = "rule-based-v0"
        _clear_llm_fallback(values)
        return False

    while True:
        provider = _text_prompt(
            "LLM provider（openai、openai-compatible 或 rule-based）", existing_provider
        ).lower()
        if provider in {"openai", "openai-compatible", "rule-based"}:
            break
        typer.echo("请输入 openai、openai-compatible 或 rule-based。")
    values[provider_key] = provider
    if provider == "rule-based":
        values[model_key] = "rule-based-v0"
        _clear_llm_fallback(values)
        return True

    default_base_url = "https://api.openai.com/v1" if provider == "openai" else ""
    values[base_url_key] = _env_current(values, base_url_key, default_base_url)
    if not values[base_url_key]:
        values[base_url_key] = _text_prompt("LLM base URL", "")
    existing_key = _env_current(values, api_key_key)
    values[api_key_key] = _secret_prompt("LLM API key", existing_key)
    existing_model = _env_current(values, model_key)
    if existing_model in {"", "rule-based-v0"}:
        existing_model = "gpt-5"
    values[model_key] = _text_prompt("LLM model", existing_model)
    primary_ready = bool(values[base_url_key] and values[api_key_key] and values[model_key])
    fallback_requested = _configure_llm_fallback(values)
    if any(
        _env_current(values, key)
        for key in (
            "RETAIL_TIDE_LLM_FALLBACK_PROVIDER",
            "RETAIL_TIDE_LLM_FALLBACK_BASE_URL",
            "RETAIL_TIDE_LLM_FALLBACK_API_KEY",
            "RETAIL_TIDE_LLM_FALLBACK_MODEL",
        )
    ) and not fallback_requested:
        typer.echo("备用 LLM 配置不完整；主模型仍可使用，retail-tide status 会列出缺项。")
    return primary_ready


def _configure_source(values: dict[str, str], name: str) -> bool:
    label = SOURCE_LABELS[name]
    prefix = SOURCE_ENV_PREFIXES[name]
    typer.echo(f"\n[{label}]")
    typer.echo(SOURCE_SETUP_GUIDANCE[name])
    endpoint_key = f"RETAIL_TIDE_{prefix}_ENDPOINT"
    api_key_key = f"RETAIL_TIDE_{prefix}_API_KEY"
    access_token_key = f"RETAIL_TIDE_{prefix}_ACCESS_TOKEN"
    api_secret_key = f"RETAIL_TIDE_{prefix}_API_SECRET"
    auth_header_key = f"RETAIL_TIDE_{prefix}_AUTH_HEADER"
    has_existing = _source_has_existing_config(values, name)
    if not typer.confirm(f"是否配置{label}？", default=has_existing):
        return False

    if name == "xiaohongshu":
        values[endpoint_key] = _text_prompt(
            "xiaohongshu-mcp 服务地址",
            _env_current(values, endpoint_key, "http://127.0.0.1:18060"),
        ).removesuffix("/mcp")
        values[api_key_key] = ""
        values[access_token_key] = ""
        values[api_secret_key] = ""
        values[auth_header_key] = "X-API-Key"
        return bool(values[endpoint_key])

    if not SOURCE_REQUIRES_KEY[name]:
        typer.echo("该来源无需 API key，跳过凭据配置。")
        return True

    if name == "zhihu":
        values[endpoint_key] = ""
        values[access_token_key] = _secret_prompt(
            "Access Secret",
            _env_current(values, access_token_key, os.getenv("ZHIHU_ACCESS_SECRET", "")),
        )
        values[api_key_key] = ""
        values[api_secret_key] = ""
        values[auth_header_key] = "X-API-Key"
        return bool(values[access_token_key])

    values[endpoint_key] = _text_prompt("接口地址", _env_current(values, endpoint_key))
    auth_kind = (
        "access_token" if _env_current(values, access_token_key) or name == "zhihu" else "api_key"
    )
    if auth_kind == "api_key":
        values[api_key_key] = _secret_prompt("API key", _env_current(values, api_key_key))
        values[access_token_key] = ""
        values[auth_header_key] = _env_current(values, auth_header_key, "X-API-Key")
    else:
        values[access_token_key] = _secret_prompt(
            "Access Secret / token", _env_current(values, access_token_key)
        )
        values[api_key_key] = ""
        values[auth_header_key] = "X-API-Key"
    # API secret is not part of the common setup path. Preserve an existing
    # value for providers that already use it; new custom schemes can still be
    # added to the dotenv file explicitly without making every setup longer.
    values[api_secret_key] = _env_current(values, api_secret_key)
    return bool(values[endpoint_key] and (values[api_key_key] or values[access_token_key]))


def _configure_market(values: dict[str, str]) -> bool:
    typer.echo("\n[market] 必需（事件回报和 Event Study 使用）")
    typer.echo(
        "当前版本没有绑定某一家行情产品，也不存在可代填的通用行情 key 申请页。"
        "只有在你已选定一个允许服务端 API 调用的行情产品后，才填写它提供的日线"
        "JSON endpoint 和 key/token；接口需接受 symbol/market/from/to/interval，并在"
        "bars/items/data/results 下返回 K 线。"
    )
    endpoint_key = "RETAIL_TIDE_MARKET_ENDPOINT"
    api_key_key = "RETAIL_TIDE_MARKET_API_KEY"
    access_token_key = "RETAIL_TIDE_MARKET_ACCESS_TOKEN"
    auth_header_key = "RETAIL_TIDE_MARKET_AUTH_HEADER"
    values["RETAIL_TIDE_MARKET_PROVIDER"] = "http-json"
    values[endpoint_key] = _text_prompt(
        "选定行情产品的 JSON endpoint（不是行情网页地址）",
        _env_current(values, endpoint_key),
    )
    current_kind = "access_token" if _env_current(values, access_token_key) else "api_key"
    while True:
        auth_kind = _text_prompt("行情认证类型（api_key 或 access_token）", current_kind).lower()
        if auth_kind in {"api_key", "access_token"}:
            break
        typer.echo("请输入 api_key 或 access_token。")
    if auth_kind == "api_key":
        values[api_key_key] = _secret_prompt("行情 API key", _env_current(values, api_key_key))
        values[access_token_key] = ""
        values[auth_header_key] = _text_prompt(
            "行情 API key 请求头名称", _env_current(values, auth_header_key, "X-API-Key")
        )
    else:
        values[access_token_key] = _secret_prompt(
            "行情 access token", _env_current(values, access_token_key)
        )
        values[api_key_key] = ""
        values[auth_header_key] = "X-API-Key"
    return bool(values[endpoint_key] and (values[api_key_key] or values[access_token_key]))


def _quote_env(value: str) -> str:
    return shlex.quote(value) if value else ""


def _write_env_file(path: Path, updates: dict[str, str]) -> None:
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = dict(updates)
    output: list[str] = []
    for line in existing_lines:
        stripped = line.strip()
        candidate = stripped[7:].lstrip() if stripped.startswith("export ") else stripped
        key, separator, _value = candidate.partition("=")
        if separator and key in remaining:
            output.append(f"{key}={_quote_env(remaining.pop(key))}")
        else:
            output.append(line)
    if not output:
        output = ["# RetailTide live configuration generated by retail-tide setup", ""]
    if remaining:
        if output and output[-1] != "":
            output.append("")
        output.extend(f"{key}={_quote_env(value)}" for key, value in remaining.items())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


@app.command("setup")
def setup(
    env_file: Path = typer.Option(".env", "--env-file", help="写入配置的 dotenv 文件"),  # noqa: B008
    force: bool = typer.Option(False, "--force", help="不询问是否覆盖已有配置文件"),
    with_market: bool = typer.Option(
        False,
        "--with-market",
        help="同时配置行情 provider；默认 setup 不询问行情",
    ),
):
    """Interactively configure each live source and the LLM credentials."""
    path = env_file.expanduser()
    if (
        path.exists()
        and not force
        and not typer.confirm(f"配置文件 {path} 已存在，继续更新吗？", default=True)
    ):
        raise typer.Abort()
    values = _read_env_values(path)
    typer.echo("RetailTide live 数据配置向导")
    typer.echo("凭据只写入本地配置文件，不会显示在终端，也不会写入数据库。")
    typer.echo("东方财富股吧和淘股吧会默认启用；下面只询问可选来源和公共 API 身份。")

    values["RETAIL_TIDE_DATA_MODE"] = "live"
    current_database = _env_current(
        values, "RETAIL_TIDE_DATABASE_URL", "sqlite:///retail-tide.db"
    )
    values["RETAIL_TIDE_DATABASE_URL"] = current_database

    current_secret = _env_current(values, "RETAIL_TIDE_AUTHOR_HMAC_SECRET")
    if current_secret in {"", "replace-this-in-production", "retail-tide-development-secret"}:
        current_secret = secrets.token_urlsafe(32)
    values["RETAIL_TIDE_AUTHOR_HMAC_SECRET"] = current_secret

    selected_sources = list(DEFAULT_ENABLED_SOURCES) + [
        source_name
        for source_name in SETUP_CONFIGURABLE_SOURCES
        if _configure_source(values, source_name)
    ]
    user_agent_key = "RETAIL_TIDE_HTTP_USER_AGENT"
    existing_user_agent = _env_current(values, user_agent_key)
    if existing_user_agent or typer.confirm(
        "是否配置 Wikimedia 的 User-Agent 联系方式？（也供显式启用 Common Crawl 使用）",
        default=True,
    ):
        values[user_agent_key] = _text_prompt(
            "公共 API User-Agent（需包含项目名和邮箱或 URL）",
            existing_user_agent,
        )
    public_ready = compliant_http_user_agent(values.get(user_agent_key))
    if public_ready:
        selected_sources.extend(DEFAULT_PUBLIC_SOURCES)
    values["RETAIL_TIDE_ENABLED_SOURCES"] = ",".join(selected_sources)
    llm_ready = _configure_llm(values)
    market_ready = _configure_market(values) if with_market else True
    _write_env_file(path, values)

    missing_sources = []
    for source_name in selected_sources:
        if source_name in DEFAULT_ENABLED_SOURCES:
            continue
        if source_name in PUBLIC_SOURCES:
            if not public_ready:
                missing_sources.append(source_name)
            continue
        if source_name == "zhihu":
            if not (
                values.get("RETAIL_TIDE_ZHIHU_ACCESS_TOKEN", "") or os.getenv("ZHIHU_ACCESS_SECRET")
            ):
                missing_sources.append(source_name)
            continue
        if source_name == "xiaohongshu":
            if not values.get("RETAIL_TIDE_XIAOHONGSHU_ENDPOINT", ""):
                missing_sources.append(source_name)
            continue
        prefix = SOURCE_ENV_PREFIXES[source_name]
        endpoint = values.get(f"RETAIL_TIDE_{prefix}_ENDPOINT", "")
        credential = values.get(f"RETAIL_TIDE_{prefix}_API_KEY", "") or values.get(
            f"RETAIL_TIDE_{prefix}_ACCESS_TOKEN", ""
        )
        if not (endpoint and credential):
            missing_sources.append(source_name)
    llm_provider = values.get("RETAIL_TIDE_LLM_PROVIDER", "rule-based")
    missing = missing_sources + (["llm"] if llm_provider != "rule-based" and not llm_ready else [])
    if with_market and not market_ready:
        missing.append("market")
    typer.echo(f"\n已写入 {path}（权限已设为 600）。")
    if path != Path(".env"):
        typer.echo(f"启动前请设置：export RETAIL_TIDE_ENV_FILE={path}")
    if missing:
        typer.echo("仍缺少已启用配置：" + ", ".join(missing))
        typer.echo("请重新运行 retail-tide setup，或执行 retail-tide status 查看缺项。")
    else:
        typer.echo("配置已写入。现在可以运行 retail-tide status，然后用 refresh 进行首次回填。")
    if not with_market:
        typer.echo("行情配置本次未修改；需要事件回报时再运行 retail-tide setup --with-market。")


@app.command("serve")
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind address for the local dashboard"),
    port: int = typer.Option(8000, min=1, max=65535, help="Bind port for the local dashboard"),
):
    """Start the API and the built-in browser dashboard."""
    import uvicorn

    typer.echo(f"RetailTide dashboard: http://{host}:{port}/")
    uvicorn.run("retail_tide.api.app:app", host=host, port=port, reload=False)


@registry_app.command("sync")
def registry_sync():
    """Create or update canonical Topics, Assets, aliases, links and sources."""
    settings, _engine, session = _session()
    try:
        _json(
            sync_registry(
                session,
                settings.config_dir,
                enabled_sources=settings.enabled_sources,
                collector_version=settings.collector_version,
            )
        )
    finally:
        session.close()


@source_app.command("list")
def source_list():
    """List source admission and health state."""
    settings, _engine, session = _session()
    try:
        _json(
            [
                {
                    "id": row.id,
                    "name": row.name,
                    "source_type": row.source_type,
                    "enabled": row.enabled,
                    "health_status": row.health_status,
                    "collector_version": row.collector_version,
                    "configuration": source_config_status(row.name, settings=settings),
                }
                for row in session.scalars(select(Source).order_by(Source.name)).all()
            ]
        )
    finally:
        session.close()


@source_app.command("check")
def source_check(
    name: str | None = typer.Argument(None),
    all_sources: bool = typer.Option(False, "--all", help="同时查看未启用的可选来源"),
):
    """Check source endpoints and credentials without making network requests."""
    settings = get_settings()
    names = (
        [name.lower().replace("_", "-")]
        if name
        else [*SOURCE_ENV_PREFIXES, *PUBLIC_SOURCES]
        if all_sources
        else list(settings.enabled_sources)
    )
    results = [source_config_status(source_name, settings=settings) for source_name in names]
    _json(results if name is None else results[0])
    if any(
        not bool(item["configured"]) and (name is not None or bool(item["required"]))
        for item in results
    ):
        raise typer.Exit(code=1)


@app.command("status")
def status():
    """Show source and LLM readiness without exposing credentials."""

    settings = get_settings()
    source_rows = []
    for name in settings.enabled_sources:
        supplemental = name in SUPPLEMENT_SOURCES
        source_rows.append(
            {
                **source_config_status(name, settings=settings),
                "pipeline_role": "supplement" if supplemental else "required-content",
                "blocks_llm": not supplemental,
            }
        )
    _json(
        {
            "mode": settings.data_mode,
            "enabled_sources": list(settings.enabled_sources),
            "sources": source_rows,
            "source_concurrency": settings.source_concurrency,
            "llm": llm_config_status(settings),
            "market": market_config_status(settings),
            "run_lock_file": str(settings.run_lock_file),
        }
    )


def _source_auth_path(name: str, override: Path | None) -> tuple[str, Path]:
    normalized = name.lower().replace("_", "-")
    settings = get_settings()
    try:
        return normalized, override or settings.source_session_file(normalized)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="name") from exc


@source_auth_app.command("login")
def source_auth_login(
    name: str,
    from_file: Annotated[
        Path,
        typer.Option(
            "--from-file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="已登录浏览器导出的 cURL/Cookie 或 storage-state JSON 文件",
        ),
    ],
    session_file: Annotated[
        Path | None,
        typer.Option(
            "--session-file",
            help="覆盖安全会话保存路径；默认读取 RETAIL_TIDE_TAOGUBA_SESSION_FILE",
        ),
    ] = None,
):
    """Import a manually authorized browser session without printing its secrets."""

    normalized, output_path = _source_auth_path(name, session_file)
    try:
        _json(import_source_session(normalized, from_file, output_path))
    except SourceSessionError as exc:
        raise typer.BadParameter(str(exc), param_hint="--from-file") from exc


@source_auth_app.command("status")
def source_auth_status(
    name: str,
    session_file: Annotated[Path | None, typer.Option("--session-file")] = None,
):
    """Report session readiness without exposing cookies or file contents."""

    normalized, target = _source_auth_path(name, session_file)
    status = source_session_status(normalized, target)
    _json(status)
    if not status["configured"]:
        raise typer.Exit(code=1)


@source_auth_app.command("logout")
def source_auth_logout(
    name: str,
    session_file: Annotated[Path | None, typer.Option("--session-file")] = None,
):
    """Delete the local browser session; the remote account is unchanged."""

    normalized, target = _source_auth_path(name, session_file)
    try:
        _json(delete_source_session(normalized, target))
    except SourceSessionError as exc:
        raise typer.BadParameter(str(exc), param_hint="--session-file") from exc


@llm_app.command("check")
def llm_check():
    """Check LLM provider configuration without making a network request."""
    status = llm_config_status(get_settings())
    _json(status)
    if not status["configured"]:
        raise typer.Exit(code=1)


@source_app.command("probe")
def source_probe(name: str):
    """Run the source admission probe; Zhihu is discovery-only if top-results-only."""
    settings = get_settings()
    normalized_name = name.lower().replace("_", "-")
    kwargs = {
        "credential": settings.source_credential(normalized_name),
        "use_fixture": settings.data_mode == "demo",
    }
    interval = settings.request_interval(normalized_name)
    if normalized_name in {"common-crawl", "wikimedia-pageviews"}:
        kwargs.update(user_agent=settings.http_user_agent, min_interval=interval)
    elif normalized_name in {"guba", "taoguba"}:
        kwargs["min_public_interval"] = interval
        kwargs["session_file"] = settings.source_session_file(normalized_name)
    elif normalized_name == "xiaohongshu":
        kwargs["min_request_interval"] = interval
    elif normalized_name == "zhihu":
        kwargs["min_public_interval"] = interval
    collector = source_for_name(normalized_name, **kwargs)
    result = collector.probe() if hasattr(collector, "probe") else {"source": name, "checks": {}}
    payload = result.model_dump() if hasattr(result, "model_dump") else result
    payload["configuration"] = source_config_status(normalized_name, settings=settings)
    _json(payload)


@source_app.command("time-audit")
def source_time_audit(
    sample_limit: int = typer.Option(20, "--sample-limit", min=0, max=200),
):
    """Audit publication evidence and normalized timestamps for every source."""

    _settings, _engine, session = _session()
    try:
        _json(publication_time_audit(session, sample_limit=sample_limit))
    finally:
        session.close()


@source_app.command("import-verified-history")
def source_import_verified_history(
    from_database: str = typer.Option(
        "retail-tide.db",
        "--from-database",
        help="Source SQLite path or SQLAlchemy URL containing immutable raw observations.",
    ),
    since: str = typer.Option(None, "--since", show_default=False),
    until: str = typer.Option(None, "--until", show_default=False),
    days: int = typer.Option(30, "--days", min=1),
    normalize: bool = typer.Option(True, "--normalize/--no-normalize"),
):
    """Recertify and import publication-time raw data; never copies analyses."""

    settings, _engine, session = _session()
    try:
        sync_registry(
            session,
            settings.config_dir,
            enabled_sources=settings.enabled_sources,
            collector_version=settings.collector_version,
        )
        try:
            start, end = resolve_collection_window(
                since=parse_datetime(since),
                until=parse_datetime(until),
                days=None if since is not None else days,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        result = import_verified_raw_history(
            session,
            source_database=from_database,
            since=start,
            until=end,
            settings=settings,
        )
        if normalize:
            result["normalized"] = normalize_pending(
                session, limit=max(result["candidate_items"] * 2, 1), settings=settings
            )
            result["resolved"] = resolve_pending_entities(
                session, limit=max(result["candidate_items"] * 2, 1)
            )
        _json(result)
    finally:
        session.close()


@source_app.command("collect")
def source_collect(
    name: str | None = typer.Argument(
        None,
        help="Source name. Omit to collect every enabled source (Guba and Taoguba by default).",
    ),
    since: str = typer.Option(
        None,
        "--since",
        help="Historical start or date (YYYY-MM-DD is Shanghai local date). Without a range, use the source/topic watermark; first run is 24h.",
        show_default=False,
    ),
    until: str = typer.Option(
        None,
        "--until",
        help="Historical end (ISO timestamp); defaults to now.",
        show_default=False,
    ),
    date_range: str = typer.Option(
        None,
        "--range",
        help="Date-only range, inclusive, e.g. 2026-08-20-2026-08-23.",
        show_default=False,
    ),
    days: int = typer.Option(
        None,
        "--days",
        min=1,
        help="Explicitly collect the previous N days through now.",
        show_default=False,
    ),
    query: str = typer.Option("黄金", help="Keyword sent to the source"),
    all_topics: bool = typer.Option(
        False,
        "--all-topics",
        help="Collect every active topic using its canonical registry name.",
    ),
):
    """Collect enabled real sources; optionally cover every active topic."""
    requested_since = since
    requested_until = until
    requested_range = date_range
    requested_days = days
    settings, _engine, session = _session()
    try:
        sync_registry(
            session,
            settings.config_dir,
            enabled_sources=settings.enabled_sources,
            collector_version=settings.collector_version,
        )
        try:
            since, until, days = _apply_date_range(date_range, since, until, days)
            start, end, explicit_days = _cli_collection_bounds(since, until, days)
            if start is not None or end is not None or explicit_days is not None:
                resolve_collection_window(since=start, until=end, days=explicit_days)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        names = [name] if name else list(settings.enabled_sources)
        if not names:
            raise typer.BadParameter("no enabled sources")
        normalized_names = [name.lower().replace("_", "-") for name in names]
        if all_topics:
            resume_key = _collection_resume_key(
                normalized_names,
                since=requested_since,
                until=requested_until,
                date_range=requested_range,
                days=requested_days,
            )
            results = collect_active_topics(
                session,
                source_names=normalized_names,
                since=start,
                until=end,
                days=explicit_days,
                settings=settings,
                resume_key=resume_key,
            )
            if "common-crawl" in normalized_names:
                normalize_pending(session, limit=50000, settings=settings)
                resolve_pending_entities(session, limit=50000)
                archive_start = start or (now_utc() - timedelta(hours=24))
                archive_end = end or now_utc()
                archive = enrich_common_crawl(
                    session,
                    since=archive_start,
                    until=archive_end,
                    settings=settings,
                )
                results.append(archive)
            _json(_collection_summary(results))
        else:
            topic_match = EntityResolver(session).resolve_topic(query)
            results = []
            for source_name in normalized_names:
                topic = session.get(Topic, topic_match.entity_id) if topic_match else None
                scope_start, scope_end, scope_explicit = resolve_incremental_window(
                    session,
                    source_name,
                    query=query,
                    topic=topic,
                    since=start,
                    until=end,
                    days=explicit_days,
                )
                if source_name.lower().replace("_", "-") == "common-crawl":
                    result = enrich_common_crawl(
                        session,
                        since=scope_start,
                        until=scope_end,
                        settings=settings,
                        topic_ids={topic.id} if topic else None,
                    )
                else:
                    result = collect_source(
                        session,
                        source_name,
                        query=query,
                        since=scope_start,
                        until=scope_end,
                        settings=settings,
                        topic_id=topic_match.entity_id if topic_match else None,
                        checkpoint_topic=topic,
                        explicit_window=scope_explicit,
                    )
                results.append(result)
            _json(results[0] if name and results else results)
        if any(result.get("source_degraded") for result in results):
            raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("collect", hidden=True)
def collect(
    name: str | None = typer.Argument(
        None,
        help="Source name. Omit to collect every enabled source.",
    ),
    since: str = typer.Option(
        None,
        "--since",
        help="Historical start or date; without a range, use the source/topic watermark; first run is 24h.",
        show_default=False,
    ),
    until: str = typer.Option(
        None, "--until", help="Historical end (ISO timestamp); defaults to now.", show_default=False
    ),
    date_range: str = typer.Option(
        None,
        "--range",
        help="Date-only range, inclusive, e.g. 2026-08-20-2026-08-23.",
        show_default=False,
    ),
    days: int = typer.Option(
        None, "--days", min=1, help="Explicitly collect the previous N days.", show_default=False
    ),
    query: str = typer.Option("黄金", help="Keyword sent to the source"),
    all_topics: bool = typer.Option(
        False,
        "--all-topics",
        help="Collect every active topic using its canonical registry name.",
    ),
):
    """Collect one source, or all enabled sources when SOURCE is omitted."""
    source_collect(
        name=name,
        since=since,
        until=until,
        date_range=date_range,
        days=days,
        query=query,
        all_topics=all_topics,
    )


@app.command("refresh")
def refresh(
    name: Annotated[
        str | None,
        typer.Option("--source", hidden=True),
    ] = None,
    exclude_source: Annotated[
        list[str] | None,
        typer.Option("--exclude-source", hidden=True),
    ] = None,
    target_date: str | None = typer.Option(
        None,
        "--date",
        help="Collect one Asia/Shanghai calendar date (YYYY-MM-DD).",
        show_default=False,
    ),
    since: str | None = typer.Option(
        None,
        "--since",
        help="Collect from this date or timestamp through now.",
        show_default=False,
    ),
    until: str | None = typer.Option(None, "--until", show_default=False, hidden=True),
    date_range: str = typer.Option(
        None,
        "--range",
        help="Date-only range, inclusive, e.g. 2026-08-20-2026-08-23.",
        show_default=False,
        hidden=True,
    ),
    days: int | None = typer.Option(
        None,
        "--days",
        min=1,
        help="Collect the most recent N Asia/Shanghai calendar dates, including today.",
        show_default=False,
    ),
    limit: int = typer.Option(
        50000,
        min=1,
        help="Maximum records per pipeline stage.",
        hidden=True,
    ),
    topic: Annotated[
        list[str] | None,
        typer.Option(
            "--topic",
            help="Refresh only this topic slug; repeat the option for multiple topics.",
        ),
    ] = None,
    sync_market_data: bool = typer.Option(
        False,
        "--sync-market/--no-sync-market",
        help="Refresh representative-asset prices after content analysis.",
        hidden=True,
    ),
):
    """Collect a time window, enrich it, deduplicate it and run LLM analysis."""

    if all(
        value is None
        for value in (target_date, since, until, date_range, days)
    ):
        raise typer.BadParameter("refresh requires --date, --days, or --since")
    settings, _engine, session = _session()
    requested_since = since
    requested_until = until
    requested_range = date_range
    requested_days = days
    requested_date = target_date
    refresh_started = now_utc()
    try:
        try:
            since, until, date_range, days = _apply_single_date(
                target_date,
                since,
                until,
                date_range,
                days,
            )
            since, until, days = _apply_date_range(date_range, since, until, days)
            start, end, explicit_days = _cli_collection_bounds(
                since,
                until,
                days,
                current=refresh_started,
            )
            start, end = resolve_collection_window(
                since=start,
                until=end,
                days=explicit_days,
                now=refresh_started,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

        names = [name] if name else list(settings.enabled_sources)
        excluded_names = {source.lower().replace("_", "-") for source in (exclude_source or [])}
        names = [
            source.lower().replace("_", "-")
            for source in names
            if source.lower().replace("_", "-") not in excluded_names
        ]
        if not names:
            raise typer.BadParameter("no enabled sources remain after exclusions")

        try:
            lock = _exclusive_refresh_lock(settings)
            lock.__enter__()
        except RuntimeError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=75) from exc
        try:
            logger.info(
                "event=refresh_started sources=%s date=%s days=%s since=%s until=%s "
                "range=%s topics=%s limit=%d",
                ",".join(names),
                requested_date,
                requested_days,
                requested_since,
                requested_until,
                requested_range,
                ",".join(topic or []) or "all",
                limit,
            )
            registry_result = sync_registry(
                session,
                settings.config_dir,
                enabled_sources=settings.enabled_sources,
                collector_version=settings.collector_version,
            )
            logger.info(
                "event=registry_synced topics=%d assets=%d aliases=%d sources=%d links=%d",
                registry_result.get("topics", 0),
                registry_result.get("assets", 0),
                registry_result.get("aliases", 0),
                registry_result.get("sources", 0),
                registry_result.get("links", 0),
            )

            topic_slugs = set(topic or [])
            required_names = [
                source for source in names if source not in SUPPLEMENT_SOURCES
            ]
            supplement_names = [
                source
                for source in names
                if source in {"xiaohongshu", "wikimedia-pageviews"}
            ]
            concurrent_names = [*required_names, *supplement_names]
            resume_key = _collection_resume_key(
                concurrent_names,
                since=requested_since,
                until=requested_until,
                date_range=requested_range,
                days=requested_days,
                single_date=requested_date,
                topic_slugs=list(topic_slugs),
                current=refresh_started,
            )

            assert start is not None and end is not None and resume_key is not None
            state_root = Path(os.getenv("RETAIL_TIDE_STATE_DIR", "var/state")) / "refresh"
            concurrent_collection = _collect_bounded_until_blocked(
                session,
                source_names=concurrent_names,
                since=start,
                until=end,
                settings=settings,
                topic_slugs=topic_slugs,
                state_path=state_root / f"{resume_key}-content.json",
            )
            required_collection = _bounded_collection_for_sources(
                concurrent_collection,
                required_names,
            )
            supplement_collection = _bounded_collection_for_sources(
                concurrent_collection,
                supplement_names,
            )
            required_summary = _backfill_collection_summary(required_collection)
            supplement_summary = (
                _backfill_collection_summary(supplement_collection)
                if supplement_names
                else {"complete": True, "sources": []}
            )
            required_blockers = _bounded_collection_blockers(required_collection)
            collection_blockers = _bounded_collection_blockers(concurrent_collection)

            archive_result = None
            archive_start = start
            archive_end = end
            if "common-crawl" in names:
                topic_ids = None
                if topic_slugs:
                    selected_topics = session.scalars(
                        select(Topic).where(Topic.slug.in_(topic_slugs))
                    ).all()
                    topic_ids = {row.id for row in selected_topics}
                normalize_pending(session, limit=limit, settings=settings)
                resolve_pending_entities(session, limit=limit)
                archive_result = enrich_common_crawl(
                    session,
                    since=archive_start,
                    until=archive_end,
                    settings=settings,
                    topic_ids=topic_ids,
                )

            if names == ["wikimedia-pageviews"]:
                # The independent UTC timer must not wake the content LLM or
                # rebuild unrelated market/event metrics. Wikimedia only needs
                # its own normalization and derived trend signal after raw
                # observations land.
                normalized_trends = normalize_pending(
                    session,
                    limit=limit,
                    settings=settings,
                    source_names={"wikimedia-pageviews"},
                )
                pipeline_result = {
                    "mode": "wikimedia-only",
                    "normalized": normalized_trends,
                    "resolved": 0,
                    "analyzed": 0,
                    "analysis_tasks": {
                        "failed": 0,
                        "pending": 0,
                        "untracked": 0,
                        "retry_ready": 0,
                        "retry_deferred": 0,
                    },
                    "trend_signals": aggregate_trend_signals(
                        session,
                        settings=settings,
                        since=start,
                        until=end,
                    ),
                }
            else:
                pipeline_result = run_core_pipeline(
                    session,
                    limit=limit,
                    settings=settings,
                    analysis_since=start,
                    analysis_until=end,
                )
            analysis_complete = _analysis_is_complete(pipeline_result)

            if sync_market_data:
                market_end = (end or now_utc()).date()
                market_result = _sync_topic_market(session, settings, end=market_end)
                pipeline_result["returns_after_market_sync"] = evaluate_events(
                    session, settings=settings
                )
            else:
                market_result = {
                    "skipped": True,
                    "reason": "market sync is separate from the post refresh",
                }

            supplement_warnings = []
            if required_blockers:
                incomplete_required_sources = sorted(
                    {
                        str(row.get("source") or "unknown")
                        for row in required_blockers
                    }
                )
                supplement_warnings.append(
                    "required source collection was incomplete: "
                    + ", ".join(incomplete_required_sources)
                )
            if required_summary.get("terminal"):
                terminal_sources = sorted(
                    {
                        str(row.get("source") or "unknown")
                        for row in required_summary["terminal"]
                    }
                )
                supplement_warnings.append(
                    "required source retry limit reached: "
                    + ", ".join(terminal_sources)
                )
            if not supplement_summary.get("complete", True):
                incomplete_supplements = sorted(
                    {
                        str(row.get("source") or "unknown")
                        for row in supplement_collection.get("jobs") or []
                        if not row.get("done")
                    }
                )
                supplement_warnings.extend(
                    f"{source} collection was incomplete"
                    for source in incomplete_supplements
                )
            if archive_result and archive_result.get("source_degraded"):
                supplement_warnings.append("common-crawl enrichment was incomplete")
            status = (
                "incomplete"
                if not analysis_complete
                else "complete_with_warnings"
                if supplement_warnings
                else "complete"
            )
            output = {
                "status": status,
                "window": {"since": start, "until": end},
                "collection": {
                    "required": required_summary,
                    "supplements": supplement_summary,
                },
                "archive": archive_result,
                "pipeline": pipeline_result,
                "market": market_result,
                "warnings": supplement_warnings,
            }
            _json(output)
            logger.info(
                "event=refresh_completed status=%s analyzed=%d elapsed_seconds=%.3f",
                status,
                int(pipeline_result.get("analyzed", 0)),
                (now_utc() - refresh_started).total_seconds(),
            )
            if not analysis_complete or collection_blockers:
                raise typer.Exit(code=1)
        finally:
            lock.__exit__(None, None, None)
    finally:
        session.close()


def _scheduled_refresh_run(
    *,
    limit: int,
    state_env: str,
    default_state_path: str,
    source: str | None,
    exclude_source: list[str] | None,
    sync_market_data: bool,
    schedule: str,
) -> None:
    """Run one independently pinned previous-day schedule."""

    state_path = Path(
        os.getenv(
            state_env,
            default_state_path,
        )
    )
    start = end = None
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            start = parse_datetime(state.get("since"))
            end = parse_datetime(state.get("until"))
            if start is None or end is None or start >= end:
                raise ValueError("invalid scheduled refresh window")
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.warning(
                "event=scheduled_refresh_state_ignored path=%s error=%r",
                state_path,
                str(exc),
            )
            start = end = None
    if start is None or end is None:
        start, end = scheduled_post_window()

    def write_state(window_start: datetime, window_end: datetime) -> None:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = state_path.with_suffix(f"{state_path.suffix}.tmp")
        temporary.write_text(
            json.dumps(
                {
                    "version": 1,
                    "since": window_start.isoformat(),
                    "until": window_end.isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        temporary.chmod(0o600)
        temporary.replace(state_path)

    while True:
        write_state(start, end)
        logger.info(
            "event=scheduled_refresh_window schedule=%s window=%s since=%s until=%s state=%s",
            schedule,
            "previous-day",
            start.isoformat(),
            end.isoformat(),
            state_path,
        )
        refresh(
            name=source,
            exclude_source=exclude_source,
            target_date=None,
            since=start.isoformat(),
            until=end.isoformat(),
            date_range=None,
            days=None,
            limit=limit,
            topic=None,
            sync_market_data=sync_market_data,
        )
        if schedule == "posts":
            _latest_start, latest_end = scheduled_post_window()
            if end < latest_end:
                previous_start, previous_end = start, end
                start = end
                end = min(end + timedelta(days=1), latest_end)
                write_state(start, end)
                logger.info(
                    "event=scheduled_refresh_catchup schedule=%s "
                    "completed_since=%s completed_until=%s next_since=%s next_until=%s",
                    schedule,
                    previous_start.isoformat(),
                    previous_end.isoformat(),
                    start.isoformat(),
                    end.isoformat(),
                )
                continue
        state_path.unlink(missing_ok=True)
        logger.info(
            "event=scheduled_refresh_state_cleared path=%s since=%s until=%s",
            state_path,
            start.isoformat(),
            end.isoformat(),
        )
        return


@app.command("scheduled-refresh", hidden=True)
def scheduled_refresh(
    limit: int = typer.Option(50000, min=1, help="每个分析阶段的最大记录数。"),
):
    """Collect posts and market data for one pinned Shanghai date."""

    _scheduled_refresh_run(
        limit=limit,
        state_env="RETAIL_TIDE_SCHEDULED_STATE_FILE",
        default_state_path="var/state/scheduled-refresh.json",
        source=None,
        exclude_source=["wikimedia-pageviews"],
        sync_market_data=True,
        schedule="posts",
    )


@app.command("scheduled-wikimedia", hidden=True)
def scheduled_wikimedia(
    limit: int = typer.Option(50000, min=1, help="每个分析阶段的最大记录数。"),
):
    """Collect the previous date's UTC Wikimedia bucket until it is available."""

    _scheduled_refresh_run(
        limit=limit,
        state_env="RETAIL_TIDE_SCHEDULED_WIKIMEDIA_STATE_FILE",
        default_state_path="var/state/scheduled-wikimedia.json",
        source="wikimedia-pageviews",
        exclude_source=None,
        sync_market_data=False,
        schedule="wikimedia-utc",
    )


@app.command("rebuild-derived", hidden=True)
def rebuild_derived(
    limit: int = typer.Option(300000, min=1, help="Maximum records per pipeline stage."),
    sync_market_data: bool = typer.Option(
        True,
        "--sync-market/--no-sync-market",
        help="Refresh representative-asset prices before recalculating available returns.",
    ),
):
    """Rebuild normalized content, analysis, metrics, events and returns."""

    settings, _engine, session = _session()
    try:
        repair_result = remove_misnormalized_zhihu_snapshots(session)
        reset_result = reset_metric_event_derivatives(session)
        pipeline_result = run_core_pipeline(session, limit=limit, settings=settings)
        if sync_market_data:
            market_result = _sync_topic_market(session, settings, end=now_utc().date())
            pipeline_result["returns_after_market_sync"] = evaluate_events(
                session, settings=settings
            )
        else:
            market_result = {"skipped": True, "reason": "disabled by command option"}
        _json(
            {
                "repair": repair_result,
                "reset": reset_result,
                "pipeline": pipeline_result,
                "market": market_result,
            }
        )
    finally:
        session.close()


@app.command("repair-derived", hidden=True)
def repair_derived():
    """Rebuild deterministic trend, metric and event data without calling the LLM."""

    settings, _engine, session = _session()
    try:
        repair_result = remove_misnormalized_zhihu_snapshots(session)
        reset_result = reset_metric_event_derivatives(session)
        trend_signals = aggregate_trend_signals(session, settings=settings)
        metrics_1h = aggregate_metrics(session, bucket_size="1h", settings=settings)
        metrics_1d = aggregate_metrics(session, bucket_size="1d", settings=settings)
        events = detect_events(session, settings=settings)
        returns = evaluate_events(session, settings=settings)
        _json(
            {
                "repair": repair_result,
                "reset": reset_result,
                "trend_signals": trend_signals,
                "metrics_1h": metrics_1h,
                "metrics_1d": metrics_1d,
                "events": events,
                "returns": returns,
            }
        )
    finally:
        session.close()


@app.command("backfill", hidden=True)
def backfill(
    name: str | None = typer.Argument(
        None,
        help="Source name. Omit to use every enabled source.",
    ),
    since: str = typer.Option(None, "--since", show_default=False),
    until: str = typer.Option(None, "--until", show_default=False),
    date_range: str = typer.Option(
        None,
        "--range",
        help="Date-only range, inclusive, e.g. 2026-08-20-2026-08-23.",
        show_default=False,
    ),
    days: int | None = typer.Option(
        None,
        "--days",
        min=1,
        help="Historical window in days; required unless --since/--until is provided.",
        show_default=False,
    ),
    limit: int = typer.Option(300000, min=1, help="Maximum records per pipeline stage."),
    topic: Annotated[
        list[str] | None,
        typer.Option(
            "--topic",
            help="Backfill only this topic slug; repeat for multiple topics.",
        ),
    ] = None,
    batch_pages: int = typer.Option(
        5,
        "--batch-pages",
        min=1,
        help="Pages fetched before saving a checkpoint and cooling down.",
    ),
    page_limit: int = typer.Option(
        1500,
        "--page-limit",
        min=1,
        help="Safety limit per topic and source unless topics.yaml overrides it.",
    ),
    cooldown: float = typer.Option(
        60,
        "--cooldown",
        min=0,
        help="Seconds between page batches; retries use a bounded multiplier.",
    ),
    max_retries: int = typer.Option(
        6,
        "--max-retries",
        min=1,
        help="Consecutive failed batches before leaving a resumable checkpoint.",
    ),
    max_jobs: int = typer.Option(
        0,
        "--max-jobs",
        min=0,
        help="Strategy jobs attempted this run; 0 means no job-count limit.",
    ),
    state_file: str = typer.Option(
        "retail_tide.backfill.json",
        "--state-file",
        help="JSON checkpoint path; it stores cursors and counts, never credentials.",
    ),
    reset_checkpoint: bool = typer.Option(
        False,
        "--reset-checkpoint",
        help="Start a new window checkpoint; already stored posts remain idempotent.",
    ),
    fair: bool = typer.Option(
        True,
        "--fair/--drain-job",
        help="Fetch one bounded batch per job so a large source cannot starve other sources.",
    ),
    run_pipeline: bool = typer.Option(
        True,
        "--pipeline/--no-pipeline",
        help="Normalize and recalculate after this collection round.",
    ),
    sync_market: bool = typer.Option(
        True,
        "--sync-market/--no-sync-market",
        help="Also refresh representative-asset prices after the content batch.",
    ),
):
    """Resume-friendly historical collection followed by a full data rebuild."""

    settings, _engine, session = _session()
    try:
        sync_registry(
            session,
            settings.config_dir,
            enabled_sources=settings.enabled_sources,
            collector_version=settings.collector_version,
        )
        try:
            since, until, days = _apply_date_range(date_range, since, until, days)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        if since is None and until is None and days is None:
            raise typer.BadParameter("backfill requires --days or --since/--until")
        try:
            start, end, explicit_days = _cli_collection_bounds(since, until, days)
            start, end = resolve_collection_window(
                since=start, until=end, days=_backfill_days(since, until, explicit_days)
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        names = [name] if name else list(settings.enabled_sources)
        if not names:
            raise typer.BadParameter("no enabled sources")

        def report_progress(row: dict) -> None:
            status = (
                "降级终止"
                if row.get("terminal")
                else (
                    "完成" if row.get("done") else ("等待冷却重试" if row.get("error") else "已存断点")
                )
            )
            strategy = f" / {row.get('sort_by')}" if row.get("sort_by") else ""
            typer.echo(
                (
                    f"[{row.get('source')}] {row.get('topic_name')}{strategy}：{status}，"
                    f"累计 {row.get('pages', 0)} 页 / {row.get('items_collected', 0)} 条，"
                    f"断点重试 {row.get('retries', 0)} 次"
                ),
                err=True,
            )

        try:
            collection = backfill_active_topics(
                session,
                source_names=names,
                since=start,
                until=end,
                settings=settings,
                topic_slugs=set(topic or []),
                state_path=state_file,
                batch_pages=batch_pages,
                default_page_limit=page_limit,
                cooldown_seconds=cooldown,
                max_retries=max_retries,
                max_jobs=max_jobs or None,
                max_jobs_per_source=1 if fair else None,
                reset_state=reset_checkpoint,
                one_batch_per_job=fair,
                source_concurrency=settings.source_concurrency,
                progress=report_progress,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

        pipeline_result = (
            run_core_pipeline(session, limit=limit, settings=settings)
            if run_pipeline
            else {"skipped": True, "reason": "disabled for this collection round"}
        )
        if sync_market and run_pipeline:
            market_result = _sync_topic_market(session, settings, end=end.date())
            pipeline_result["returns_after_market_sync"] = evaluate_events(
                session, settings=settings
            )
        else:
            market_result = {
                "skipped": True,
                "reason": "disabled or deferred until the content rebuild completes",
            }
        collection_output = {key: value for key, value in collection.items() if key != "jobs"}
        collection_output["job_counts"] = {
            "total": len(collection["jobs"]),
            "done": sum(bool(row.get("done")) for row in collection["jobs"]),
            "terminal": len(collection["terminal_jobs"]),
            "deferred": len(collection["deferred_jobs"]),
            "pending": collection["pending_jobs"],
            "attempted_this_run": collection["attempted_jobs"],
        }
        _json(
            {
                "collection": collection_output,
                "pipeline": pipeline_result,
                "market": market_result,
            }
        )
        if collection["degraded"]:
            raise typer.Exit(code=1)
    finally:
        session.close()


@app.command("normalize", hidden=True)
def normalize(limit: int = typer.Option(500, min=1)):
    """Normalize raw observations into Content or TrendObservation."""
    settings, _engine, session = _session()
    try:
        typer.echo(str(normalize_pending(session, limit=limit, settings=settings)))
    finally:
        session.close()


@app.command("resolve", hidden=True)
def resolve(limit: int = typer.Option(50000, min=1)):
    """Resolve canonical Topic and Asset entities."""
    _settings, _engine, session = _session()
    try:
        typer.echo(str(resolve_pending_entities(session, limit=limit)))
    finally:
        session.close()


@app.command("analyze", hidden=True)
def analyze(limit: int = typer.Option(500, min=1)):
    """Run versioned strict-schema content analysis."""
    settings, _engine, session = _session()
    try:
        analyzed = analyze_pending(session, limit=limit, settings=settings)
        _json(
            {
                "analyzed": analyzed,
                "tasks": analysis_task_summary(session, model=settings.analysis_model),
            }
        )
    finally:
        session.close()


@llm_app.command("review")
def llm_review(
    batch_size: int = typer.Option(50, "--batch-size", min=1, max=100),
    limit: int = typer.Option(0, "--limit", min=0, help="0 reviews every pending post."),
    since: str = typer.Option(None, "--since", show_default=False),
    until: str = typer.Option(None, "--until", show_default=False),
    days: int = typer.Option(None, "--days", min=1, show_default=False),
    source: Annotated[
        list[str] | None,
        typer.Option("--source", help="Review only this source; repeat as needed."),
    ] = None,
    candidate_model: str = typer.Option(
        None,
        "--candidate-model",
        help="Review only posts whose latest row from this model has a selected intent.",
        show_default=False,
    ),
    candidate_intent: Annotated[
        list[str] | None,
        typer.Option(
            "--candidate-intent",
            help="Candidate intent to review; repeat for buy/sell/hold/wait.",
        ),
    ] = None,
    only_unscanned: bool = typer.Option(
        False,
        "--only-unscanned",
        help="Skip posts that already have a non-rule-based LLM analysis.",
    ),
    min_content_chars: int = typer.Option(
        0,
        "--min-content-chars",
        min=0,
        help="Review only posts whose title and body reach this length.",
    ),
    max_chars: int = typer.Option(2400, "--max-chars", min=400, max=20000),
    model: str = typer.Option(CODEX_MODEL, "--model"),
    timeout: float = typer.Option(600, "--timeout", min=30),
    shards: int = typer.Option(1, "--shards", min=1, max=32),
    shard: int = typer.Option(0, "--shard", min=0),
    rebuild: bool = typer.Option(
        True,
        "--rebuild/--no-rebuild",
        help="Recalculate metrics, events and returns after the review.",
    ),
):
    """Use the locally authenticated Codex model for evidence-backed review."""

    settings, _engine, session = _session()
    try:
        requested_candidate_intents = set(candidate_intent or [])
        invalid_candidate_intents = requested_candidate_intents - {
            "buy",
            "sell",
            "hold",
            "wait",
            "unknown",
        }
        if invalid_candidate_intents:
            raise typer.BadParameter(
                "unknown candidate intent(s): " + ", ".join(sorted(invalid_candidate_intents))
            )
        if requested_candidate_intents and not candidate_model:
            raise typer.BadParameter("--candidate-model is required with --candidate-intent")
        start = end = None
        if since is not None or until is not None or days is not None:
            try:
                start, end = resolve_collection_window(
                    since=parse_datetime(since), until=parse_datetime(until), days=days
                )
            except ValueError as exc:
                raise typer.BadParameter(str(exc)) from exc

        def report_progress(row: dict) -> None:
            typer.echo(
                f"Codex 已审 {row['reviewed']}/{row['pending_at_start']}，"
                f"本批 {row['batch']} 条，意图 {row['intent_counts']}",
                err=True,
            )

        result = review_contents_with_codex(
            session,
            batch_size=batch_size,
            limit=limit or None,
            since=start,
            until=end,
            source_names=set(source or []),
            candidate_model=candidate_model,
            candidate_intents=requested_candidate_intents,
            only_unscanned=only_unscanned,
            min_content_chars=min_content_chars,
            max_chars=max_chars,
            model=model,
            timeout=timeout,
            shard_count=shards,
            shard_index=shard,
            progress=report_progress,
        )
        if rebuild:
            result["metrics_1h"] = aggregate_metrics(session, bucket_size="1h", settings=settings)
            result["metrics_1d"] = aggregate_metrics(session, bucket_size="1d", settings=settings)
            result["events"] = detect_events(session, settings=settings)
            result["returns"] = evaluate_events(session, settings=settings)
        _json(result)
    finally:
        session.close()


@llm_app.command("review-compatible")
def llm_review_compatible(
    batch_size: int = typer.Option(12, "--batch-size", min=1, max=100),
    limit: int = typer.Option(0, "--limit", min=0, help="0 reviews every pending post."),
    since: str = typer.Option(None, "--since", show_default=False),
    until: str = typer.Option(None, "--until", show_default=False),
    days: int = typer.Option(None, "--days", min=1, show_default=False),
    source: Annotated[
        list[str] | None,
        typer.Option("--source", help="Review only this source; repeat as needed."),
    ] = None,
    candidate_model: str = typer.Option(
        None,
        "--candidate-model",
        help="Review only posts whose latest row from this model has a selected intent.",
        show_default=False,
    ),
    candidate_intent: Annotated[
        list[str] | None,
        typer.Option(
            "--candidate-intent",
            help="Candidate intent to review; repeat for buy/sell/hold/wait/unknown.",
        ),
    ] = None,
    min_content_chars: int = typer.Option(
        0,
        "--min-content-chars",
        min=0,
        help="Review only posts whose title and body reach this length.",
    ),
    max_chars: int = typer.Option(2400, "--max-chars", min=400, max=20000),
    timeout: float = typer.Option(120, "--timeout", min=15),
    shards: int = typer.Option(1, "--shards", min=1, max=32),
    shard: int = typer.Option(0, "--shard", min=0),
    rebuild: bool = typer.Option(
        True,
        "--rebuild/--no-rebuild",
        help="Recalculate metrics, events and returns after the review.",
    ),
):
    """Use the configured compatible model for evidence-backed review."""

    settings, _engine, session = _session()
    try:
        requested_candidate_intents = set(candidate_intent or [])
        invalid_candidate_intents = requested_candidate_intents - {
            "buy",
            "sell",
            "hold",
            "wait",
            "unknown",
        }
        if invalid_candidate_intents:
            raise typer.BadParameter(
                "unknown candidate intent(s): " + ", ".join(sorted(invalid_candidate_intents))
            )
        if requested_candidate_intents and not candidate_model:
            raise typer.BadParameter("--candidate-model is required with --candidate-intent")
        status = llm_config_status(settings)
        if settings.llm_provider not in {"openai", "openai-compatible"} or not status["configured"]:
            raise typer.BadParameter(
                "compatible LLM configuration is incomplete: "
                + ", ".join(status.get("missing") or [])
            )
        fallback = settings.fallback_llm() if status["failover_enabled"] else None
        start = end = None
        if since is not None or until is not None or days is not None:
            try:
                start, end = resolve_collection_window(
                    since=parse_datetime(since), until=parse_datetime(until), days=days
                )
            except ValueError as exc:
                raise typer.BadParameter(str(exc)) from exc

        def report_progress(row: dict) -> None:
            typer.echo(
                f"兼容 LLM 已审 {row['reviewed']}/{row['pending_at_start']}，"
                f"本批 {row['batch']} 条，意图 {row['intent_counts']}",
                err=True,
            )

        try:
            result = review_contents_with_compatible_llm(
                session,
                endpoint=settings.llm_base_url or "",
                api_key=settings.llm_api_key or "",
                model=settings.analysis_model,
                batch_size=batch_size,
                limit=limit or None,
                since=start,
                until=end,
                source_names=set(source or []),
                candidate_model=candidate_model,
                candidate_intents=requested_candidate_intents,
                min_content_chars=min_content_chars,
                max_chars=max_chars,
                timeout=timeout,
                min_interval=settings.llm_min_interval,
                fallback_endpoint=fallback.base_url if fallback else None,
                fallback_api_key=fallback.api_key if fallback else None,
                fallback_model=fallback.model if fallback else None,
                fallback_timeout=fallback.timeout_seconds if fallback else None,
                fallback_min_interval=fallback.min_interval if fallback else 0.0,
                shard_count=shards,
                shard_index=shard,
                progress=report_progress,
            )
        except RuntimeError as exc:
            logger.error(
                "event=compatible_review_failed model=%s error=%r",
                settings.analysis_model,
                str(exc),
            )
            typer.echo(f"兼容 LLM 审查失败：{exc}", err=True)
            raise typer.Exit(code=1) from None
        if rebuild:
            result["metrics_1h"] = aggregate_metrics(session, bucket_size="1h", settings=settings)
            result["metrics_1d"] = aggregate_metrics(session, bucket_size="1d", settings=settings)
            result["events"] = detect_events(session, settings=settings)
            result["returns"] = evaluate_events(session, settings=settings)
        _json(result)
    finally:
        session.close()


@app.command("aggregate", hidden=True)
def aggregate(bucket: str = typer.Option("1d", "--bucket")):
    """Build platform metrics and rolling baseline signals."""
    settings, _engine, session = _session()
    try:
        typer.echo(str(aggregate_metrics(session, bucket_size=bucket, settings=settings)))
    finally:
        session.close()


@market_app.command("sync")
def market_sync(
    symbol: str = typer.Option(..., help="Asset symbol, e.g. 518880"),
    from_date: str = typer.Option(..., "--from", help="Start date YYYY-MM-DD"),
    to_date: str = typer.Option(None, "--to", help="End date YYYY-MM-DD; defaults to today"),
):
    """Sync daily bars and explicit A-share trading sessions."""
    settings, _engine, session = _session()
    try:
        asset = session.scalar(select(Asset).where(Asset.symbol == symbol))
        if asset is None:
            raise typer.BadParameter(f"unknown asset symbol {symbol}; run registry sync first")
        start = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date) if to_date else now_utc().date()
        market_status = market_config_status(settings)
        if not market_status["configured"]:
            raise typer.BadParameter(
                "market provider is not configured: " + ", ".join(market_status["missing"])
            )
        provider = provider_for_name(
            settings.market_provider,
            endpoint=settings.market_endpoint,
            headers=settings.market_headers(),
        )
        count = sync_market(session, asset, start, end, provider=provider)
        # Benchmarks are synced in the same command when registered, so abnormal
        # return fields are populated without changing user-facing CLI shape.
        if asset.benchmark_asset_id:
            benchmark = session.get(Asset, asset.benchmark_asset_id)
            if benchmark:
                sync_market(session, benchmark, start, end, provider=provider)
        typer.echo(str(count))
    finally:
        session.close()


@market_app.command("sync-all")
def market_sync_all(
    days: int = typer.Option(120, "--days", min=30, help="Calendar days to synchronize."),
):
    """Sync every representative asset linked to an active topic."""

    settings, _engine, session = _session()
    try:
        sync_registry(
            session,
            settings.config_dir,
            enabled_sources=settings.enabled_sources,
            collector_version=settings.collector_version,
        )
        _json(_sync_topic_market(session, settings, end=now_utc().date(), days=days))
    finally:
        session.close()


@market_app.command("check")
def market_check():
    """Check the live market provider configuration without making a request."""
    settings = get_settings()
    status = market_config_status(settings)
    _json(status)
    if not status["configured"]:
        raise typer.Exit(code=1)


@event_app.command("detect")
def event_detect(event_date: str = typer.Option(None, "--date")):
    """Detect and merge contiguous versioned discovery events."""
    settings, _engine, session = _session()
    try:
        parsed = date.fromisoformat(event_date) if event_date else None
        typer.echo(str(detect_events(session, settings=settings, event_date=parsed)))
    finally:
        session.close()


@event_app.command("evaluate")
def event_evaluate(event_id: int = typer.Option(None, "--event-id")):
    """Fill mature T+1/T+3/T+5/T+10/T+20 returns without look-ahead."""
    settings, _engine, session = _session()
    try:
        typer.echo(str(evaluate_events(session, settings=settings, event_id=event_id)))
    finally:
        session.close()


@research_app.command("event-study")
def research_event_study(
    topic: str = typer.Option("gold"),
    event: str = typer.Option("fomo_spike"),
):
    settings, _engine, session = _session()
    try:
        result = event_study(
            session,
            topic_slug=topic,
            event_type=event,
            settings=settings,
            persist=True,
        )
        typer.echo("RetailTide Event Study")
        typer.echo("======================")
        typer.echo(f"Topic: {topic}")
        typer.echo(f"Event: {event}")
        period = result["observation_period"]
        typer.echo(f"Observation Period: {period.get('from')} -> {period.get('until')}")
        typer.echo(f"Events: {result['events']}")
        typer.echo("                 T+1     T+3     T+5    T+10    T+20")
        for field, label in (
            ("raw_return", "Raw Return"),
            ("market_abnormal_return", "Market Adj"),
        ):
            values = []
            for horizon in ("1d", "3d", "5d", "10d", "20d"):
                summary = result["horizons"].get(horizon, {}).get(field, {})
                value = summary.get("mean")
                values.append("   n/a" if value is None else f"{value:7.2%}")
            typer.echo(f"{label:<16}" + " ".join(values))
        _json({"versions": result["versions"]})
    finally:
        session.close()


@research_app.command("quantile-study")
def research_quantile_study(
    topic: str = typer.Option("gold"),
    metric: str = typer.Option("fomo_ratio"),
):
    settings, _engine, session = _session()
    try:
        _json(
            quantile_study(
                session,
                topic_slug=topic,
                metric_name=metric,
                settings=settings,
                persist=True,
            )
        )
    finally:
        session.close()


@demo_app.command("run")
def demo_run():
    """Run the complete offline 30-day acceptance path using fixture sources."""
    settings = get_settings()
    demo_settings = settings.for_demo()
    engine = init_db(make_engine(demo_settings))
    session = session_factory(engine)()
    try:
        sync_registry(
            session,
            demo_settings.config_dir,
            enabled_sources=demo_settings.enabled_sources,
            collector_version=demo_settings.collector_version,
        )
        since = now_utc() - timedelta(days=44)
        until = now_utc()
        for name in ("guba", "taoguba", "wikimedia-pageviews"):
            result = collect_source(
                session,
                name,
                query="黄金",
                since=since,
                until=until,
                settings=demo_settings,
            )
            _json(result)
        result = run_core_pipeline(session, limit=10000, settings=demo_settings)
        asset = session.scalar(select(Asset).where(Asset.symbol == "518880"))
        if asset:
            provider = provider_for_name(demo_settings.market_provider)
            sync_market(session, asset, since.date(), now_utc().date(), provider=provider)
            if asset.benchmark_asset_id:
                benchmark = session.get(Asset, asset.benchmark_asset_id)
                if benchmark:
                    sync_market(
                        session, benchmark, since.date(), now_utc().date(), provider=provider
                    )
            result["returns_after_market_sync"] = evaluate_events(session, settings=demo_settings)
        _json(result)
    finally:
        session.close()


if __name__ == "__main__":
    app()
