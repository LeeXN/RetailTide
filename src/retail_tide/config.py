from __future__ import annotations

import os
import re
import shlex
import time
from dataclasses import dataclass, field, replace
from pathlib import Path

from .source_sessions import source_session_status

SOURCE_ENV_PREFIXES = {
    "guba": "GUBA",
    "taoguba": "TAOGUBA",
    "zhihu": "ZHIHU",
    "xiaohongshu": "XIAOHONGSHU",
}
# Keep the default collection profile aligned with the V0 source priorities.
# Candidate sources are opt-in and public sources are enabled after the
# operator provides a compliant User-Agent identity.
DEFAULT_ENABLED_SOURCES = ("guba", "taoguba")
BUILTIN_PUBLIC_SOURCES = DEFAULT_ENABLED_SOURCES
RUNTIME_DEFAULT_ENABLED_SOURCES = DEFAULT_ENABLED_SOURCES
PUBLIC_SOURCES = ("common-crawl", "wikimedia-pageviews")
DEFAULT_PUBLIC_SOURCES = ("wikimedia-pageviews",)
OPTIONAL_SOURCES = ("zhihu", "xiaohongshu", *PUBLIC_SOURCES)
PRIMARY_CONTENT_SOURCES = ("guba", "taoguba")
QUESTION_CONTENT_SOURCES = ("zhihu",)
SUPPLEMENT_SOURCES = ("xiaohongshu", "common-crawl", "wikimedia-pageviews")
SOURCE_REQUIRED = {name: name in DEFAULT_ENABLED_SOURCES for name in SOURCE_ENV_PREFIXES}
DEFAULT_SOURCE_INTERVALS = {
    # Authenticated Guba sessions still trigger identity verification when
    # historical pagination advances too quickly. Keep the default deliberately
    # slower than the site's nominal page latency.
    "guba": 15.0,
    "taoguba": 15.0,
    "zhihu": 1.0,
    "xiaohongshu": 15.0,
    "common-crawl": 1.0,
    "wikimedia-pageviews": 1.0,
}


def compliant_http_user_agent(value: str | None) -> bool:
    """Require a project token plus a reachable operator contact."""

    text = str(value or "").strip()
    return bool(
        len(text) >= 8
        and re.search(r"\S+/\S+", text)
        and ("@" in text or "http://" in text or "https://" in text)
    )


def _parse_enabled_sources(
    value: str | None, *, default: tuple[str, ...] = DEFAULT_ENABLED_SOURCES
) -> tuple[str, ...]:
    # Older setup versions wrote an empty value after skipping both public P0
    # sources. Treat that legacy value as "use defaults" so an upgrade starts
    # collecting them without requiring the operator to recreate .env.
    if value is None or not value.strip():
        return default
    if value.strip().lower() in {"none", "off", "disabled"}:
        return ()
    names: list[str] = []
    for raw_name in value.split(","):
        normalized = raw_name.strip().lower().replace("_", "-")
        if not normalized:
            continue
        if normalized not in (*SOURCE_ENV_PREFIXES, *PUBLIC_SOURCES):
            raise ValueError(
                f"unknown source {raw_name!r}; choose from {', '.join((*SOURCE_ENV_PREFIXES, *PUBLIC_SOURCES))}"
            )
        if normalized not in names:
            names.append(normalized)
    return tuple(names)


def _load_env_file(path: Path) -> None:
    """Load a small dotenv-compatible file without overriding the shell."""

    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        if not separator or not key.isidentifier() or key in os.environ:
            continue
        try:
            values = shlex.split(raw_value, comments=False)
            value = values[0] if values else ""
        except ValueError:
            value = raw_value.strip().strip('"').strip("'")
        os.environ[key] = value


@dataclass(frozen=True)
class SourceCredential:
    """Runtime-only credentials for one authorized remote source.

    Secrets are read from the environment and are never persisted in the
    database or included in status payloads.
    """

    name: str
    endpoint: str | None = None
    api_key: str | None = field(default=None, repr=False)
    access_token: str | None = field(default=None, repr=False)
    api_secret: str | None = field(default=None, repr=False)
    auth_header: str = "X-API-Key"

    @property
    def has_auth(self) -> bool:
        return bool(self.api_key or self.access_token)

    def headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
            if self.name == "zhihu":
                headers["X-Request-Timestamp"] = str(int(time.time()))
        elif self.api_key:
            headers[self.auth_header or "X-API-Key"] = self.api_key
        if self.api_secret:
            headers["X-API-Secret"] = self.api_secret
        return headers

    @classmethod
    def from_env(cls, name: str) -> SourceCredential:
        prefix = SOURCE_ENV_PREFIXES[name]
        access_token = os.getenv(f"RETAIL_TIDE_{prefix}_ACCESS_TOKEN") or None
        if name == "zhihu" and not access_token:
            access_token = os.getenv("ZHIHU_ACCESS_SECRET") or None
        return cls(
            name=name,
            endpoint=os.getenv(f"RETAIL_TIDE_{prefix}_ENDPOINT") or None,
            api_key=os.getenv(f"RETAIL_TIDE_{prefix}_API_KEY") or None,
            access_token=access_token,
            api_secret=os.getenv(f"RETAIL_TIDE_{prefix}_API_SECRET") or None,
            auth_header=os.getenv(f"RETAIL_TIDE_{prefix}_AUTH_HEADER", "X-API-Key"),
        )


def _source_prefix(name: str) -> str:
    normalized = name.lower().replace("_", "-")
    try:
        return SOURCE_ENV_PREFIXES[normalized]
    except KeyError as exc:
        raise ValueError(f"unknown source: {name}") from exc


def source_config_status(name: str, *, settings: Settings) -> dict[str, object]:
    """Return safe, non-secret readiness information for a source."""

    normalized = name.lower().replace("_", "-")
    if normalized in PUBLIC_SOURCES:
        configured = settings.data_mode == "demo" or compliant_http_user_agent(
            settings.http_user_agent
        )
        return {
            "name": normalized,
            "enabled": normalized in settings.enabled_sources,
            "required": normalized in settings.enabled_sources,
            "mode": settings.data_mode,
            "transport": "built-in-public",
            "configured": configured,
            "endpoint_configured": True,
            "credential_configured": configured,
            "missing": [] if configured else ["RETAIL_TIDE_HTTP_USER_AGENT"],
        }
    prefix = _source_prefix(normalized)
    credential = settings.source_credentials.get(normalized, SourceCredential(normalized))
    enabled = normalized in settings.enabled_sources
    if settings.data_mode == "demo":
        return {
            "name": normalized,
            "enabled": enabled,
            "required": enabled,
            "mode": "demo",
            "configured": True,
            "endpoint_configured": False,
            "credential_configured": False,
            "missing": [],
        }
    if normalized == "zhihu":
        configured = bool(credential.access_token)
        return {
            "name": normalized,
            "enabled": enabled,
            "required": enabled,
            "mode": settings.data_mode,
            "transport": "built-in-official-api",
            "configured": configured,
            "endpoint_configured": False,
            "endpoint_available": True,
            "endpoint_source": "built-in",
            "credential_configured": configured,
            "missing": []
            if configured
            else ["RETAIL_TIDE_ZHIHU_ACCESS_TOKEN or ZHIHU_ACCESS_SECRET"],
        }
    if normalized == "xiaohongshu":
        mcp_configured = bool(credential.endpoint)
        spider_configured = bool(settings.xiaohongshu_spider_endpoint)
        configured = mcp_configured or spider_configured
        if spider_configured and mcp_configured:
            transport = "spider-primary-mcp-fallback"
        elif spider_configured:
            transport = "spider-primary"
        else:
            transport = "project-owned-xiaohongshu-mcp"
        missing = (
            []
            if configured
            else ["RETAIL_TIDE_XIAOHONGSHU_SPIDER_ENDPOINT or RETAIL_TIDE_XIAOHONGSHU_ENDPOINT"]
        )
        return {
            "name": normalized,
            "enabled": enabled,
            "required": enabled,
            "mode": settings.data_mode,
            "transport": transport,
            "configured": configured,
            "endpoint_configured": configured,
            "spider_endpoint_configured": spider_configured,
            "mcp_fallback_configured": mcp_configured,
            "credential_configured": (
                credential.has_auth or settings.xiaohongshu_spider_credential().has_auth
            ),
            "session_auth": "managed-by-collector-services",
            "missing": missing,
        }
    if normalized in BUILTIN_PUBLIC_SOURCES and not credential.endpoint:
        # A credential without an endpoint is treated as a partial custom
        # override. With neither present, use the built-in read-only collector.
        missing = [] if not credential.has_auth else [f"RETAIL_TIDE_{prefix}_ENDPOINT"]
        result: dict[str, object] = {
            "name": normalized,
            "enabled": enabled,
            "required": enabled,
            "mode": settings.data_mode,
            "transport": "built-in-public" if not missing else "custom-json",
            "configured": not missing,
            "endpoint_configured": False,
            "credential_configured": credential.has_auth,
            "missing": missing,
        }
        if normalized in {"guba", "taoguba"}:
            result["browser_session"] = source_session_status(
                normalized, settings.source_session_file(normalized)
            )
        return result
    missing: list[str] = []
    if not credential.endpoint:
        missing.append(f"RETAIL_TIDE_{prefix}_ENDPOINT")
    if not credential.has_auth:
        missing.append(f"RETAIL_TIDE_{prefix}_API_KEY or RETAIL_TIDE_{prefix}_ACCESS_TOKEN")
    return {
        "name": normalized,
        "enabled": enabled,
        "required": enabled,
        "mode": settings.data_mode,
        "transport": "custom-json",
        "configured": not missing,
        "endpoint_configured": bool(credential.endpoint),
        "credential_configured": credential.has_auth,
        "missing": missing,
    }


@dataclass(frozen=True)
class LLMProviderConfig:
    """One provider slot in the ordered analysis failover route."""

    role: str
    provider: str
    base_url: str | None
    api_key: str | None = field(default=None, repr=False)
    model: str = ""
    min_interval: float = 1.0
    timeout_seconds: float = 120.0


def _llm_provider_status(config: LLMProviderConfig, *, prefix: str) -> dict[str, object]:
    provider = config.provider or "rule-based"
    if provider in {"rule-based", "local", "none", "disabled"}:
        return {
            "role": config.role,
            "provider": "rule-based",
            "model": config.model or "rule-based-v0",
            "enabled": config.role == "primary",
            "configured": True,
            "base_url_configured": False,
            "credential_configured": False,
            "min_interval": config.min_interval,
            "timeout_seconds": config.timeout_seconds,
            "missing": [],
        }

    missing: list[str] = []
    if provider not in {"openai", "openai-compatible"}:
        missing.append(f"{prefix}_PROVIDER must be openai or openai-compatible")
    if not config.base_url:
        missing.append(f"{prefix}_BASE_URL")
    if not config.api_key:
        missing.append(f"{prefix}_API_KEY")
    if not config.model or config.model == "rule-based-v0":
        missing.append(
            "RETAIL_TIDE_ANALYSIS_MODEL"
            if config.role == "primary"
            else "RETAIL_TIDE_LLM_FALLBACK_MODEL"
        )
    return {
        "role": config.role,
        "provider": provider,
        "model": config.model or None,
        "enabled": True,
        "base_url_configured": bool(config.base_url),
        "configured": not missing,
        "credential_configured": bool(config.api_key),
        "min_interval": config.min_interval,
        "timeout_seconds": config.timeout_seconds,
        "missing": missing,
    }


def llm_config_status(settings: Settings) -> dict[str, object]:
    """Return non-secret readiness information for the ordered LLM route."""

    primary = _llm_provider_status(settings.primary_llm(), prefix="RETAIL_TIDE_LLM")
    fallback_config = settings.fallback_llm()
    fallback = (
        _llm_provider_status(fallback_config, prefix="RETAIL_TIDE_LLM_FALLBACK")
        if fallback_config is not None
        else {
            "role": "fallback",
            "provider": None,
            "model": None,
            "enabled": False,
            "configured": False,
            "base_url_configured": False,
            "credential_configured": False,
            "min_interval": None,
            "timeout_seconds": None,
            "missing": [],
        }
    )
    return {
        "provider": primary["provider"],
        "model": primary["model"],
        "base_url_configured": primary["base_url_configured"],
        "configured": primary["configured"],
        "credential_configured": primary["credential_configured"],
        "missing": primary["missing"],
        "failover_enabled": bool(fallback["enabled"] and fallback["configured"]),
        "fallback_configured": bool(fallback["configured"]),
        "fallback_missing": fallback["missing"],
        "providers": [primary, fallback],
    }


def market_config_status(settings: Settings) -> dict[str, object]:
    """Return safe readiness information for the market data provider."""

    if settings.data_mode == "demo":
        return {
            "mode": "demo",
            "provider": "synthetic-a-share",
            "configured": True,
            "missing": [],
        }
    missing: list[str] = []
    if not settings.market_provider:
        missing.append("RETAIL_TIDE_MARKET_PROVIDER")
    elif settings.market_provider not in {
        "tencent",
        "qq",
        "eastmoney",
        "nasdaq",
        "public",
        "http-json",
    }:
        missing.append(
            "RETAIL_TIDE_MARKET_PROVIDER must be public, tencent, eastmoney, nasdaq, "
            "or http-json in live mode"
        )
    if settings.market_provider == "http-json":
        if not settings.market_endpoint:
            missing.append("RETAIL_TIDE_MARKET_ENDPOINT")
        if not (settings.market_api_key or settings.market_access_token):
            missing.append("RETAIL_TIDE_MARKET_API_KEY or RETAIL_TIDE_MARKET_ACCESS_TOKEN")
    return {
        "mode": settings.data_mode,
        "provider": settings.market_provider or None,
        "configured": not missing,
        "missing": missing,
    }


@dataclass(frozen=True)
class Settings:
    database_url: str = "sqlite:///retail-tide.db"
    log_level: str = "INFO"
    author_hmac_secret: str = "retail-tide-development-secret"
    config_dir: Path = Path("config")
    prompt_version: str = "content-analysis-v1"
    analysis_schema_version: str = "content-analysis-v1"
    analysis_model: str = "rule-based-v0"
    llm_provider: str = "rule-based"
    llm_base_url: str | None = None
    llm_api_key: str | None = field(default=None, repr=False)
    llm_fallback_provider: str | None = None
    llm_fallback_base_url: str | None = None
    llm_fallback_api_key: str | None = field(default=None, repr=False)
    llm_fallback_model: str | None = None
    data_mode: str = "live"
    market_provider: str = "public"
    market_endpoint: str | None = None
    market_api_key: str | None = field(default=None, repr=False)
    market_access_token: str | None = field(default=None, repr=False)
    market_auth_header: str = "X-API-Key"
    http_user_agent: str | None = None
    llm_min_interval: float = 1.0
    llm_timeout_seconds: float = 120.0
    llm_fallback_min_interval: float = 1.0
    llm_fallback_timeout_seconds: float = 120.0
    source_concurrency: int = 5
    source_request_intervals: dict[str, float] = field(default_factory=dict)
    common_crawl_url_limit: int = 200
    common_crawl_warc_limit: int = 50
    guba_session_file: Path = Path("var/auth/guba.session.json")
    taoguba_session_file: Path = Path("var/auth/taoguba.session.json")
    xiaohongshu_spider_endpoint: str | None = None
    xiaohongshu_spider_api_key: str | None = field(default=None, repr=False)
    xiaohongshu_spider_access_token: str | None = field(default=None, repr=False)
    xiaohongshu_spider_auth_header: str = "X-API-Key"
    run_lock_file: Path = Path("var/locks/refresh.lock")
    source_credentials: dict[str, SourceCredential] = field(default_factory=dict, repr=False)
    enabled_sources: tuple[str, ...] = DEFAULT_ENABLED_SOURCES
    collector_version: str = "collector-v2"
    metric_version: str = "metric-v1"
    event_rule_version: str = "discovery-v1"

    def source_credential(self, name: str) -> SourceCredential:
        normalized = name.lower().replace("_", "-")
        return self.source_credentials.get(normalized, SourceCredential(normalized))

    def xiaohongshu_spider_credential(self) -> SourceCredential:
        return SourceCredential(
            "xiaohongshu-spider",
            endpoint=self.xiaohongshu_spider_endpoint,
            api_key=self.xiaohongshu_spider_api_key,
            access_token=self.xiaohongshu_spider_access_token,
            auth_header=self.xiaohongshu_spider_auth_header,
        )

    def source_session_file(self, name: str) -> Path:
        normalized = name.lower().replace("_", "-")
        paths = {
            "guba": self.guba_session_file,
            "taoguba": self.taoguba_session_file,
        }
        try:
            return paths[normalized]
        except KeyError as exc:
            raise ValueError(f"browser-session reuse is not supported for source {name!r}") from exc

    def request_interval(self, name: str) -> float:
        normalized = name.lower().replace("_", "-")
        return max(
            0.0,
            float(
                self.source_request_intervals.get(
                    normalized, DEFAULT_SOURCE_INTERVALS.get(normalized, 1.0)
                )
            ),
        )

    def market_headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.market_access_token:
            headers["Authorization"] = f"Bearer {self.market_access_token}"
        elif self.market_api_key:
            headers[self.market_auth_header or "X-API-Key"] = self.market_api_key
        return headers

    def primary_llm(self) -> LLMProviderConfig:
        return LLMProviderConfig(
            role="primary",
            provider=self.llm_provider or "rule-based",
            base_url=self.llm_base_url,
            api_key=self.llm_api_key,
            model=self.analysis_model,
            min_interval=self.llm_min_interval,
            timeout_seconds=self.llm_timeout_seconds,
        )

    def fallback_llm(self) -> LLMProviderConfig | None:
        if not any(
            (
                self.llm_fallback_provider,
                self.llm_fallback_base_url,
                self.llm_fallback_api_key,
                self.llm_fallback_model,
            )
        ):
            return None
        return LLMProviderConfig(
            role="fallback",
            provider=self.llm_fallback_provider or "openai-compatible",
            base_url=self.llm_fallback_base_url,
            api_key=self.llm_fallback_api_key,
            model=self.llm_fallback_model or "",
            min_interval=self.llm_fallback_min_interval,
            timeout_seconds=self.llm_fallback_timeout_seconds,
        )

    def for_demo(self) -> Settings:
        """Explicitly opt into deterministic fixtures for the demo command."""

        return replace(
            self,
            database_url=os.getenv(
                "RETAIL_TIDE_DEMO_DATABASE_URL", "sqlite:///retail_tide-demo.db"
            ),
            data_mode="demo",
            market_provider="synthetic-a-share",
            analysis_model="rule-based-v0",
            llm_provider="rule-based",
            llm_base_url=None,
            llm_api_key=None,
            llm_fallback_provider=None,
            llm_fallback_base_url=None,
            llm_fallback_api_key=None,
            llm_fallback_model=None,
        )

    @classmethod
    def from_env(cls) -> Settings:
        env_file = Path(os.getenv("RETAIL_TIDE_ENV_FILE", ".env"))
        _load_env_file(env_file)
        config_dir = Path(os.getenv("RETAIL_TIDE_CONFIG_DIR", "config"))
        data_mode = os.getenv("RETAIL_TIDE_DATA_MODE", "live").lower()
        if data_mode not in {"live", "demo"}:
            raise ValueError("RETAIL_TIDE_DATA_MODE must be live or demo")
        database_url = os.getenv("RETAIL_TIDE_DATABASE_URL")
        if not database_url:
            database_url = (
                "sqlite:///retail_tide-demo.db"
                if data_mode == "demo"
                else "sqlite:///retail-tide.db"
            )
        # Live runtime defaults to an external LLM so missing credentials are
        # visible instead of silently producing a local substitute analysis.
        llm_provider = os.getenv("RETAIL_TIDE_LLM_PROVIDER", "openai").lower()
        analysis_model = os.getenv("RETAIL_TIDE_ANALYSIS_MODEL")
        if not analysis_model:
            analysis_model = (
                "gpt-5" if llm_provider in {"openai", "openai-compatible"} else "rule-based-v0"
            )
        llm_base_url = os.getenv("RETAIL_TIDE_LLM_BASE_URL") or None
        if llm_provider == "openai" and not llm_base_url:
            llm_base_url = "https://api.openai.com/v1"
        llm_min_interval = max(0.0, float(os.getenv("RETAIL_TIDE_LLM_MIN_INTERVAL", "1.0")))
        llm_timeout_seconds = max(15.0, float(os.getenv("RETAIL_TIDE_LLM_TIMEOUT_SECONDS", "120")))
        fallback_provider = os.getenv("RETAIL_TIDE_LLM_FALLBACK_PROVIDER") or None
        fallback_base_url = os.getenv("RETAIL_TIDE_LLM_FALLBACK_BASE_URL") or None
        fallback_api_key = os.getenv("RETAIL_TIDE_LLM_FALLBACK_API_KEY") or None
        fallback_model = os.getenv("RETAIL_TIDE_LLM_FALLBACK_MODEL") or None
        if not fallback_provider and any((fallback_base_url, fallback_api_key, fallback_model)):
            fallback_provider = "openai-compatible"
        if fallback_provider == "openai" and not fallback_base_url:
            fallback_base_url = "https://api.openai.com/v1"
        return cls(
            database_url=database_url,
            log_level=os.getenv("RETAIL_TIDE_LOG_LEVEL", "INFO").strip().upper(),
            author_hmac_secret=os.getenv(
                "RETAIL_TIDE_AUTHOR_HMAC_SECRET", "retail-tide-development-secret"
            ),
            config_dir=config_dir,
            prompt_version=os.getenv("RETAIL_TIDE_PROMPT_VERSION", "content-analysis-v1"),
            analysis_schema_version=os.getenv(
                "RETAIL_TIDE_ANALYSIS_SCHEMA_VERSION", "content-analysis-v1"
            ),
            analysis_model=analysis_model,
            llm_provider=llm_provider,
            llm_base_url=llm_base_url,
            llm_api_key=os.getenv("RETAIL_TIDE_LLM_API_KEY") or None,
            llm_fallback_provider=fallback_provider,
            llm_fallback_base_url=fallback_base_url,
            llm_fallback_api_key=fallback_api_key,
            llm_fallback_model=fallback_model,
            data_mode=data_mode,
            market_provider=os.getenv("RETAIL_TIDE_MARKET_PROVIDER", "public").lower(),
            market_endpoint=os.getenv("RETAIL_TIDE_MARKET_ENDPOINT") or None,
            market_api_key=os.getenv("RETAIL_TIDE_MARKET_API_KEY") or None,
            market_access_token=os.getenv("RETAIL_TIDE_MARKET_ACCESS_TOKEN") or None,
            market_auth_header=os.getenv("RETAIL_TIDE_MARKET_AUTH_HEADER", "X-API-Key"),
            http_user_agent=os.getenv("RETAIL_TIDE_HTTP_USER_AGENT") or None,
            llm_min_interval=llm_min_interval,
            llm_timeout_seconds=llm_timeout_seconds,
            llm_fallback_min_interval=max(
                0.0,
                float(
                    os.getenv(
                        "RETAIL_TIDE_LLM_FALLBACK_MIN_INTERVAL",
                        str(llm_min_interval),
                    )
                ),
            ),
            llm_fallback_timeout_seconds=max(
                15.0,
                float(
                    os.getenv(
                        "RETAIL_TIDE_LLM_FALLBACK_TIMEOUT_SECONDS",
                        str(llm_timeout_seconds),
                    )
                ),
            ),
            source_concurrency=max(
                1,
                min(8, int(os.getenv("RETAIL_TIDE_SOURCE_CONCURRENCY", "5"))),
            ),
            source_request_intervals={
                name: float(
                    os.getenv(
                        f"RETAIL_TIDE_{name.upper().replace('-', '_')}_MIN_INTERVAL",
                        str(default),
                    )
                )
                for name, default in DEFAULT_SOURCE_INTERVALS.items()
                if os.getenv(f"RETAIL_TIDE_{name.upper().replace('-', '_')}_MIN_INTERVAL")
            },
            common_crawl_url_limit=max(
                1, int(os.getenv("RETAIL_TIDE_COMMON_CRAWL_URL_LIMIT", "200"))
            ),
            common_crawl_warc_limit=max(
                1, int(os.getenv("RETAIL_TIDE_COMMON_CRAWL_WARC_LIMIT", "50"))
            ),
            guba_session_file=Path(
                os.getenv(
                    "RETAIL_TIDE_GUBA_SESSION_FILE",
                    "var/auth/guba.session.json",
                )
            ),
            taoguba_session_file=Path(
                os.getenv(
                    "RETAIL_TIDE_TAOGUBA_SESSION_FILE",
                    "var/auth/taoguba.session.json",
                )
            ),
            xiaohongshu_spider_endpoint=(
                os.getenv("RETAIL_TIDE_XIAOHONGSHU_SPIDER_ENDPOINT") or None
            ),
            xiaohongshu_spider_api_key=(
                os.getenv("RETAIL_TIDE_XIAOHONGSHU_SPIDER_API_KEY") or None
            ),
            xiaohongshu_spider_access_token=(
                os.getenv("RETAIL_TIDE_XIAOHONGSHU_SPIDER_ACCESS_TOKEN") or None
            ),
            xiaohongshu_spider_auth_header=os.getenv(
                "RETAIL_TIDE_XIAOHONGSHU_SPIDER_AUTH_HEADER", "X-API-Key"
            ),
            run_lock_file=Path(os.getenv("RETAIL_TIDE_RUN_LOCK_FILE", "var/locks/refresh.lock")),
            source_credentials={
                name: SourceCredential.from_env(name) for name in SOURCE_ENV_PREFIXES
            },
            enabled_sources=_parse_enabled_sources(
                os.getenv("RETAIL_TIDE_ENABLED_SOURCES"),
                default=RUNTIME_DEFAULT_ENABLED_SOURCES,
            ),
            collector_version=os.getenv("RETAIL_TIDE_COLLECTOR_VERSION", "collector-v2"),
            metric_version=os.getenv("RETAIL_TIDE_METRIC_VERSION", "metric-v1"),
            event_rule_version=os.getenv("RETAIL_TIDE_EVENT_RULE_VERSION", "discovery-v1"),
        )


def get_settings() -> Settings:
    return Settings.from_env()
