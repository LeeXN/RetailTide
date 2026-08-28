from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .time import as_utc


class RawObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    source_item_id: str = Field(min_length=1)
    observation_kind: str
    published_at: datetime | None = None
    observed_at: datetime
    payload: dict[str, Any]

    @field_validator("published_at", "observed_at")
    @classmethod
    def timezone_aware_utc(cls, value: datetime | None) -> datetime | None:
        return as_utc(value)


class CollectResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[RawObservation]
    next_cursor: str | None = None
    exhausted: bool = False
    # A collector can preserve useful, dated observations while truthfully
    # reporting that a secondary path failed (for example, XHS search falling
    # back to the personalized feed). Warnings make that state distinct from
    # both a healthy run and a fatal source failure.
    warnings: list[str] = Field(default_factory=list)
    # Warnings may be informational quality exclusions. Only collectors that
    # know their usable result set is incomplete set this flag.
    partial: bool = False
    # Structured, credential-free collector evidence. Coordinators may merge
    # these counters into resumable checkpoints without parsing warning text.
    diagnostics: dict[str, Any] = Field(default_factory=dict)


ActorType = Literal[
    "retail", "kol", "media", "institutional", "commercial", "bot_or_spam", "unknown"
]
InvestorLevel = Literal["novice", "intermediate", "experienced", "unknown"]
Direction = Literal["bullish", "neutral", "bearish", "unknown"]
Intent = Literal["buy", "sell", "hold", "wait", "unknown"]
Position = Literal["not_owned", "owned", "sold", "unknown"]


class ComponentValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str
    confidence: float = Field(ge=0, le=1)


class DirectionValue(ComponentValue):
    value: Direction


class IntentValue(ComponentValue):
    value: Intent


class PositionValue(ComponentValue):
    value: Position


class ActorValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: ActorType
    confidence: float = Field(ge=0, le=1)


class InvestorValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: InvestorLevel
    confidence: float = Field(ge=0, le=1)


class EmotionSignals(BaseModel):
    model_config = ConfigDict(extra="forbid")

    urgency: bool = False
    fear_of_missing: bool = False
    social_proof: bool = False
    price_chasing: bool = False
    regret: bool = False
    panic: bool = False

    @property
    def fomo_score(self) -> float:
        return (
            sum(
                [
                    self.urgency,
                    self.fear_of_missing,
                    self.social_proof,
                    self.price_chasing,
                    self.regret,
                ]
            )
            / 5.0
        )


class SpamValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: bool
    confidence: float = Field(ge=0, le=1)


class PromotionValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: bool
    confidence: float = Field(ge=0, le=1)


class AnalysisContract(BaseModel):
    """Strict, provider-independent version of the content-analysis JSON contract."""

    model_config = ConfigDict(extra="forbid")

    actor: ActorValue
    investor: InvestorValue
    novice_signals: list[str] = []
    direction: DirectionValue
    intent: IntentValue
    position: PositionValue
    emotion: EmotionSignals
    spam: SpamValue
    # Promotion is intentionally independent from spam: a legitimate paid
    # placement or course advertisement is commercial exposure, but not
    # necessarily bot/spam behavior.
    promotion: PromotionValue = Field(
        default_factory=lambda: PromotionValue(value=False, confidence=0.0)
    )


class Bar(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0
    amount: float = 0
    adjustment: str = "none"

    @field_validator("ts")
    @classmethod
    def bar_time_utc(cls, value: datetime) -> datetime:
        return as_utc(value)  # type: ignore[return-value]


class TradingSessionSchema(BaseModel):
    market: str
    trade_date: str
    is_open: bool
    open_at: datetime | None = None
    close_at: datetime | None = None
    metadata: dict[str, Any] = {}
