from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, StringConstraints, field_validator

MetricName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120, pattern=r"^[a-zA-Z][a-zA-Z0-9_.-]*$")]


class MetricEvent(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: MetricName
    value: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source: str = Field(min_length=1, max_length=80)
    tags: dict[str, str] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def timezone_required(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        return value


class IngestResponse(BaseModel):
    accepted: bool = True
    event_id: UUID
    stream: str


class MetricSummary(BaseModel):
    name: str
    count: int
    average: float
    minimum: float
    maximum: float
    last_value: float
    updated_at: datetime

