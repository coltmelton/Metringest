from datetime import UTC, datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class DeviceStatus(str, Enum):
    ok = "OK"
    warning = "WARNING"
    failed = "FAILED"


class TelemetryEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    device_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime
    temperature: Annotated[float, Field(ge=-100, le=250)]
    voltage: Annotated[float, Field(ge=0, le=1000)]
    status: DeviceStatus
    region: str = Field(min_length=1, max_length=64)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class TelemetryEnvelope(BaseModel):
    event: TelemetryEvent
    received_at: datetime

    @field_validator("received_at")
    @classmethod
    def normalize_received_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
