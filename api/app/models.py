from datetime import datetime, timezone
from enum import Enum
from typing import Annotated
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class DeviceStatus(str, Enum):
    ok = "OK"
    warning = "WARNING"
    failed = "FAILED"


class TelemetryIn(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
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
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class TelemetryEnvelope(BaseModel):
    event: TelemetryIn
    received_at: datetime


class BatchResponse(BaseModel):
    accepted: int
    rejected: int
    errors: list[dict]


class TelemetryRow(BaseModel):
    event_id: str
    device_id: str
    timestamp: datetime
    received_at: datetime
    processed_at: datetime
    temperature: float
    voltage: float
    status: str
    region: str
    avg_temperature_5m: float | None = None
    voltage_drop_detected: bool
    event_lag_ms: int
    outlier_detected: bool
