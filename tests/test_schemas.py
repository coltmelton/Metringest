from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas import MetricEvent


def test_metric_event_accepts_valid_payload():
    event = MetricEvent(name="checkout.duration", value=184.2, source="storefront")
    assert event.timestamp.tzinfo == UTC
    assert event.tags == {}


def test_metric_event_rejects_bad_name_and_naive_timestamp():
    with pytest.raises(ValidationError):
        MetricEvent(name="bad name", value=1, source="test", timestamp=datetime.now())
