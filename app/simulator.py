"""Generate repeatable, realistic metric traffic for local evaluation."""

import json
import math
import os
import random
import time
from datetime import UTC, datetime, timedelta
from urllib.request import Request, urlopen

API_URL = os.getenv("SIMULATOR_API_URL", "http://api:8000/v1/metrics")
EVENT_COUNT = int(os.getenv("SIMULATOR_EVENT_COUNT", "2500"))
SOURCES = {
    "storefront": ["checkout.duration", "http.requests", "cart.items"],
    "mobile-app": ["checkout.duration", "api.latency", "sessions.active"],
    "billing": ["payment.duration", "payment.failures", "invoices.processed"],
}
BASELINES = {
    "checkout.duration": (220, 55), "http.requests": (1, 0.08),
    "cart.items": (3.4, 1.2), "api.latency": (95, 24),
    "sessions.active": (820, 110), "payment.duration": (410, 90),
    "payment.failures": (0.025, 0.015), "invoices.processed": (1, 0.1),
}


def build_event(index: int) -> dict:
    source = random.choices(list(SOURCES), weights=[5, 4, 2])[0]
    name = random.choice(SOURCES[source])
    baseline, spread = BASELINES[name]
    value = max(0, random.gauss(baseline + math.sin(index / 90) * spread * 0.35, spread))
    if random.random() < 0.008:
        value *= random.uniform(2.5, 5.0)
    occurred_at = datetime.now(UTC) - timedelta(seconds=(EVENT_COUNT - index) * 18)
    return {
        "name": name, "value": round(value, 4), "timestamp": occurred_at.isoformat(),
        "source": source,
        "tags": {"region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
                 "environment": "development"},
    }


def main() -> None:
    random.seed(42)
    for index in range(EVENT_COUNT):
        request = Request(API_URL, data=json.dumps(build_event(index)).encode(),
                          headers={"content-type": "application/json"}, method="POST")
        for attempt in range(6):
            try:
                with urlopen(request, timeout=10) as response:
                    if response.status != 202:
                        raise RuntimeError(f"unexpected status {response.status}")
                break
            except Exception:
                if attempt == 5:
                    raise
                time.sleep(2)
        if (index + 1) % 250 == 0:
            print(f"simulator: accepted {index + 1}/{EVENT_COUNT}", flush=True)


if __name__ == "__main__":
    main()
