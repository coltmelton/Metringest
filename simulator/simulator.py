import asyncio
import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_url: str = "http://api:8000"
    device_count: int = 100
    events_per_second: float = 50
    batch_size: int = 25
    duplicate_rate: float = 0.02
    failure_rate: float = 0.03
    late_event_rate: float = 0.05
    malformed_rate: float = 0.005

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")


settings = Settings()
regions = ["us-east", "us-west", "eu-central", "ap-south"]
last_events: list[dict] = []


def make_event() -> dict:
    device_number = random.randint(1, settings.device_count)
    failed = random.random() < settings.failure_rate
    status = "FAILED" if failed else random.choices(["OK", "WARNING"], [0.92, 0.08])[0]
    timestamp = datetime.now(timezone.utc)
    if random.random() < settings.late_event_rate:
        timestamp -= timedelta(seconds=random.randint(30, 600))

    event = {
        "event_id": str(uuid4()),
        "device_id": f"sensor-{device_number:05d}",
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "temperature": round(random.gauss(72, 6), 2),
        "voltage": round(random.gauss(12.1, 0.35), 2),
        "status": status,
        "region": random.choice(regions),
    }

    if failed:
        event["temperature"] = round(random.choice([random.uniform(145, 170), random.uniform(-55, -42)]), 2)
        event["voltage"] = round(random.uniform(6.0, 9.0), 2)

    if random.random() < settings.malformed_rate:
        event.pop(random.choice(["device_id", "timestamp", "temperature"]))

    return event


def next_event() -> dict:
    if last_events and random.random() < settings.duplicate_rate:
        return random.choice(last_events)
    event = make_event()
    last_events.append(event)
    del last_events[:-1000]
    return event


async def wait_for_api(client: httpx.AsyncClient) -> None:
    while True:
        try:
            response = await client.get(f"{settings.api_url}/health", timeout=2)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(2)


async def run() -> None:
    interval = settings.batch_size / settings.events_per_second
    async with httpx.AsyncClient(timeout=10) as client:
        await wait_for_api(client)
        while True:
            batch = [next_event() for _ in range(settings.batch_size)]
            try:
                response = await client.post(f"{settings.api_url}/telemetry/batch", json=batch)
                response.raise_for_status()
                print(response.json(), flush=True)
            except httpx.HTTPError as exc:
                print(f"simulator publish failed: {exc}", flush=True)
            await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(run())
