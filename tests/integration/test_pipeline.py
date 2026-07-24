import asyncio
import os
import subprocess
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.integration

API_URL = os.getenv("METRINGEST_TEST_URL", "http://localhost:8001")


async def wait_for(client: httpx.AsyncClient, path: str, predicate, timeout: float = 45):
    deadline = asyncio.get_running_loop().time() + timeout
    last_response = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            last_response = await client.get(path)
            if last_response.is_success and predicate(last_response.json()):
                return last_response.json()
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    detail = last_response.text if last_response is not None else "no response"
    pytest.fail(f"timed out waiting for {path}: {detail}")


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True)


@pytest.mark.asyncio
async def test_event_traverses_kafka_worker_postgres_and_redis():
    name = f"integration.pipeline.{uuid4().hex}"
    payload = {"name": name, "value": 42.5, "source": "pytest", "tags": {"suite": "e2e"}}

    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        response = await client.post("/v1/metrics", json=payload)
        assert response.status_code == 202
        accepted = response.json()
        assert accepted["accepted"] is True

        recent = await wait_for(
            client,
            f"/v1/metrics/{name}/recent",
            lambda body: len(body["events"]) == 1,
        )
        assert recent["events"][0]["id"] == accepted["event_id"]
        assert recent["events"][0]["tags"] == {"suite": "e2e"}

        summaries = await client.get("/v1/metrics", params={"limit": 100})
        assert summaries.status_code == 200
        summary = next(item for item in summaries.json() if item["name"] == name)
        assert summary["count"] == 1
        assert summary["average"] == 42.5


@pytest.mark.asyncio
async def test_worker_catches_up_after_failure_without_losing_events():
    name = f"integration.recovery.{uuid4().hex}"
    event_ids = []

    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        compose("stop", "worker")
        try:
            for value in (11.0, 12.0, 13.0):
                response = await client.post(
                    "/v1/metrics",
                    json={"name": name, "value": value, "source": "recovery-test"},
                )
                assert response.status_code == 202
                event_ids.append(response.json()["event_id"])

            recent = await client.get(f"/v1/metrics/{name}/recent")
            assert recent.status_code == 200
            assert recent.json()["events"] == []
        finally:
            compose("start", "worker")

        recovered = await wait_for(
            client,
            f"/v1/metrics/{name}/recent",
            lambda body: len(body["events"]) == 3,
            timeout=60,
        )
        assert {event["id"] for event in recovered["events"]} == set(event_ids)

        summaries = await client.get("/v1/metrics", params={"limit": 100})
        summary = next(item for item in summaries.json() if item["name"] == name)
        assert summary["count"] == 3
        assert summary["minimum"] == 11.0
        assert summary["maximum"] == 13.0
