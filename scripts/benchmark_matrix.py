#!/usr/bin/env python3
import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path
from uuid import uuid4

import httpx


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(round((len(ordered) - 1) * fraction), len(ordered) - 1)]


async def one_run(client, count: int, concurrency: int, run_number: int) -> dict:
    prefix = f"benchmark-{concurrency}-{run_number}-{uuid4().hex}"
    semaphore = asyncio.Semaphore(concurrency)
    latencies = []
    failures = []

    async def send(index: int):
        event = {
            "event_id": str(uuid4()),
            "device_id": f"{prefix}-{index % max(concurrency, 1)}",
            "timestamp": "2026-07-24T12:00:00Z",
            "temperature": 72.4,
            "voltage": 12.1,
            "status": "OK",
            "region": "benchmark",
        }
        async with semaphore:
            started = time.perf_counter()
            response = await client.post("/telemetry", json=event)
            latencies.append((time.perf_counter() - started) * 1000)
            if response.status_code != 202:
                failures.append(response.status_code)

    started = time.perf_counter()
    await asyncio.gather(*(send(index) for index in range(count)))
    ingestion_seconds = time.perf_counter() - started
    persisted = 0
    deadline = time.perf_counter() + 120
    while not failures and time.perf_counter() < deadline:
        response = await client.get("/pipeline/stats", params={"device_prefix": prefix})
        response.raise_for_status()
        persisted = response.json()["event_count"]
        if persisted == count:
            break
        await asyncio.sleep(0.05)
    pipeline_seconds = time.perf_counter() - started
    return {
        "concurrency": concurrency,
        "run": run_number,
        "requests": count,
        "accepted": count - len(failures),
        "persisted": persisted,
        "failures": len(failures),
        "ingestion_seconds": round(ingestion_seconds, 3),
        "pipeline_seconds": round(pipeline_seconds, 3),
        "ingestion_rps": round(count / ingestion_seconds, 1),
        "pipeline_eps": round(persisted / pipeline_seconds, 1),
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 2),
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
        },
    }


async def benchmark(url: str, count: int, levels: list[int], runs: int) -> dict:
    results = []
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        for concurrency in levels:
            for run_number in range(1, runs + 1):
                results.append(await one_run(client, count, concurrency, run_number))
    summaries = []
    for concurrency in levels:
        group = [item for item in results if item["concurrency"] == concurrency]
        rates = [item["pipeline_eps"] for item in group]
        summaries.append(
            {
                "concurrency": concurrency,
                "runs": runs,
                "median_pipeline_eps": round(statistics.median(rates), 1),
                "min_pipeline_eps": min(rates),
                "max_pipeline_eps": max(rates),
                "total_failures": sum(item["failures"] for item in group),
                "all_persisted": all(item["persisted"] == count for item in group),
            }
        )
    return {"configuration": {"requests_per_run": count}, "summaries": summaries, "runs": results}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency-levels", default="1,10,25,50")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/matrix.json"))
    args = parser.parse_args()
    levels = [int(value) for value in args.concurrency_levels.split(",")]
    result = asyncio.run(benchmark(args.url, args.count, levels, args.runs))
    rendered = json.dumps(result, indent=2)
    print(rendered)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(f"{rendered}\n")
    if any(item["total_failures"] or not item["all_persisted"] for item in result["summaries"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
