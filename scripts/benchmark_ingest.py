#!/usr/bin/env python3
"""Small, dependency-light HTTP ingestion benchmark with machine-readable output."""

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


async def run(url: str, count: int, concurrency: int) -> dict:
    semaphore = asyncio.Semaphore(concurrency)
    latencies = []
    failures = []
    metric_name = f"benchmark.ingest.{uuid4().hex}"

    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        async def send(index: int) -> None:
            async with semaphore:
                started = time.perf_counter()
                response = await client.post(
                    "/v1/metrics",
                    json={"name": metric_name, "value": index, "source": "benchmark"},
                )
                latencies.append((time.perf_counter() - started) * 1000)
                if response.status_code != 202:
                    failures.append({"status": response.status_code, "body": response.text[:200]})

        started = time.perf_counter()
        await asyncio.gather(*(send(index) for index in range(count)))
        elapsed = time.perf_counter() - started

    return {
        "requests": count,
        "concurrency": concurrency,
        "accepted": count - len(failures),
        "failures": len(failures),
        "elapsed_seconds": round(elapsed, 3),
        "requests_per_second": round(count / elapsed, 1),
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 2),
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
        },
        "metric_name": metric_name,
        "failure_samples": failures[:3],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--minimum-rps", type=float, default=0)
    args = parser.parse_args()

    if args.count < 1 or args.concurrency < 1:
        parser.error("count and concurrency must be positive")
    result = asyncio.run(run(args.url, args.count, args.concurrency))
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n")
    if result["failures"] or result["requests_per_second"] < args.minimum_rps:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
