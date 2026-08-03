import pytest

from scripts import benchmark_matrix


@pytest.mark.asyncio
async def test_benchmark_warms_pipeline_before_recorded_runs(monkeypatch):
    calls = []

    async def fake_run(_client, count, concurrency, run_number):
        calls.append((count, concurrency, run_number))
        return {
            "concurrency": concurrency,
            "run": run_number,
            "requests": count,
            "accepted": count,
            "persisted": count,
            "failures": 0,
            "pipeline_seconds": 0.5,
            "pipeline_eps": 200.0,
        }

    monkeypatch.setattr(benchmark_matrix, "one_run", fake_run)

    result = await benchmark_matrix.benchmark(
        "http://unused", "key", count=100, levels=[1, 10], runs=2,
        max_pipeline_seconds=5, warmup_count=7,
    )

    assert calls[0] == (7, 10, 0)
    assert calls[1:] == [(100, 1, 1), (100, 1, 2), (100, 10, 1), (100, 10, 2)]
    assert len(result["runs"]) == 4
    assert result["warmup"] == {
        "requests": 7, "accepted": 7, "persisted": 7, "failures": 0,
    }
    assert all(summary["within_slo"] for summary in result["summaries"])


@pytest.mark.asyncio
async def test_benchmark_can_disable_warmup(monkeypatch):
    calls = []

    async def fake_run(_client, count, concurrency, run_number):
        calls.append((count, concurrency, run_number))
        return {
            "concurrency": concurrency, "run": run_number, "requests": count,
            "accepted": count, "persisted": count, "failures": 0,
            "pipeline_seconds": 0.5, "pipeline_eps": 20.0,
        }

    monkeypatch.setattr(benchmark_matrix, "one_run", fake_run)

    result = await benchmark_matrix.benchmark(
        "http://unused", "key", count=10, levels=[1], runs=1, warmup_count=0,
    )

    assert calls == [(10, 1, 1)]
    assert result["warmup"] is None
