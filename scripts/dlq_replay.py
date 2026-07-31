#!/usr/bin/env python3
import argparse
import asyncio
import base64
import json
import os
from pathlib import Path

import asyncpg
from aiokafka import AIOKafkaProducer


def decode_jsonb(value) -> dict:
    return json.loads(value) if isinstance(value, str) else value


def replay_bytes(payload: dict) -> bytes:
    if set(payload) == {"raw_base64"}:
        return base64.b64decode(payload["raw_base64"], validate=True)
    return json.dumps(payload).encode()


def message_key(payload: dict) -> bytes | None:
    device_id = payload.get("event", {}).get("device_id")
    return str(device_id).encode() if device_id else None


async def list_errors(pool, limit: int) -> list[dict]:
    rows = await pool.fetch(
        """
        SELECT id, source_topic, source_partition, source_offset, reason,
               created_at, replay_count, replayed_at
        FROM pipeline_errors ORDER BY id DESC LIMIT $1
        """,
        limit,
    )
    return [dict(row) for row in rows]


async def replay_error(
    pool,
    producer,
    error_id: int,
    payload_override: dict | None,
    execute: bool,
) -> dict:
    row = await pool.fetchrow(
        "SELECT id, payload, reason, replay_count FROM pipeline_errors WHERE id = $1",
        error_id,
    )
    if row is None:
        raise ValueError(f"pipeline error {error_id} was not found")
    payload = payload_override if payload_override is not None else decode_jsonb(row["payload"])
    data = replay_bytes(payload)
    result = {
        "error_id": error_id,
        "reason": row["reason"],
        "payload_bytes": len(data),
        "next_replay_count": row["replay_count"] + 1,
        "executed": execute,
    }
    if not execute:
        return result
    await producer.send_and_wait(
        "raw-telemetry",
        data,
        key=message_key(payload),
        headers=[("replay_error_id", str(error_id).encode())],
    )
    await pool.execute(
        """
        UPDATE pipeline_errors
        SET replay_count = replay_count + 1, replayed_at = now()
        WHERE id = $1
        """,
        error_id,
    )
    return result


async def run(args) -> None:
    pool = await asyncpg.create_pool(args.database_url, min_size=1, max_size=2)
    try:
        if args.command == "list":
            print(json.dumps(await list_errors(pool, args.limit), default=str, indent=2))
            return
        if args.payload_file:
            override = json.loads(Path(args.payload_file).read_text())
        elif args.payload_json:
            override = json.loads(args.payload_json)
        else:
            override = None
        producer = None
        if args.execute:
            producer = AIOKafkaProducer(
                bootstrap_servers=args.kafka_bootstrap_servers,
                acks="all",
                enable_idempotence=True,
            )
            await producer.start()
        try:
            result = await replay_error(pool, producer, args.error_id, override, args.execute)
            print(json.dumps(result, indent=2))
        finally:
            if producer is not None:
                await producer.stop()
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect and safely replay pipeline DLQ records")
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL", "postgresql://telemetry:telemetry@postgres:5432/telemetry"
        ),
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--limit", type=int, default=20)
    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--error-id", type=int, required=True)
    payload_group = replay_parser.add_mutually_exclusive_group()
    payload_group.add_argument("--payload-file")
    payload_group.add_argument("--payload-json")
    replay_parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
