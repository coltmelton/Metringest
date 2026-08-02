#!/usr/bin/env python3
import argparse
import asyncio
import copy
import json
from pathlib import Path

import httpx
from jsonschema import Draft202012Validator

SCHEMA_DIRECTORY = Path(__file__).parents[1] / "schemas"


def load_schema(version: int) -> dict:
    schema = json.loads((SCHEMA_DIRECTORY / f"raw-telemetry-v{version}.json").read_text())
    Draft202012Validator.check_schema(schema)
    return schema


async def register(client: httpx.AsyncClient, subject: str, schema: dict) -> int:
    response = await client.post(
        f"/subjects/{subject}/versions",
        json={"schemaType": "JSON", "schema": json.dumps(schema)},
    )
    response.raise_for_status()
    return int(response.json()["id"])


async def compatible(client: httpx.AsyncClient, subject: str, schema: dict) -> bool:
    response = await client.post(
        f"/compatibility/subjects/{subject}/versions/latest",
        json={"schemaType": "JSON", "schema": json.dumps(schema)},
    )
    response.raise_for_status()
    return bool(response.json()["is_compatible"])


async def verify_registry(url: str, subject: str) -> dict:
    v0 = load_schema(0)
    v1 = load_schema(1)
    async with httpx.AsyncClient(base_url=url, timeout=15) as client:
        response = await client.put(
            f"/config/{subject}",
            json={"compatibility": "BACKWARD_TRANSITIVE"},
        )
        response.raise_for_status()
        v0_id = await register(client, subject, v0)
        v1_compatible = await compatible(client, subject, v1)
        if not v1_compatible:
            raise RuntimeError("v1 is not backward compatible with v0")
        v1_id = await register(client, subject, v1)

        incompatible = copy.deepcopy(v1)
        incompatible["properties"]["event"]["properties"]["device_id"] = {
            "type": "integer"
        }
        incompatible_rejected = not await compatible(client, subject, incompatible)
        if not incompatible_rejected:
            raise RuntimeError("registry accepted an incompatible device_id type change")

        response = await client.get(f"/config/{subject}")
        response.raise_for_status()
        mode = response.json()["compatibilityLevel"]
        if mode != "BACKWARD_TRANSITIVE":
            raise RuntimeError(f"unexpected compatibility mode: {mode}")
    return {
        "subject": subject,
        "compatibility": mode,
        "v0_id": v0_id,
        "v1_id": v1_id,
        "v1_compatible": v1_compatible,
        "incompatible_change_rejected": incompatible_rejected,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8081")
    parser.add_argument("--subject", default="raw-telemetry-contract-ci")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(verify_registry(args.url, args.subject)), indent=2))


if __name__ == "__main__":
    main()
