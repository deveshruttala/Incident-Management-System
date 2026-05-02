"""Mock a stack-wide failure scenario.

Scenario:  RDBMS primary goes down → API errors cascade → MCP host degrades.

Usage:
    python scripts/simulate_failure.py [base_url]

Default base_url is http://localhost:8000.
Set the `IMS_API_KEY` env var to authenticate when the backend has
`INGEST_API_KEY` configured.
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import time

import httpx

API_KEY = os.environ.get("IMS_API_KEY", "")
HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}


SCENARIO = [
    # (component_id, component_type, message, error_code, count, delay_seconds)
    ("rdbms-primary", "RDBMS", "connection refused", "E_CONN", 50, 0.0),
    ("rdbms-primary", "RDBMS", "replication lag exceeded threshold", "E_REPL", 30, 1.0),
    ("api-gateway", "API", "downstream RDBMS timeout", "E_TIMEOUT", 80, 2.0),
    ("api-gateway", "API", "5xx burst on /v1/users", "E_5XX", 60, 3.0),
    ("mcp-host-eu", "MCP_HOST", "tool call failed: db_query", "E_TOOL", 40, 4.0),
    ("cache-cluster-01", "CACHE", "redis OOM eviction storm", "E_OOM", 25, 5.0),
    ("queue-events", "QUEUE", "consumer lag spike", "E_LAG", 20, 6.0),
]


async def fire(client: httpx.AsyncClient, base: str, batch: list[dict]) -> None:
    try:
        r = await client.post(
            f"{base}/ingest/batch",
            json={"signals": batch},
            headers=HEADERS,
            timeout=5.0,
        )
        r.raise_for_status()
        body = r.json()
        print(f"sent {len(batch):4d} signals → accepted={body['accepted']} rejected={body['rejected']}")
    except Exception as exc:
        print(f"FAILED to send batch ({len(batch)}): {exc}")


async def main(base_url: str) -> None:
    async with httpx.AsyncClient() as client:
        # confirm the service is up
        try:
            r = await client.get(f"{base_url}/health", timeout=5.0)
            r.raise_for_status()
            print(f"health = {r.json()['status']}")
        except Exception as exc:
            print(f"backend unreachable at {base_url}: {exc}")
            return

        start = time.monotonic()
        for component_id, comp_type, msg, code, count, delay in SCENARIO:
            await asyncio.sleep(max(0, (start + delay) - time.monotonic()))
            batch = [
                {
                    "component_id": component_id,
                    "component_type": comp_type,
                    "message": msg,
                    "error_code": code,
                    "latency_ms": round(random.uniform(50, 5000), 2),
                    "payload": {"trace_id": f"trace-{random.randint(1, 999_999)}"},
                }
                for _ in range(count)
            ]
            await fire(client, base_url, batch)

        print("\n✔ Scenario complete. Visit the dashboard to triage.")


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    asyncio.run(main(base))
