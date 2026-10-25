"""Submit text from stdin and wait for an active MCP worker to summarize it.

Run: uv run python examples/summarize.py < article.txt
"""

import json
import os
import sys
import time

import httpx


def main():
    with httpx.Client(
        base_url=os.getenv("RMCPQ_URL", "http://127.0.0.1:8790"), timeout=10, trust_env=False
    ) as client:
        response = client.post(
            "/v1/jobs",
            json={
                "queue": "summaries",
                "instruction": "Summarize the supplied text in three concise bullet points.",
                "context": {"text": sys.stdin.read()},
                "result_format": "text",
            },
        )
        response.raise_for_status()
        job = response.json()
        print("Queued " + job["id"], file=sys.stderr)
        deadline = time.monotonic() + 300
        while job["status"] in ("queued", "processing"):
            if time.monotonic() >= deadline:
                raise TimeoutError("No result within five minutes; job remains saved")
            time.sleep(1)
            response = client.get("/v1/jobs/" + job["id"])
            response.raise_for_status()
            job = response.json()
        if job["status"] != "completed":
            raise RuntimeError(f"Job {job['status']}: {job['error']}")
        print(job["result"] if isinstance(job["result"], str) else json.dumps(job["result"]))


if __name__ == "__main__":
    main()
