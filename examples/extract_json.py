"""Structured extraction producer; validates the application-specific result itself.

Run: uv run python examples/extract_json.py
Subscribe an active MCP worker to the `extraction` queue first.
"""

import os
import time

import httpx
from pydantic import BaseModel, ConfigDict


class Contact(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str
    email: str


def main():
    with httpx.Client(
        base_url=os.getenv("RMCPQ_URL", "http://127.0.0.1:8790"), timeout=10, trust_env=False
    ) as client:
        response = client.post(
            "/v1/jobs",
            json={
                "queue": "extraction",
                "instruction": "Extract the contact. Return a JSON object with name and email fields only.",
                "context": {"text": "Please contact Alex at alex@example.org."},
                "result_format": "json",
            },
        )
        response.raise_for_status()
        job = response.json()
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
        print(Contact.model_validate(job["result"]).model_dump_json(indent=2))


if __name__ == "__main__":
    main()
