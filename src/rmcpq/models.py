"""Shared producer/worker contracts and bounded JSON serialization."""

import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StringConstraints, model_validator

Name = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")]
WorkerId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$")]
ModelId = Annotated[
    str, StringConstraints(min_length=1, max_length=200, pattern=r"^[^\s\x00-\x1f\x7f]+$")
]
JobStatus = Literal["queued", "processing", "completed", "failed", "cancelled"]
ResultFormat = Literal["text", "json"]
MAX_PAYLOAD_BYTES = 1_048_576
DEFAULT_WAIT = 86_400
MAX_WAIT = 86_400
WorkerWaitSeconds = Annotated[float, Field(gt=0, le=MAX_WAIT, allow_inf_nan=False)]


class BrokerError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def canonical(value: object) -> str:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        size = len(encoded.encode("utf-8"))
    except (TypeError, ValueError, RecursionError, UnicodeError) as exc:
        raise ValueError("Value must be finite, UTF-8 JSON") from exc
    if size > MAX_PAYLOAD_BYTES:
        raise ValueError("Payload exceeds 1 MiB")
    return encoded


class JobInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    queue: Name
    instruction: str = Field(min_length=1, max_length=200_000)
    context: JsonValue = None
    result_format: ResultFormat = "text"
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_payload(self):
        if not self.instruction.strip():
            raise ValueError("Instruction must not be blank")
        canonical(self.model_dump())
        return self


class WorkerInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    queues: list[Name] = Field(min_length=1, max_length=100)
    worker_id: WorkerId | None = None
    wait_seconds: WorkerWaitSeconds | None = None
    model: ModelId | None = None

    @model_validator(mode="after")
    def normalize_queues(self):
        self.queues = sorted(set(self.queues))
        return self


class ListQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue: Name | None = None
    status: JobStatus | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    q: str | None = Field(default=None, max_length=200)
