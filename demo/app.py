"""Swagger-controlled scenarios for the local EVE streaming demo."""

import threading
import time
from typing import Any, Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from .scenarios import run_alert, run_load


app = FastAPI(
    title="EVE streaming demo scenarios",
    description="Publish controlled market observations to Redpanda and check Flink processing.",
    version="1.0.0",
)


class LoadRequest(BaseModel):
    """Choose the size and publication rate of a multi-key load run."""

    count: int = Field(default=5000, ge=100, le=100000)
    rate: float = Field(default=200, ge=0, le=5000, description="Events per second; zero sends as fast as possible")
    keys: int = Field(default=100, ge=1, le=10000)


class RunResponse(BaseModel):
    """Current result of a scenario triggered from Swagger."""

    run_id: str
    scenario: Literal["alert", "load"]
    state: Literal["queued", "running", "passed", "failed"]
    published: int = 0
    elapsed_seconds: float | None = None
    result: dict[str, Any] | None = None
    error: str | None = None


_runs: dict[str, RunResponse] = {}
_lock = threading.Lock()


def _start(scenario: Literal["alert", "load"], options: LoadRequest | None = None) -> RunResponse:
    with _lock:
        if any(run.state in {"queued", "running"} for run in _runs.values()):
            raise HTTPException(status_code=409, detail="A demo scenario is already running")
        run_id = uuid4().hex
        record = RunResponse(run_id=run_id, scenario=scenario, state="queued")
        _runs[run_id] = record
        while len(_runs) > 50:
            oldest = next(iter(_runs))
            del _runs[oldest]

    def work() -> None:
        started = time.monotonic()
        with _lock:
            record.state = "running"

        def progress(published: int) -> None:
            with _lock:
                record.published = published

        try:
            if scenario == "alert":
                result = run_alert(run_id, progress)
            else:
                assert options is not None
                result = run_load(run_id, options.count, options.rate, options.keys, progress)
            with _lock:
                record.result = result
                record.state = "passed" if result["passed"] else "failed"
        except Exception as error:
            with _lock:
                record.error = str(error)
                record.state = "failed"
        finally:
            with _lock:
                record.elapsed_seconds = round(time.monotonic() - started, 3)

    threading.Thread(target=work, name=f"demo-{scenario}-{run_id[:8]}", daemon=True).start()
    return record.model_copy(deep=True)


@app.get("/health", tags=["service"])
def health() -> dict[str, str]:
    """Report that the demo control API is running."""
    return {"status": "ok"}


@app.post("/scenarios/alert", response_model=RunResponse, status_code=status.HTTP_202_ACCEPTED, tags=["scenarios"])
def start_alert() -> RunResponse:
    """Send 90 baseline prices, one configured drop, then nine recoveries."""
    return _start("alert")


@app.post("/scenarios/load", response_model=RunResponse, status_code=status.HTTP_202_ACCEPTED, tags=["scenarios"])
def start_load(request: LoadRequest) -> RunResponse:
    """Publish many stable-price observations across market keys at a chosen rate."""
    return _start("load", request)


@app.get("/runs/{run_id}", response_model=RunResponse, tags=["results"])
def get_run(run_id: str) -> RunResponse:
    """Poll the run until its state is passed or failed."""
    with _lock:
        record = _runs.get(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Unknown run ID")
        return record.model_copy(deep=True)
