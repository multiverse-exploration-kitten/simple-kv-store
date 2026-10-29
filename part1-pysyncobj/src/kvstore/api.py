"""HTTP API of one node. Maps requests to KeyValueNode calls and errors to status codes."""

from typing import Any, Protocol

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvstore.errors import CommandOutcomeUnknownError, CommandRejectedError
from kvstore.node import ReadConsistency


class KeyValueNodeProtocol(Protocol):
    """The part of KeyValueNode that the API uses. Tests pass a fake."""

    def put(self, key: str, value: str) -> None: ...

    def delete(self, key: str) -> bool: ...

    def compare_and_set(self, key: str, expected_value: str | None, new_value: str) -> bool: ...

    def get(self, key: str, consistency: ReadConsistency = ...) -> str | None: ...

    def status(self) -> dict[str, Any]: ...

    def leader_address(self) -> str | None: ...

    def is_ready(self) -> bool: ...


class PutRequest(BaseModel):
    value: str


class CompareAndSetRequest(BaseModel):
    expected_value: str | None
    new_value: str


class PutResponse(BaseModel):
    key: str
    value: str


class GetResponse(BaseModel):
    key: str
    value: str
    consistency: ReadConsistency


class DeleteResponse(BaseModel):
    key: str
    deleted: bool


class CompareAndSetResponse(BaseModel):
    key: str
    succeeded: bool


class LiveResponse(BaseModel):
    status: str


def create_app(node: KeyValueNodeProtocol) -> FastAPI:
    app = FastAPI(title="kvstore node")

    @app.exception_handler(CommandRejectedError)
    def handle_command_rejected(request: Request, error: CommandRejectedError) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"error": "command_rejected", "reason": error.reason, "retry_safe": True},
            headers={"Retry-After": "1"},
        )

    @app.exception_handler(CommandOutcomeUnknownError)
    def handle_outcome_unknown(request: Request, error: CommandOutcomeUnknownError) -> JSONResponse:
        return JSONResponse(
            status_code=504,
            content={
                "error": "outcome_unknown",
                "detail": "the command may or may not be committed",
                "retry_safe": False,
            },
        )

    # Sync `def` endpoints: node calls block until commit, so FastAPI runs them in its threadpool.
    @app.put("/kv/{key}")
    def put_value(key: str, body: PutRequest) -> PutResponse:
        node.put(key, body.value)
        return PutResponse(key=key, value=body.value)

    @app.get("/kv/{key}")
    def get_value(
        key: str, consistency: ReadConsistency = ReadConsistency.LINEARIZABLE
    ) -> GetResponse:
        value = node.get(key, consistency)
        if value is None:
            raise HTTPException(status_code=404, detail="key not found")
        return GetResponse(key=key, value=value, consistency=consistency)

    @app.delete("/kv/{key}")
    def delete_value(key: str) -> DeleteResponse:
        return DeleteResponse(key=key, deleted=node.delete(key))

    @app.post("/kv/{key}/cas")
    def compare_and_set_value(key: str, body: CompareAndSetRequest) -> CompareAndSetResponse:
        succeeded = node.compare_and_set(key, body.expected_value, body.new_value)
        return CompareAndSetResponse(key=key, succeeded=succeeded)

    @app.get("/status")
    def get_status() -> dict[str, Any]:
        return node.status()

    @app.get("/health/live")
    def health_live() -> LiveResponse:
        return LiveResponse(status="ok")

    @app.get("/health/ready")
    def health_ready() -> JSONResponse:
        leader_address = node.leader_address()
        is_ready = leader_address is not None and node.is_ready()
        return JSONResponse(
            status_code=200 if is_ready else 503,
            content={"status": "ready" if is_ready else "not_ready", "leader": leader_address},
        )

    return app
