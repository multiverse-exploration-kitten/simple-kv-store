"""API tests with a fake node. No Raft here: only routing, bodies and error mapping."""

from typing import Any

import pytest
from fastapi.testclient import TestClient

from kvstore.api import create_app
from kvstore.errors import CommandOutcomeUnknownError, CommandRejectedError
from kvstore.node import ReadConsistency


class FakeNode:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.stale_data: dict[str, str] = {}
        self.error_to_raise: Exception | None = None
        self.leader: str | None = "node1:4321"
        self.ready = True

    def _maybe_fail(self) -> None:
        if self.error_to_raise is not None:
            raise self.error_to_raise

    def put(self, key: str, value: str) -> None:
        self._maybe_fail()
        self.data[key] = value

    def delete(self, key: str) -> bool:
        self._maybe_fail()
        return self.data.pop(key, None) is not None

    def compare_and_set(self, key: str, expected_value: str | None, new_value: str) -> bool:
        self._maybe_fail()
        if self.data.get(key) != expected_value:
            return False
        self.data[key] = new_value
        return True

    def get(
        self, key: str, consistency: ReadConsistency = ReadConsistency.LINEARIZABLE
    ) -> str | None:
        self._maybe_fail()
        source = self.stale_data if consistency is ReadConsistency.STALE else self.data
        return source.get(key)

    def status(self) -> dict[str, Any]:
        return {"address": "node1:4321", "state": "leader"}

    def leader_address(self) -> str | None:
        return self.leader

    def is_ready(self) -> bool:
        return self.ready


@pytest.fixture
def fake_node() -> FakeNode:
    return FakeNode()


@pytest.fixture
def client(fake_node: FakeNode) -> TestClient:
    return TestClient(create_app(fake_node))


def test_put_then_get_linearizable_by_default(client: TestClient) -> None:
    put_response = client.put("/kv/color", json={"value": "blue"})
    assert put_response.status_code == 200
    assert put_response.json() == {"key": "color", "value": "blue"}

    get_response = client.get("/kv/color")
    assert get_response.status_code == 200
    assert get_response.json() == {"key": "color", "value": "blue", "consistency": "linearizable"}


def test_get_stale_reads_local_state(client: TestClient, fake_node: FakeNode) -> None:
    fake_node.data["color"] = "blue"
    fake_node.stale_data["color"] = "old"

    response = client.get("/kv/color", params={"consistency": "stale"})

    assert response.status_code == 200
    assert response.json() == {"key": "color", "value": "old", "consistency": "stale"}


def test_get_missing_key_returns_404(client: TestClient) -> None:
    response = client.get("/kv/missing")
    assert response.status_code == 404
    assert response.json() == {"detail": "key not found"}


def test_get_rejects_unknown_consistency(client: TestClient) -> None:
    assert client.get("/kv/color", params={"consistency": "fast"}).status_code == 422


def test_delete_reports_whether_key_existed(client: TestClient, fake_node: FakeNode) -> None:
    fake_node.data["color"] = "blue"
    assert client.delete("/kv/color").json() == {"key": "color", "deleted": True}
    assert client.delete("/kv/color").json() == {"key": "color", "deleted": False}


def test_cas_succeeds_and_fails(client: TestClient, fake_node: FakeNode) -> None:
    fake_node.data["counter"] = "1"

    success = client.post("/kv/counter/cas", json={"expected_value": "1", "new_value": "2"})
    failure = client.post("/kv/counter/cas", json={"expected_value": "1", "new_value": "3"})

    assert success.json() == {"key": "counter", "succeeded": True}
    assert failure.json() == {"key": "counter", "succeeded": False}
    assert fake_node.data["counter"] == "2"


def test_cas_with_null_expected_value_creates_key(client: TestClient, fake_node: FakeNode) -> None:
    response = client.post("/kv/new/cas", json={"expected_value": None, "new_value": "first"})
    assert response.json() == {"key": "new", "succeeded": True}
    assert fake_node.data == {"new": "first"}


def test_status_returns_node_status(client: TestClient) -> None:
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json() == {"address": "node1:4321", "state": "leader"}


def test_health_live(client: TestClient) -> None:
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_ready_when_leader_known_and_ready(client: TestClient) -> None:
    assert client.get("/health/ready").status_code == 200


def test_health_ready_503_without_leader(client: TestClient, fake_node: FakeNode) -> None:
    fake_node.leader = None
    assert client.get("/health/ready").status_code == 503


def test_health_ready_503_when_not_ready(client: TestClient, fake_node: FakeNode) -> None:
    fake_node.ready = False
    assert client.get("/health/ready").status_code == 503


REQUESTS_THAT_REACH_THE_NODE = [
    ("put", "/kv/k", {"json": {"value": "v"}}),
    ("get", "/kv/k", {}),
    ("delete", "/kv/k", {}),
    ("post", "/kv/k/cas", {"json": {"expected_value": None, "new_value": "v"}}),
]


@pytest.mark.parametrize(("method", "path", "options"), REQUESTS_THAT_REACH_THE_NODE)
def test_rejected_command_returns_503_with_retry_after(
    client: TestClient, fake_node: FakeNode, method: str, path: str, options: dict[str, Any]
) -> None:
    fake_node.error_to_raise = CommandRejectedError("no_leader")

    response = getattr(client, method)(path, **options)

    assert response.status_code == 503
    assert response.json() == {
        "error": "command_rejected",
        "reason": "no_leader",
        "retry_safe": True,
    }
    assert response.headers["Retry-After"] == "1"


@pytest.mark.parametrize(("method", "path", "options"), REQUESTS_THAT_REACH_THE_NODE)
def test_unknown_outcome_returns_504(
    client: TestClient, fake_node: FakeNode, method: str, path: str, options: dict[str, Any]
) -> None:
    fake_node.error_to_raise = CommandOutcomeUnknownError("timeout")

    response = getattr(client, method)(path, **options)

    assert response.status_code == 504
    assert response.json() == {
        "error": "outcome_unknown",
        "detail": "the command may or may not be committed",
        "retry_safe": False,
    }
