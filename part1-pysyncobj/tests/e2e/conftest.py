import uuid
from collections.abc import Iterator

import pytest

from tests.e2e.docker_cluster import DockerCluster

# Own project name, host ports and subnet, so the e2e cluster can run next to `make up`.
E2E_PROJECT_NAME = "kvstore-part1-e2e"
E2E_HTTP_PORTS = {"node1": 18001, "node2": 18002, "node3": 18003}
E2E_SUBNET_PREFIX = "172.28.2"


@pytest.fixture(scope="session")
def docker_cluster() -> Iterator[DockerCluster]:
    """Build and start the cluster once per session. Delete it and its volumes at the end."""
    cluster = DockerCluster(E2E_PROJECT_NAME, E2E_HTTP_PORTS, E2E_SUBNET_PREFIX)
    try:
        cluster.down()  # remove what an interrupted earlier run left behind
        cluster.up()
        cluster.wait_for_leader()
        yield cluster
    finally:
        cluster.down()
        cluster.close()


@pytest.fixture
def cluster(docker_cluster: DockerCluster) -> Iterator[DockerCluster]:
    """The session cluster. After the test, undo every fault and wait for one leader."""
    yield docker_cluster
    docker_cluster.restore()


@pytest.fixture
def key() -> str:
    """A key that no other test uses, also across runs on the same volumes."""
    return f"key-{uuid.uuid4().hex[:12]}"
