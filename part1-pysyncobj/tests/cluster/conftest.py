from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from tests.cluster.harness import Cluster


@pytest.fixture
def make_cluster(tmp_path: Path) -> Iterator[Callable[..., Cluster]]:
    """Factory for clusters with custom options. Every cluster stops at test end."""
    clusters: list[Cluster] = []

    def factory(**options) -> Cluster:
        cluster = Cluster(tmp_path / f"cluster-{len(clusters)}", **options)
        clusters.append(cluster)
        return cluster

    yield factory
    for cluster in clusters:
        cluster.close()


@pytest.fixture
def cluster(make_cluster: Callable[..., Cluster]) -> Cluster:
    """A 3-node cluster with the default test options."""
    return make_cluster()
