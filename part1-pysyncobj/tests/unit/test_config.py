from pathlib import Path

import pytest

from kvstore.config import NodeConfig, RaftTimings


def test_from_env_reads_all_values():
    config = NodeConfig.from_env(
        {
            "KV_SELF_ADDRESS": "node1:4321",
            "KV_PEERS": "node1:4321, node2:4321,node3:4321",
            "KV_BIND_ADDRESS": "0.0.0.0:4321",
            "KV_DATA_DIR": "/data",
            "KV_HTTP_HOST": "127.0.0.1",
            "KV_HTTP_PORT": "9000",
            "KV_COMMAND_TIMEOUT_SECONDS": "1.5",
        }
    )
    assert config.self_address == "node1:4321"
    assert config.peer_addresses == ("node2:4321", "node3:4321")
    assert config.bind_address == "0.0.0.0:4321"
    assert config.data_dir == Path("/data")
    assert config.http_host == "127.0.0.1"
    assert config.http_port == 9000
    assert config.command_timeout_seconds == 1.5


def test_from_env_uses_defaults_for_optional_values():
    config = NodeConfig.from_env({"KV_SELF_ADDRESS": "node1:4321"})
    assert config.peer_addresses == ()
    assert config.bind_address is None
    assert config.data_dir is None
    assert config.http_port == 8000
    assert config.timings == RaftTimings()


def test_from_env_requires_self_address():
    with pytest.raises(ValueError, match="KV_SELF_ADDRESS"):
        NodeConfig.from_env({})


def test_test_timings_keep_heartbeat_well_below_election_timeout():
    timings = RaftTimings.for_tests()
    assert timings.heartbeat_interval * 3 < timings.election_timeout_min
    assert timings.election_timeout_min < timings.election_timeout_max


def test_from_env_reads_raft_password():
    config = NodeConfig.from_env({"KV_SELF_ADDRESS": "node1:4321", "KV_RAFT_PASSWORD": "s3cret"})
    assert config.raft_password == "s3cret"


def test_from_env_treats_empty_raft_password_as_none():
    assert NodeConfig.from_env({"KV_SELF_ADDRESS": "n:1"}).raft_password is None
    assert (
        NodeConfig.from_env({"KV_SELF_ADDRESS": "n:1", "KV_RAFT_PASSWORD": ""}).raft_password
        is None
    )
