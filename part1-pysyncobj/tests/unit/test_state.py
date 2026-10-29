from kvstore.state import KeyValueState


def test_put_then_get_returns_value():
    state = KeyValueState()
    state.put("color", "blue")
    assert state.get("color") == "blue"


def test_get_missing_key_returns_none():
    assert KeyValueState().get("missing") is None


def test_put_overwrites_existing_value():
    state = KeyValueState()
    state.put("color", "blue")
    state.put("color", "red")
    assert state.get("color") == "red"


def test_delete_reports_whether_key_existed():
    state = KeyValueState()
    state.put("color", "blue")
    assert state.delete("color") is True
    assert state.delete("color") is False
    assert state.get("color") is None


def test_compare_and_set_succeeds_when_expected_value_matches():
    state = KeyValueState()
    state.put("counter", "1")
    assert state.compare_and_set("counter", "1", "2") is True
    assert state.get("counter") == "2"


def test_compare_and_set_fails_and_keeps_value_when_expected_value_differs():
    state = KeyValueState()
    state.put("counter", "5")
    assert state.compare_and_set("counter", "1", "2") is False
    assert state.get("counter") == "5"


def test_compare_and_set_with_none_creates_only_missing_key():
    state = KeyValueState()
    assert state.compare_and_set("lock", None, "owner-a") is True
    assert state.compare_and_set("lock", None, "owner-b") is False
    assert state.get("lock") == "owner-a"


def test_same_commands_in_same_order_give_same_state_on_every_replica():
    """Raft lesson: replicas stay equal only if apply is deterministic."""
    commands = [("put", "a", "1"), ("put", "b", "2"), ("delete", "a"), ("cas", "b", "2", "3")]
    replicas = [KeyValueState() for _ in range(3)]
    for replica in replicas:
        for command in commands:
            match command:
                case ("put", key, value):
                    replica.put(key, value)
                case ("delete", key):
                    replica.delete(key)
                case ("cas", key, expected, new):
                    replica.compare_and_set(key, expected, new)
    assert all(replica.values == {"b": "3"} for replica in replicas)
