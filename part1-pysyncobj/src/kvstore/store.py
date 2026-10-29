"""The replicated store. pysyncobj applies each @replicated method through the Raft log."""

from pysyncobj import SyncObjConsumer, replicated

from kvstore.state import KeyValueState


class ReplicatedKeyValueStore(SyncObjConsumer):
    """Each @replicated method is one Raft command.

    pysyncobj sends a command as (method number, arguments). It numbers the methods in
    sorted (ver, name) order. So a new ver=0 method, or a renamed one, changes the numbers of
    other methods, and a cluster with mixed code versions applies the wrong method. To add
    or change a command, give it a ver higher than every existing one and keep the old
    methods, so nodes can still replay old log entries (AGENTS.md, Raft rule 2).
    """

    def __init__(self) -> None:
        super().__init__()
        # pysyncobj puts every attribute set after super().__init__() into snapshots.
        self._state = KeyValueState()

    @replicated
    def put(self, key: str, value: str) -> None:
        self._state.put(key, value)

    @replicated
    def delete(self, key: str) -> bool:
        return self._state.delete(key)

    @replicated
    def compare_and_set(self, key: str, expected_value: str | None, new_value: str) -> bool:
        return self._state.compare_and_set(key, expected_value, new_value)

    @replicated
    def read(self, key: str) -> str | None:
        """A linearizable read. It goes through the log like a write, so it costs one entry.

        pysyncobj has no ReadIndex or lease read, so this is the only safe read it offers.
        """
        return self._state.get(key)

    def read_local(self, key: str) -> str | None:
        """A stale read from local memory. No Raft round trip. The value may be old."""
        return self._state.get(key)
