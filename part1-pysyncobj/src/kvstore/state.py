"""The key-value state machine data.

Raft applies the same commands in the same order on every node. So every method here
must be deterministic: no clock, no random numbers, no I/O (AGENTS.md, Raft rule 1).
"""

from dataclasses import dataclass, field


@dataclass
class KeyValueState:
    values: dict[str, str] = field(default_factory=dict)

    def put(self, key: str, value: str) -> None:
        self.values[key] = value

    def delete(self, key: str) -> bool:
        """Return True if the key existed."""
        return self.values.pop(key, None) is not None

    def compare_and_set(self, key: str, expected_value: str | None, new_value: str) -> bool:
        """Set the key only if its current value equals expected_value.

        expected_value=None means "the key must not exist yet".
        """
        if self.values.get(key) != expected_value:
            return False
        self.values[key] = new_value
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)
