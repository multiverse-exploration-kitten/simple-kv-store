"""Network fault injection for in-process pysyncobj nodes.

All nodes of a test cluster share one NetworkFaults object. Each node uses a TCPTransport
subclass that drops Raft messages on blocked links, on send and on receive. The TCP
connection itself stays open, so a block acts like packet loss, not like a closed port.
"""

import threading
from collections.abc import Iterable

from pysyncobj.transport import TCPTransport


class NetworkFaults:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._blocked_links: set[tuple[str, str]] = set()

    def block(self, source_address: str, destination_address: str) -> None:
        """Drop every message from source to destination (one-way)."""
        with self._lock:
            self._blocked_links.add((source_address, destination_address))

    def partition(self, group_a: Iterable[str], group_b: Iterable[str]) -> None:
        """Drop every message between the two groups, in both directions."""
        group_b = list(group_b)
        for address_a in group_a:
            for address_b in group_b:
                self.block(address_a, address_b)
                self.block(address_b, address_a)

    def heal(self) -> None:
        with self._lock:
            self._blocked_links.clear()

    def is_blocked(self, source_address: str | None, destination_address: str | None) -> bool:
        with self._lock:
            return (source_address, destination_address) in self._blocked_links

    def transport_class(self) -> type[TCPTransport]:
        faults = self

        class FaultInjectingTransport(TCPTransport):
            def send(self, node, message):
                if faults.is_blocked(self._selfNode.address, getattr(node, "address", None)):
                    return True  # the message is lost on the network
                return super().send(node, message)

            def _onMessageReceived(self, node, message):  # noqa: N802 (pysyncobj method name)
                if faults.is_blocked(getattr(node, "address", None), self._selfNode.address):
                    return  # the message was in flight when the link failed
                super()._onMessageReceived(node, message)

        return FaultInjectingTransport
