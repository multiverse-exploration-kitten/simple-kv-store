"""Drive the Docker Compose cluster from tests: the docker CLI for faults, httpx for the API."""

import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx

PART_DIRECTORY = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PART_DIRECTORY / "docker-compose.yml"
SERVICES = ("node1", "node2", "node3")
# docker-compose.yml gives nodeN the static IP <subnet prefix>.1N.
STATIC_HOST_NUMBERS = {"node1": 11, "node2": 12, "node3": 13}

# Docker runs the production Raft timings, not the fast test timings.
DEFAULT_WAIT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.2
STATUS_TIMEOUT_SECONDS = 2.0
# The node gives up a command after 3 s (KV_COMMAND_TIMEOUT_SECONDS), so 10 s is a safe bound.
REQUEST_TIMEOUT_SECONDS = 10.0


def wait_until[T](
    probe: Callable[[], T], description: str, timeout: float = DEFAULT_WAIT_SECONDS
) -> T:
    """Call probe until it returns a truthy value. Return that value.

    A failed wait raises AssertionError that names the condition and the last probe error.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return result
        except Exception as error:  # the cluster is in transition, so probes can fail
            last_error = error
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"timed out after {timeout}s waiting for: {description} (last error: {last_error!r})"
    )


def service_of(raft_address: str) -> str:
    """'node2:4321' -> 'node2'."""
    return raft_address.rsplit(":", 1)[0]


class DockerCluster:
    """One Compose project with 3 nodes. Its own project name, host ports and subnet."""

    def __init__(
        self, project_name: str, http_ports: Mapping[str, int], subnet_prefix: str
    ) -> None:
        self.project_name = project_name
        self._http_ports = dict(http_ports)
        self._subnet_prefix = subnet_prefix
        self._environment = {
            **os.environ,
            "KV_SUBNET_PREFIX": subnet_prefix,
            **{
                f"KV_{service.upper()}_HTTP_PORT": str(port) for service, port in http_ports.items()
            },
        }
        # No keep-alive: a pooled connection to a killed or restarted node would fail.
        self._http = httpx.Client(
            timeout=REQUEST_TIMEOUT_SECONDS, limits=httpx.Limits(max_keepalive_connections=0)
        )

    @property
    def network_name(self) -> str:
        return f"{self.project_name}_raft"

    def close(self) -> None:
        self._http.close()

    # --- docker -----------------------------------------------------------------------

    def compose(self, *arguments: str) -> str:
        return self._run(
            "docker",
            "compose",
            "--file",
            str(COMPOSE_FILE),
            "--project-name",
            self.project_name,
            *arguments,
        )

    def up(self) -> None:
        self.compose("up", "--detach", "--build", "--wait")

    def down(self) -> None:
        self.compose("down", "--volumes", "--remove-orphans")

    def kill(self, service: str) -> None:
        self.compose("kill", "--signal", "SIGKILL", service)

    def stop(self, *services: str) -> None:
        """Stop the services (SIGTERM, then SIGKILL after the grace period). No service: all."""
        self.compose("stop", *services)

    def start(self, *services: str) -> None:
        self.compose("start", *services)

    def pause(self, service: str) -> None:
        self.compose("pause", service)

    def unpause(self, service: str) -> None:
        self.compose("unpause", service)

    def disconnect(self, service: str) -> None:
        self._run("docker", "network", "disconnect", self.network_name, self.container_id(service))

    def connect(self, service: str) -> None:
        """Connect again with the same static IP and the service name as DNS alias.

        Without the alias, a peer that starts later cannot resolve the service name.
        """
        static_ip = f"{self._subnet_prefix}.{STATIC_HOST_NUMBERS[service]}"
        self._run(
            "docker",
            "network",
            "connect",
            "--ip",
            static_ip,
            "--alias",
            service,
            self.network_name,
            self.container_id(service),
        )

    def container_id(self, service: str) -> str:
        return self.compose("ps", "--all", "--quiet", service).strip()

    def container_state(self, service: str) -> str:
        """'running', 'paused', 'exited', ..."""
        return self._run(
            "docker", "inspect", "--format", "{{.State.Status}}", self.container_id(service)
        ).strip()

    def is_connected(self, service: str) -> bool:
        template = f'{{{{if index .NetworkSettings.Networks "{self.network_name}"}}}}yes{{{{end}}}}'
        output = self._run("docker", "inspect", "--format", template, self.container_id(service))
        return output.strip() == "yes"

    def _run(self, *command: str) -> str:
        completed = subprocess.run(
            command,
            cwd=PART_DIRECTORY,
            env=self._environment,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"{' '.join(command)} failed with exit code {completed.returncode}:\n"
                f"{completed.stdout}\n{completed.stderr}"
            )
        return completed.stdout

    # --- HTTP -------------------------------------------------------------------------

    def url(self, service: str) -> str:
        return f"http://127.0.0.1:{self._http_ports[service]}"

    def status(self, service: str) -> dict[str, Any]:
        response = self._http.get(f"{self.url(service)}/status", timeout=STATUS_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()

    def is_ready(self, service: str) -> bool:
        """GET /health/ready. The first call on a node runs a linearizable read (up to 3 s).

        /status shows "ready" only after this check passed once, so call this one.
        """
        return self._http.get(f"{self.url(service)}/health/ready").status_code == 200

    def put(self, service: str, key: str, value: str) -> httpx.Response:
        return self._http.put(f"{self.url(service)}/kv/{key}", json={"value": value})

    def get(self, service: str, key: str, consistency: str = "linearizable") -> httpx.Response:
        return self._http.get(f"{self.url(service)}/kv/{key}", params={"consistency": consistency})

    def read_value(self, service: str, key: str, consistency: str = "linearizable") -> str | None:
        """The value on success, else None (missing key, no quorum, unknown outcome)."""
        response = self.get(service, key, consistency)
        return response.json()["value"] if response.status_code == 200 else None

    # --- waits ------------------------------------------------------------------------

    def wait_for_leader(self, among: Sequence[str] = SERVICES) -> str:
        """Wait until the nodes in `among` agree on one leader and one term. Return the leader.

        The leader must be one of `among`. It must see itself as leader with a quorum,
        and every node in `among` must pass /health/ready (it has applied every committed write).
        """

        def agreed_leader() -> str | None:
            statuses = {service: self.status(service) for service in among}
            leader_addresses = {status["leader"] for status in statuses.values()}
            terms = {status["term"] for status in statuses.values()}
            if len(leader_addresses) != 1 or len(terms) != 1 or None in leader_addresses:
                return None
            leader = service_of(leader_addresses.pop())
            if leader not in statuses:
                return None
            leader_status = statuses[leader]
            if leader_status["state"] != "leader" or not leader_status["has_quorum"]:
                return None
            if not all(self.is_ready(service) for service in among):
                return None
            return leader

        try:
            return wait_until(agreed_leader, f"one leader that {', '.join(among)} agree on")
        except AssertionError as error:
            raise AssertionError(f"{error}\n{self.describe(among)}") from None

    def wait_for_value(
        self, service: str, key: str, value: str, consistency: str = "stale"
    ) -> None:
        wait_until(
            lambda: self.read_value(service, key, consistency) == value,
            f"{service} to return {key}={value!r} ({consistency} read)",
        )

    def restore(self) -> None:
        """Undo every fault: connect, unpause and start each node. Then wait for one leader."""
        for service in SERVICES:
            if not self.is_connected(service):
                self.connect(service)
            state = self.container_state(service)
            if state == "paused":
                self.unpause(service)
            elif state != "running":
                self.start(service)
        self.wait_for_leader()

    def describe(self, services: Sequence[str] = SERVICES) -> str:
        """One line per node, for failure messages."""
        lines = []
        for service in services:
            try:
                status = self.status(service)
                lines.append(
                    f"{service}: state={status['state']} leader={status['leader']}"
                    f" term={status['term']} ready={status['ready']}"
                    f" quorum={status['has_quorum']} commit={status['commit_index']}"
                )
            except Exception as error:
                lines.append(f"{service}: unreachable ({error!r})")
        return "\n".join(lines)
