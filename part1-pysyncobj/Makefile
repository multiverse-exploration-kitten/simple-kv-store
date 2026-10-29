# Developer and chaos commands for part 1. Run them from part1-pysyncobj/.
# `make` alone prints the list of targets.

.DEFAULT_GOAL := help
.PHONY: help install lint format test e2e up down logs status kill-leader \
	partition heal pause unpause start

# docker-compose.yml sets the project name `kvstore-part1`. Compose names the network <project>_raft.
RAFT_NETWORK := kvstore-part1_raft
# Static IPs as in docker-compose.yml: nodeN has <prefix>.1N.
SUBNET_PREFIX := $(or $(KV_SUBNET_PREFIX),172.28.1)
NODE_IP = $(SUBNET_PREFIX).1$(subst node,,$(NODE))
NODE_CONTAINER = $$(docker compose ps --all --quiet $(NODE))

require_node = @test -n "$(NODE)" || { echo "Set NODE, for example: make $@ NODE=node1" >&2; exit 1; }

# Reads /status of the 3 nodes on the host ports.
# Argument "status": print one line per node. Argument "leader": print the service name of the leader.
define CLUSTER_SCRIPT
import json, os, sys, urllib.request

leaders = []
for number in (1, 2, 3):
    service = f"node{number}"
    port = os.environ.get(f"KV_NODE{number}_HTTP_PORT", str(8000 + number))
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/status", timeout=2) as response:
            status = json.load(response)
    except OSError as error:
        if sys.argv[1] == "status":
            print(f"{service}: unreachable ({error})")
        continue
    if status["state"] == "leader":
        leaders.append((status["term"], service))
    if sys.argv[1] == "status":
        print(
            f"{service}: state={status['state']} leader={status['leader']} term={status['term']}"
            f" commit={status['commit_index']} applied={status['last_applied']}"
            f" ready={status['ready']} quorum={status['has_quorum']}"
        )
if sys.argv[1] == "leader":
    if not leaders:
        sys.exit("no node reports itself as leader")
    print(max(leaders)[1])
endef
export CLUSTER_SCRIPT

help: ## Show this list
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

install: ## Install the dependencies into .venv
	uv sync

lint: ## Check the code with ruff (lint and format)
	uv run ruff check .
	uv run ruff format --check .

format: ## Format the code with ruff
	uv run ruff format .

test: ## Run the unit and cluster tests
	uv run pytest

e2e: ## Run the e2e tests against their own Docker cluster
	uv run pytest -m docker

up: ## Build and start the 3-node cluster (HTTP on 8001-8003)
	docker compose up -d --build --wait

down: ## Stop the cluster and delete its data volumes
	docker compose down -v

logs: ## Follow the logs (all nodes, or one node with NODE=nodeN)
	docker compose logs --follow --tail 100 $(NODE)

status: ## Print the Raft status of each node, one line each
	@uv run python -c "$$CLUSTER_SCRIPT" status

kill-leader: ## Kill the current leader with SIGKILL
	@leader=$$(uv run python -c "$$CLUSTER_SCRIPT" leader) \
		&& echo "Killing the leader $$leader" \
		&& docker compose kill --signal SIGKILL $$leader

partition: ## Cut NODE off the Raft network, for example: make partition NODE=node1
	$(require_node)
	docker network disconnect $(RAFT_NETWORK) $(NODE_CONTAINER)

heal: ## Connect NODE again with its static IP and DNS name, for example: make heal NODE=node1
	$(require_node)
	docker network connect --ip $(NODE_IP) --alias $(NODE) $(RAFT_NETWORK) $(NODE_CONTAINER)

pause: ## Freeze NODE (docker pause), for example: make pause NODE=node2
	$(require_node)
	docker compose pause $(NODE)

unpause: ## Unfreeze NODE, for example: make unpause NODE=node2
	$(require_node)
	docker compose unpause $(NODE)

start: ## Start a stopped or killed NODE, for example: make start NODE=node1
	$(require_node)
	docker compose start $(NODE)
