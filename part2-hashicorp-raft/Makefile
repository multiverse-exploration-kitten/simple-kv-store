# Developer and chaos commands for part 2. Run them from part2-hashicorp-raft/.
# `make` alone prints the list of targets. The cluster targets use only curl and docker.

.DEFAULT_GOAL := help
.PHONY: help lint format test e2e up down logs status kill-leader \
	partition heal pause unpause start

# Use the installed Go. Never download another toolchain.
export GOTOOLCHAIN := local

# docker-compose.yml sets the project name kvstore-part2. Compose names the network <project>_raft.
RAFT_NETWORK := kvstore-part2_raft
# Static IPs as in docker-compose.yml: nodeN has <prefix>.1N.
SUBNET_PREFIX := $(or $(KV_SUBNET_PREFIX),172.28.11)
NODE_IP = $(SUBNET_PREFIX).1$(subst node,,$(NODE))
NODE_CONTAINER = $$(docker compose ps --all --quiet $(NODE))
# node:host port, as in docker-compose.yml.
HTTP_PORTS := node1:$(or $(KV_NODE1_HTTP_PORT),8101) \
	node2:$(or $(KV_NODE2_HTTP_PORT),8102) \
	node3:$(or $(KV_NODE3_HTTP_PORT),8103)

require_node = @test -n "$(NODE)" || { echo "Set NODE, for example: make $@ NODE=node1" >&2; exit 1; }

help: ## Show this list
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

lint: ## Check gofmt, go vet and staticcheck (the e2e package too)
	@unformatted=$$(gofmt -l .); \
		if [ -n "$$unformatted" ]; then echo "Run make format. Not formatted:"; echo "$$unformatted"; exit 1; fi
	go vet ./...
	go vet -tags e2e ./tests/e2e/
	go tool staticcheck ./...
	go tool staticcheck -tags e2e ./tests/e2e/

format: ## Format the Go code with gofmt
	gofmt -w .

test: ## Run the unit and cluster tests with the race detector
	go test -race ./...

e2e: ## Run the e2e tests against their own Docker cluster
	go test -tags e2e -count=1 -timeout 30m -v ./tests/e2e/

up: ## Build and start the 3-node cluster (HTTP on 8101-8103)
	docker compose up -d --build --wait

down: ## Stop the cluster and delete its data and certificate volumes
	docker compose down -v

logs: ## Follow the logs (all nodes, or one node with NODE=nodeN)
	docker compose logs --follow --tail 100 $(NODE)

status: ## Print the /status of each node, one line each
	@for entry in $(HTTP_PORTS); do \
		node=$${entry%%:*}; port=$${entry##*:}; \
		echo "$$node: $$(curl -s --max-time 2 http://127.0.0.1:$$port/status || echo unreachable)"; \
	done

# /health/leader: 200 on a ready leader, 503 on a leader before its barrier, 429 on a follower.
kill-leader: ## Kill the current leader with SIGKILL
	@leader=""; \
	for entry in $(HTTP_PORTS); do \
		node=$${entry%%:*}; port=$${entry##*:}; \
		code=$$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:$$port/health/leader); \
		if [ "$$code" = 200 ] || [ "$$code" = 503 ]; then leader=$$node; fi; \
	done; \
	if [ -z "$$leader" ]; then echo "No node reports itself as leader" >&2; exit 1; fi; \
	echo "Killing the leader $$leader"; \
	docker compose kill --signal SIGKILL $$leader

partition: ## Cut NODE off the Raft network, for example: make partition NODE=node1
	$(require_node)
	docker network disconnect $(RAFT_NETWORK) $(NODE_CONTAINER)

heal: ## Connect NODE again with its static IP, for example: make heal NODE=node1
	$(require_node)
	docker network connect --ip $(NODE_IP) $(RAFT_NETWORK) $(NODE_CONTAINER)

pause: ## Freeze NODE (docker pause), for example: make pause NODE=node2
	$(require_node)
	docker compose pause $(NODE)

unpause: ## Unfreeze NODE, for example: make unpause NODE=node2
	$(require_node)
	docker compose unpause $(NODE)

start: ## Start a stopped or killed NODE, for example: make start NODE=node1
	$(require_node)
	docker compose start $(NODE)
