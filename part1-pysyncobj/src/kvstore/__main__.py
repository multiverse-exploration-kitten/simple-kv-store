"""Entry point: one Raft node plus its HTTP API in one process."""

import logging

import uvicorn

from kvstore.api import create_app
from kvstore.config import NodeConfig
from kvstore.node import KeyValueNode


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = NodeConfig.from_env()
    node = KeyValueNode(config)
    try:
        # One process and one worker: each worker would start its own Raft node (Raft rule 8).
        uvicorn.run(create_app(node), host=config.http_host, port=config.http_port)
    finally:
        node.close()


if __name__ == "__main__":
    main()
