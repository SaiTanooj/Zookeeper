"""Start one server:  python -m zk.server --id 1 --config cluster.json"""

import argparse
import asyncio
import logging

from zk.common.config import ClusterConfig
from zk.server.server import ZKServer


def main():
    parser = argparse.ArgumentParser(description="Run one ZooKeeper-style server")
    parser.add_argument("--id", type=int, required=True, help="server id from the config file")
    parser.add_argument("--config", default="cluster.json", help="cluster config file")
    parser.add_argument("--data-dir", default="data", help="where servers keep their files")
    parser.add_argument("--verbose", action="store_true", help="show debug logs")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    config = ClusterConfig.load(args.config)
    server = ZKServer(args.id, config, args.data_dir)
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        print(f"S{args.id} shut down")


if __name__ == "__main__":
    main()
