"""Tiny command-line client.

Examples (run from the project root):
    python -m zk.tools.cli create /app hello
    python -m zk.tools.cli create /app/task- job1 --sequential
    python -m zk.tools.cli get /app
    python -m zk.tools.cli set /app world --version 0
    python -m zk.tools.cli ls /
    python -m zk.tools.cli stat /app
    python -m zk.tools.cli delete /app
    python -m zk.tools.cli status
    python -m zk.tools.cli --server 127.0.0.1:2182 get /app     # ask one server
"""

import argparse
import asyncio

from zk.client import ZKClient
from zk.common.config import ClusterConfig
from zk.common.errors import ZKError
from zk.common.zxid import zxid_str


def build_parser():
    p = argparse.ArgumentParser(description="ZooKeeper-style command-line client")
    p.add_argument("--config", default="cluster.json", help="cluster config (for server list)")
    p.add_argument("--server", help="talk to this one server (host:port) only")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create", help="create a znode")
    c.add_argument("path")
    c.add_argument("data", nargs="?", default="")
    c.add_argument("--sequential", action="store_true")
    c.add_argument("--ephemeral", action="store_true")

    for name in ("get", "ls", "stat"):
        sub.add_parser(name).add_argument("path")

    s = sub.add_parser("set", help="set a znode's data")
    s.add_argument("path")
    s.add_argument("data")
    s.add_argument("--version", type=int, default=-1)

    d = sub.add_parser("delete", help="delete a znode")
    d.add_argument("path")
    d.add_argument("--version", type=int, default=-1)

    sub.add_parser("status", help="role of the connected server")
    return p


def print_stat(stat):
    print(f"  czxid={zxid_str(stat.czxid)}  mzxid={zxid_str(stat.mzxid)}  "
          f"version={stat.version}  cversion={stat.cversion}  "
          f"children={stat.num_children}  data_length={stat.data_length}  "
          f"ephemeral_owner={stat.ephemeral_owner}")


async def run(args):
    if args.server:
        servers = [args.server]
    else:
        servers = [s.client_address for s in ClusterConfig.load(args.config).servers.values()]

    async with ZKClient(servers) as zk:
        print(f"(connected to {zk.address})")
        if args.cmd == "create":
            path = await zk.create(args.path, args.data.encode(),
                                   ephemeral=args.ephemeral, sequential=args.sequential)
            print(f"created {path}")
        elif args.cmd == "get":
            data, stat = await zk.get_data(args.path)
            print(data.decode(errors="replace"))
            print_stat(stat)
        elif args.cmd == "set":
            print_stat(await zk.set_data(args.path, args.data.encode(), args.version))
        elif args.cmd == "ls":
            print(await zk.get_children(args.path))
        elif args.cmd == "stat":
            stat = await zk.exists(args.path)
            if stat:
                print_stat(stat)
            else:
                print("does not exist")
        elif args.cmd == "delete":
            await zk.delete(args.path, args.version)
            print(f"deleted {args.path}")
        elif args.cmd == "status":
            for k, v in (await zk.status()).items():
                print(f"  {k:15} {v}")


def main():
    args = build_parser().parse_args()
    try:
        asyncio.run(run(args))
    except ZKError as e:
        print(f"error: {type(e).__name__}: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
