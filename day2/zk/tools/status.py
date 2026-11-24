"""Show every server's role in one table; --watch refreshes it.

    python -m zk.tools.status                  # once
    python -m zk.tools.status --watch          # every 0.5 s, Ctrl+C to stop
"""

import argparse
import asyncio
import time

import grpc

from zk.common.config import ClusterConfig
from zk.common.zxid import zxid_str
from zk.proto import zk_pb2, zk_pb2_grpc


async def fetch(info):
    async with grpc.aio.insecure_channel(info.client_address) as channel:
        try:
            return await zk_pb2_grpc.ZooKeeperStub(channel).GetStatus(zk_pb2.Empty(), timeout=0.5)
        except grpc.aio.AioRpcError:
            return None


async def show(config):
    results = await asyncio.gather(*(fetch(i) for i in config.servers.values()))
    print(time.strftime("%H:%M:%S"))
    print(f"{'server':8}{'state':11}{'serving':9}{'leader':8}{'epoch':7}"
          f"{'applied zxid':14}{'znodes':8}{'sessions'}")
    for info, st in zip(config.servers.values(), results):
        if st is None:
            print(f"S{info.id:<7}DOWN")
            continue
        leader = f"S{st.leader_id}" if st.leader_id else "-"
        serving = "yes" if st.serving else "no"
        print(f"S{info.id:<7}{st.state:11}{serving:9}{leader:8}{st.current_epoch:<7}"
              f"{zxid_str(st.last_applied_zxid):14}{st.node_count:<8}{st.session_count}")
    print()


async def main_async(args):
    config = ClusterConfig.load(args.config)
    while True:
        await show(config)
        if not args.watch:
            return
        await asyncio.sleep(0.5)


def main():
    p = argparse.ArgumentParser(description="Cluster status table")
    p.add_argument("--config", default="cluster.json")
    p.add_argument("--watch", action="store_true")
    try:
        asyncio.run(main_async(p.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
