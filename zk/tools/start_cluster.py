"""Start every server from a config file as separate processes.

    python -m zk.tools.start_cluster                    # 3 servers (cluster.json)
    python -m zk.tools.start_cluster --config cluster5.json

All logs appear in this window, each line tagged with the server id.
Press Ctrl+C to stop them all. To kill a single server (e.g. the leader)
for a demo, run the servers in separate terminals instead:
    python -m zk.server --id 1
"""

import argparse
import subprocess
import sys
import time

from zk.common.config import ClusterConfig


def main():
    p = argparse.ArgumentParser(description="Start a local cluster")
    p.add_argument("--config", default="cluster.json")
    p.add_argument("--data-dir", default="data")
    args = p.parse_args()

    config = ClusterConfig.load(args.config)
    procs = {}
    for sid in config.servers:
        cmd = [sys.executable, "-m", "zk.server", "--id", str(sid),
               "--config", args.config, "--data-dir", args.data_dir]
        procs[sid] = subprocess.Popen(cmd)
        print(f"started S{sid} (pid {procs[sid].pid})")

    try:
        while all(proc.poll() is None for proc in procs.values()):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            proc.wait()
        print("cluster stopped")


if __name__ == "__main__":
    main()
