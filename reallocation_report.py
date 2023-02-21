"""
Guesstimate the results of chopping nodes.
"""

import requests
import argparse
import random
from getpass import getpass, getuser
import bitmath


def main(args):

    # Setup
    username = args.username
    password = args.password or getpass()
    auth = (username, password)
    url = args.URL
    node_query = args.node_query
    cut_off = args.cut_off_percentage

    if int(cut_off // 1) >= 1:
        raise SystemExit(f"Cannot use cut off of {cut_off * 100}%")

    # Get nodes, filter, get byte values
    nodes = requests.get(
        f"{url}/_cat/allocation", params={"bytes": "b", "format": "json"}, auth=auth
    ).json()
    if node_query:
        nodes = [n for n in nodes if node_query in n["node"]]
    for node in nodes:
        node["disk.used"] = bitmath.Byte(int(node["disk.used"]))
        node["disk.total"] = bitmath.Byte(int(node["disk.total"]))
        node["disk.percent"] = int(node["disk.percent"])
        node["shards"] = int(node["shards"])

    total_remove = int(len(nodes) * cut_off)
    total_disk_removed = bitmath.Byte(0)
    total_shards_removed = 0
    print(f"Would need to remove {total_remove} nodes")
    while total_remove > 0:
        total_remove -= 1
        random.shuffle(nodes)
        dead_node = nodes.pop()
        total_disk_removed += dead_node["disk.used"]
        total_shards_removed += dead_node["shards"]

    total_left = len(nodes)
    disk_per = total_disk_removed / total_left
    shards_per = total_shards_removed // total_left

    for node in nodes:
        node["disk.used"] += disk_per
        node["shards"] += shards_per
        node["disk.percent"] = int((node["disk.used"] / node["disk.total"]) * 100)

    # leftover shards go somewhere
    nodes[0]["shards"] += total_shards_removed % shards_per

    average_used = sum([n["disk.percent"] for n in nodes]) / total_left

    print(f"Would have to move:")
    print(f"  {total_shards_removed} shards")
    print(f"  {total_disk_removed.best_prefix()} in data")
    print(f"  {average_used}% average disk usage")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("URL")
    parser.add_argument("-u", "--username", default=getuser())
    parser.add_argument("-p", "--password")
    parser.add_argument("-n", "--node-query", default=None, help="node search term")
    parser.add_argument(
        "-c",
        "--cut-off-percentage",
        default=0.1,
        type=float,
        help="float value for how many nodes to cut off, e.g. 0.5 = 50%",
    )
    args = parser.parse_args()
    main(args)
