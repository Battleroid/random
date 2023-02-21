"""
Randomly index random junk to a cluster just for testing.
"""

import json
import argparse
import random
import time
from getpass import getpass

import requests


def main(args):

    # Setup
    index_count = args.index_count
    shard_bias = args.shard_bias
    shard_count = args.shard_count
    min_docs_per_bulk = min(args.min_docs_per_bulk, 500)
    continuous = False
    if args.wait_range:
        wait_min, wait_max = list(map(float, args.wait_range.split(",")))
    else:
        continuous = True
    auth = (args.username, args.password or getpass())
    url = args.url.rstrip("/")
    add_template = args.add_template

    if add_template:
        print(f"Adding template ...")
        refresh_template = {
            "index_patterns": ["test-*"],
            "settings": {
                "index.refresh_interval": "1s",
                "index.number_of_shards": shard_count,
                "index.number_of_replicas": 1,
                "index.routing.allocation.total_shards_per_node": None,
            },
            "mappings": {},
            "aliases": {},
            "order": 2147483647,
        }
        requests.delete(f"{url}/_template/test-refresh", auth=auth)
        rv = requests.put(
            f"{url}/_template/test-refresh", json=refresh_template, auth=auth
        )
        if rv.status_code not in [200, 201]:
            print(rv.content)
            rv.raise_for_status()

    midpoint = shard_count // 2
    shards_weights = list(range(1, midpoint + 1)) + list(range(1, midpoint + 1))[::-1]
    if shard_bias:
        print(f"Using shard bias for a curve of 1-{midpoint}!")

    # Until cancelled, just keep indexing
    bi = 0
    while True:
        bi += 1
        total = random.randint(min_docs_per_bulk, 500)
        docs = []
        shard_choices = list(
            random.choices(list(range(shard_count)), weights=shards_weights, k=total)
        )
        for i in range(total):

            index = "test-" + str(random.randint(0, index_count))
            action = {"index": {"_index": index, "_type": "doc"}}
            if shard_bias:
                action["index"]["_routing"] = shard_choices.pop()
            doc = {"message": f"hello world from {index}, message {i} from bulk {bi}"}
            docs.append(json.dumps(action, separators=(",", ":")))
            docs.append(json.dumps(doc, separators=(",", ":")))

        bulk = "\n".join(docs) + "\n"
        print(f"Sending bulk {bi} ({total} docs) ...")
        rv = requests.post(
            f"{url}/_bulk",
            data=bulk,
            auth=auth,
            headers={"Content-Type": "application/x-ndjson"},
        )
        if continuous:
            continue
        else:
            time.sleep(random.uniform(wait_min, wait_max))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--username", "-u", default=None, help="Basic auth username")
    parser.add_argument("--password", "-p", default=None, help="Basic auth password")
    parser.add_argument(
        "--add-template",
        default=False,
        action="store_true",
        help="Add refresh rate template",
    )
    parser.add_argument(
        "--index-count", "-i", default=1, type=int, help="Number of indices to spawn"
    )
    parser.add_argument(
        "--wait-range", "-r", default=None, help="Time to wait between bulks (e.g. 0,5)"
    )
    parser.add_argument(
        "--shard-bias",
        "-s",
        default=False,
        action="store_true",
        help="Use shard count to build basic weight curve for routing",
    )
    parser.add_argument(
        "--shard-count", "-c", default=32, type=int, help="Shard count for indices"
    )
    parser.add_argument(
        "--min-docs-per-bulk",
        "-b",
        default=1,
        type=int,
        help="Minimum doc count per bulk",
    )
    args = parser.parse_args()
    try:
        main(args)
    except KeyboardInterrupt:
        print("Stopping")
