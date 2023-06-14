"""
Lots of shit probably broke, allocate all of them at once and just rip the bandaid off.
"""


import argparse
import json
import time
from getpass import getpass
from random import choice

import requests


def main(args):

    # Setup
    api = args.API.rstrip("/")
    if args.es_user:
        auth = (args.es_user, args.es_pass or getpass())
    for_real = args.for_real
    debug = args.debug

    # nodes
    nodes = [
        n["name"]
        for n in requests.get(
            f"{api}/_cat/nodes", params={"format": "json", "h": "name"}, auth=auth
        ).json()
        if "master" not in n["name"]
    ]

    print(f'Node choices: {", ".join(nodes)}')

    # Get reds
    indices = {
        i["index"]: set()
        for i in requests.get(
            f"{api}/_cat/indices",
            params={"format": "json", "h": "health,index", "health": "red"},
            auth=auth,
        ).json()
    }
    all_shards = requests.get(
        f"{api}/_cat/shards",
        params={"format": "json", "h": "index,shard,prirep,state"},
        auth=auth,
    ).json()
    for item in all_shards:
        index, shard, prirep, state = (
            item["index"],
            int(item["shard"]),
            item["prirep"],
            item["state"],
        )
        if state != "UNASSIGNED" or index not in indices or prirep != "p":
            continue
        print(f"Found shard {index}:{shard}")
        indices[index].add(shard)

    for i, (index, shards) in enumerate(sorted(indices.items()), start=1):
        print(
            f'[{i}/{len(indices)}] Allocating for {index}:{",".join(map(str, shards))} ...',
            end="",
            flush=True,
        )
        stale_commands = [
            {
                "allocate_empty_primary": {
                    "index": index,
                    "shard": shard,
                    "node": choice(nodes),
                    "accept_data_loss": True,
                }
            }
            for shard in shards
        ]
        body = {"commands": stale_commands}

        if debug:
            print()
            print(f"Reroute commands:")
            print(json.dumps(body, sort_keys=True, indent=2))

        if for_real:
            rv = requests.post(f"{api}/_cluster/reroute", json=body, auth=auth)
            if rv.ok:
                print(f" submitted ok", flush=True)
            else:
                print(f" submitted error", flush=True)
                print(rv.content)
            time.sleep(0.25)
        else:
            print(f" submitted (dry run)", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("API")
    parser.add_argument("-u", "--es-user")
    parser.add_argument("-p", "--es-pass", default=None)
    parser.add_argument("--debug", default=False, action="store_true")
    parser.add_argument("--for-real", default=False, action="store_true")
    args = parser.parse_args()
    main(args)
