"""
Move shards from node to elsewhere with ease.

Requires $ pip install inquirer requests
"""


import argparse
from getpass import getuser, getpass

import requests
import inquirer


def _get_nodes_by_space(api, auth, node_filter=None, remove=None, sort="desc"):
    nodes = requests.get(
        f"{api}/_cat/allocation",
        params={
            "format": "json",
            "h": "disk.percent,shards,node",
            "s": f"disk.percent:{sort},shards,node",
        },
        auth=auth,
    ).json()
    rnodes = []
    for node in nodes:
        if node_filter:
            if not node_filter in node["node"]:
                continue
        if remove:
            if node["node"] in remove:
                continue
        node["shards"] = int(node["shards"])
        node["disk.percent"] = f'{node["disk.percent"]}%'
        rnodes.append(node)
    nodes_names = [n["node"] for n in rnodes]
    nodes_choices = [
        f'{n["disk.percent"]}, {n["shards"]} shards, {n["node"]}' for n in rnodes
    ]
    return nodes, nodes_names, nodes_choices


def main(args):

    # Setup
    api = args.API
    for_real = args.for_real
    index_filter = args.index_filter
    node_filter = args.node_filter
    type_filter = args.type
    auth = (args.es_user, args.es_pass or getpass())
    requests.get(api, auth=auth).raise_for_status()

    # Get nodes list
    nodes, nodes_names, nodes_choices = _get_nodes_by_space(
        api, auth, node_filter=node_filter
    )
    nodes_q = inquirer.Checkbox(
        "picked_nodes",
        message="Which nodes do you want to move shards from? (space to select, enter to finish)",
        choices=nodes_choices,
    )
    picked_nodes = [
        n.split(",")[-1].strip() for n in inquirer.prompt([nodes_q])["picked_nodes"]
    ]

    # For each node, get its shards, pick shards to move
    # After all shards are picked, for each shard pick the destination node
    index_filter_str = ""
    if index_filter:
        index_filter_str = f"/{index_filter}"
    cat_shards = requests.get(
        f"{api}/_cat/shards{index_filter_str}",
        params={
            "format": "json",
            "h": "index,store,shard,prirep,node",
            "s": "store:desc",
        },
        auth=auth,
    ).json()
    shards = {}
    for entry in cat_shards:
        node = entry.pop("node")
        if node not in picked_nodes:
            continue
        shards.setdefault(node, [])
        shards[node].append(entry)

    for src_node in picked_nodes:
        moves = []
        node_shard_choices = []
        for shard in shards[src_node]:
            if type_filter != "*":
                if shard["prirep"] != type_filter:
                    continue
            node_shard_choices.append(
                f'{shard["index"]}, #{shard["shard"]}, ({shard["prirep"]}) {shard["store"]}'
            )
        shards_q = inquirer.Checkbox(
            "picked_shards",
            message=f"Move which shards from {src_node}? (space to select, enter to finish)",
            choices=node_shard_choices,
        )
        picked_shards = inquirer.prompt([shards_q])["picked_shards"]

        for shard in picked_shards:
            # Queue all moves from the source node
            nodes, nodes_names, nodes_choices = _get_nodes_by_space(
                api, auth, remove=[src_node], sort="asc"
            )
            dest_node_q = inquirer.List(
                "dest_node",
                message=f"Where to send {shard} from {src_node}? (enter to select)",
                choices=nodes_choices,
            )
            dest_node = (
                inquirer.prompt([dest_node_q])["dest_node"].split(",")[-1].strip()
            )
            shard_index = shard.split(",")[0]
            shard_shard = int(shard.split(",")[1].replace("#", "").strip())
            moves.append(
                {
                    "move": {
                        "index": shard_index,
                        "shard": shard_shard,
                        "from_node": src_node,
                        "to_node": dest_node,
                    }
                }
            )

        # Issue moves for the source node
        print()
        print(f"Issuing the following moves:")
        for move in moves:
            md = move["move"]
            print(
                f'* {md["index"]} #{md["shard"]} | {md["from_node"]} -> {md["to_node"]}'
            )

        actions = {"commands": [*moves]}

        if for_real:
            try:
                rv = requests.post(f"{api}/_cluster/reroute", auth=auth, json=actions)
                rv.raise_for_status()
            except Exception as e:
                import json

                print(
                    "Problem submitting reroute actions, saving actions to failed_moves.json:"
                )
                print(e)
                print(json.dumps(rv.json(), indent=2, sort_keys=True))
                with open("failed_moves.json", "w") as f:
                    f.write(json.dumps(actions, indent=2, sort_keys=True))

        print(f'Sent reroutes for {src_node} {"(not really)" if not for_real else ""}')
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("API")
    parser.add_argument(
        "--node-filter",
        default=None,
        help="node filter (for initial choices, not for moving)",
    )
    parser.add_argument("--index-filter", default=None, help="index filter")
    parser.add_argument("--es-user", default=getuser())
    parser.add_argument("--es-pass", default=None)
    parser.add_argument("--for-real", default=False, action="store_true")
    parser.add_argument(
        "-t", "--type", choices=["*", "r", "p"], default="*", help="replica or primary"
    )
    args = parser.parse_args()
    main(args)
