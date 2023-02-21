"""
Similar to where_are_write_indices, ties write indices to what node based on the what shards live where. Not exact as multiple nodes may have harbor shards all from the same index.
"""

from getpass import getpass, getuser
from os import supports_effective_ids
import re
import argparse
import requests
import tabulate
import bitmath


# Flip flop since that's how exclude works
TIER_MAP = {"hot": "warm", "warm": "hot"}


def main(args):

    # Setup
    api = args.API
    node = args.NODE
    es_user = args.es_user
    es_pass = args.es_pass or getpass()
    auth = es_user, es_pass

    # Get write index -> alias
    write_aliases = {}
    alias_regex = re.compile(r"(.*)\-\d{4}\.\d{2}\.\d{2}\-\d{6}$")
    current_aliases = requests.get(f"{api}/_aliases", auth=auth)
    current_aliases.raise_for_status()
    for index, index_config in current_aliases.json().items():
        try:
            extracted_alias = alias_regex.findall(index)[0]
        except IndexError:
            print(f"{index} has no alias matching the rollover pattern")
            continue
        for alias, alias_config in index_config["aliases"].items():
            if extracted_alias == alias:
                if alias_config.get("is_write_index", True):
                    write_aliases[index] = alias

    # Get shard information filter towards node
    # TODO: double check that we have a list of shards that represent the write index to node
    # e.g. if shards on nodeA, nodeB then index is on nodeA & nodeB, not just whatever came last
    shards = requests.get(
        f"{api}/_cat/shards", params={"format": "json", "bytes": "b"}, auth=auth
    ).json()
    shard_to_nodes = []
    for item in shards:
        if node not in item["node"]:
            continue
        if not item["store"]:
            continue
        index = item["index"]
        shard = item["shard"]
        prirep = item["prirep"].upper()
        size = int(item["store"])
        shard_to_nodes.append(
            (
                index,
                shard,
                prirep,
                size,
            )
        )

    sorted_shard_to_nodes = sorted(
        shard_to_nodes, key=lambda i: (i[3], i[0], i[1], i[2]), reverse=True
    )
    for i in range(len(sorted_shard_to_nodes)):
        *row, size = sorted_shard_to_nodes[i]
        size = bitmath.Byte(size).best_prefix().format("{value:.02f} {unit}")
        sorted_shard_to_nodes[i] = [*row, size]

    # Show
    print(
        "\n"
        + tabulate.tabulate(
            sorted_shard_to_nodes, headers=("Index", "Shard #", "P/R?", "Size")
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("API")
    parser.add_argument("NODE")
    parser.add_argument("--es-user", default=getuser())
    parser.add_argument("--es-pass", default=None)
    args = parser.parse_args()
    main(args)
