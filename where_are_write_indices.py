"""
Will attempt to tie where a write index may be. E.g. if on hot, the index will have a exclude warm setting. Optional index/tier filters available for checking specific items. You may also provide a list of indices to filter from a file.

Results can be sorted by physical primary store size.
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
    api = args.api
    es_user = args.es_user
    es_pass = args.es_pass or getpass()
    auth = es_user, es_pass
    tier_filter = args.tier
    index_filter = args.index
    filter_from_file = args.filter_from_file
    if filter_from_file:
        filter_from_file = open(filter_from_file).read().splitlines()
    sort_by_size = args.sort_by_size

    if tier_filter:
        print(f'Looking for indices on tier "{tier_filter}"')

    if index_filter:
        print(f'Looking for indices with substring "{index_filter}"')

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
        if index_filter:
            if index_filter not in extracted_alias:
                continue
        for alias, alias_config in index_config["aliases"].items():
            if extracted_alias == alias:
                if alias_config.get("is_write_index", True):
                    write_aliases[index] = alias

    if sort_by_size:
        print(f"Getting index sizes for sorting later")
        index_to_size = {
            item["index"]: int(item["pri.store.size"])
            for item in requests.get(
                f"{api}/_cat/indices",
                auth=auth,
                params={"h": "index,pri.store.size", "format": "json", "bytes": "b"},
            ).json()
            if item["pri.store.size"]
        }

    # Map index to settings
    index_to_tier = {}
    index_settings = requests.get(
        f"{api}/_settings",
        auth=auth,
        params={"filter_path": "**.routing.allocation.exclude.tier"},
    )
    index_settings.raise_for_status()
    for index, index_config in index_settings.json().items():
        if index not in write_aliases.keys():
            continue

        tier_excluded = TIER_MAP.get(
            index_config["settings"]["index"]["routing"]["allocation"]["exclude"][
                "tier"
            ]
        )
        if tier_filter:
            if tier_excluded != tier_filter:
                continue
        index_to_tier[index] = tier_excluded

    if filter_from_file:
        print(
            f"Using filtered indices from file, prior to filtering there is {len(index_to_tier)} indices"
        )
        index_to_tier = {
            i: t for i, t in index_to_tier.items() if i in filter_from_file
        }
        print(f"After filtering there are {len(index_to_tier)} indices")

    # Show
    sorted_index_to_tier = sorted(index_to_tier.items(), key=lambda d: (d[1], d[0]))
    if sort_by_size:
        sorted_index_to_tier = sorted(
            index_to_tier.items(), key=lambda d: index_to_size[d[0]], reverse=True
        )
        for i in range(len(sorted_index_to_tier)):
            index, value = sorted_index_to_tier[i]
            size = (
                bitmath.Byte(index_to_size[index])
                .best_prefix()
                .format("{value:.02f} {unit}")
            )
            row = (
                index,
                size,
                value,
            )
            sorted_index_to_tier[i] = row
    print(
        "\n"
        + tabulate.tabulate(sorted_index_to_tier, headers=("Index", "Size", "Tier"))
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("api")
    parser.add_argument("--es-user", default=getuser())
    parser.add_argument("--es-pass", default=None)
    parser.add_argument(
        "--filter-from-file", default=None, help="filter indices based on file contents"
    )
    parser.add_argument("--sort-by-size", default=False, action="store_true")
    parser.add_argument(
        "--index", default=None, help="filter indices with this substring"
    )
    parser.add_argument(
        "--tier",
        default=None,
        help="filter write indices based on tier, expects fixed tiers such as hot, warm, leave blank for no filter",
    )
    args = parser.parse_args()
    main(args)
