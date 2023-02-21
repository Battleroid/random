"""
Calculates the average doc size per shard for an index. Gives you an idea of how complex or dense an index may be.
"""


import argparse
import re
import statistics
from getpass import getpass, getuser

import bitmath
import requests
import tabulate


def main(args):

    # Setup
    auth = (args.es_user, args.es_pass or getpass())
    api = args.API.rstrip("/")

    # Add up sizes
    alias_pattern = re.compile("(.*)\-\d{4}\.\d{2}\.\d{2}\-\d{6}$")
    sizes = {}
    for item in requests.get(
        f"{api}/_cat/shards", params={"format": "json", "bytes": "b"}, auth=auth
    ).json():
        if item["index"].startswith("."):
            print(f'Ignoring {item["index"]}')
            continue
        if item["state"] != "STARTED":
            continue
        if item["prirep"] == "r":
            continue
        if item["docs"] == "0" or not item["docs"] or not item["store"]:
            continue
        alias = alias_pattern.findall(item["index"])[0]
        sizes.setdefault(alias, [])
        size = int(item["store"]) / int(item["docs"])
        sizes[alias].append(size)

    averages = []
    for item, item_sizes in sizes.items():
        avg = statistics.mean(item_sizes)
        averages.append([item, bitmath.Byte(avg), avg])

    averages = [
        (x[0], x[1].best_prefix().format("{value:.02f} {unit}"))
        for x in sorted(averages, key=lambda x: x[2], reverse=True)
    ]

    print(
        tabulate.tabulate(
            averages, headers=["Index/Alias", "Average Size per Doc"], tablefmt="presto"
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("API")
    parser.add_argument("-u", "--es-user", default=getuser())
    parser.add_argument("-p", "--es-pass", default=None)
    args = parser.parse_args()
    main(args)
