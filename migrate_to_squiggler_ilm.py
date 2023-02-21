"""
This should assign write aliases to all indices where needed, as well as assign the lifecycle information required. Assumes a cluster is NOT using ILM of any kind, still relying entirely on squiggler for lifecycle movement.

Does NOT turn ILM back on once it starts!
"""


import argparse
from collections import Counter
from getpass import getpass, getuser

import requests
import tabulate


def main(args):

    # Setup
    api = args.API.rstrip("/")
    auth = (args.es_user, args.es_pass or getpass())
    for_real = args.for_real
    lifecycle_policy = args.lifecycle_policy

    # Before anything, check to make sure ilm policy is present
    rv = requests.get(f"{api}/_ilm/policy/{lifecycle_policy}", auth=auth)
    if rv.status_code != 200:
        raise SystemExit(f'The lifecycle policy "{lifecycle_policy}" does not exist!')

    # Grab aliases, this will give us the current write alias
    current_aliases = requests.get(
        f"{api}/_cat/aliases", params={"format": "json", "h": "a,i"}, auth=auth
    ).json()
    aliases_mapping = {i["a"]: i["i"] for i in current_aliases}
    aliases_counter = Counter([i["a"] for i in current_aliases])
    for alias, count in aliases_counter.items():
        if alias.startswith("."):
            print(f"{alias} is dot prefixed, skipping ...")
            aliases_mapping.pop(alias, None)
        if count > 1:
            print(f"{alias} is attached to more than one index, skipping")
            aliases_mapping.pop(alias, None)

    # Show table of where we will attach indices
    print(f"Need to attach {len(aliases_mapping)} pairs!")
    print(
        tabulate.tabulate(
            sorted(aliases_mapping.items(), key=lambda i: i[0]),
            headers=["Alias", "To Attach To"],
            tablefmt="presto",
        )
    )

    # Stop ILM to avoid issues while iterating
    if for_real:
        print(f"Stopping ILM ...")
        requests.post(f"{api}/_ilm/stop", auth=auth).raise_for_status()

    # Attach and inject the lifecycle settings to each alias
    settings_body = {"index.lifecycle.name": lifecycle_policy}
    pad_len = max(map(len, aliases_mapping.keys()))
    total_aliases = len(aliases_mapping)
    total_aliases_len = len(str(total_aliases))
    for i, (alias, index) in enumerate(
        sorted(aliases_mapping.items(), key=lambda i: i[0]), 1
    ):
        print(
            f"[{i: <{total_aliases_len}}/{total_aliases}] Fixing {alias: <{pad_len}} -> {index} ... ",
            end="",
            flush=True,
        )
        if for_real:
            alias_body = {
                "actions": [
                    {"add": {"index": index, "alias": alias, "is_write_index": True}}
                ]
            }
            rvs = []
            rvs.append(
                requests.put(f"{api}/{index}/_settings", json=settings_body, auth=auth)
            )
            rvs.append(requests.post(f"{api}/_aliases", json=alias_body, auth=auth))
            good = all([rv.ok for rv in rvs])
            if not good:
                print(f"failed", end="\n", flush=True)
            else:
                print("done", end="\n", flush=True)
        else:
            print("dry run", end="\n", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("API")
    parser.add_argument("-u", "--es-user", default=getuser())
    parser.add_argument("-p", "--es-pass", default=None)
    parser.add_argument("--for-real", default=False, action="store_true")
    parser.add_argument(
        "-l",
        "--lifecycle-policy",
        default="default",
        help="lifecycle policy to add to write aliases",
    )
    args = parser.parse_args()
    main(args)
