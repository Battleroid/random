"""
Find all empties on cluster that are not a write index, then delete them.
"""

from getpass import getuser, getpass
from colorama import Style, Fore, Back, init
import requests
import re
import time
import argparse


def main(args):

    init()

    # Setup
    auth = (args.username, args.password or getpass())
    for_real = args.for_real
    list_mode = args.list
    api = args.api

    # Get all empties
    empties = set()
    indices = requests.get(
        f"{api}/_cat/indices",
        params={"h": "index,docs.count", "format": "json"},
        auth=auth,
    ).json()
    for blob in indices:
        index = blob["index"]
        if not blob["docs.count"]:
            continue
        docs_count = int(blob["docs.count"])
        if docs_count == 0:
            empties.add(index)

    # Determine if they are the write index
    aliases = requests.get(
        f"{api}/_aliases", params={"filter_path": "**.is_write_index"}, auth=auth
    ).json()
    for index, alias_body in aliases.items():
        if any(x in index for x in [".kibana", ".tasks"]):
            continue
        if index not in empties:
            continue
        alias_name = next(iter(alias_body["aliases"]))
        is_write_index = alias_body["aliases"][alias_name]["is_write_index"]
        if is_write_index:
            empties.remove(index)
            print(f"{Fore.YELLOW}{index} is the write index, skipping{Style.RESET_ALL}")
        else:
            print(
                f"{Fore.GREEN}Keeping {index}, empty and not write index{Style.RESET_ALL}"
            )

    if not empties:
        print("Empties list empty!")
        return

    if list_mode:
        print()
        print(f"Found the following {len(empties)} empties:")
        print("---")
        for index in empties:
            print(index)
        return

    # Remove them in bulk batches
    empties = list(empties)
    bulks = [empties[x : x + 10] for x in range(0, len(empties), 10)]
    print(
        f"{Back.RED}{Fore.BLACK}Will delete {len(empties)} indices over {len(bulks)} bulks (of 10){Style.RESET_ALL}"
    )
    if for_real:
        for bulk in bulks:
            print(f'Deleting {", ".join(bulk)}')
            bulk_str = ",".join(bulk)
            requests.delete(f"{api}/{bulk_str}", auth=auth, timeout=360)
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--for-real", default=False, action="store_true", help="Do it for real"
    )
    parser.add_argument("-u", "--username", default=getuser(), help="es username")
    parser.add_argument("-p", "--password", default=None, help="es password")
    parser.add_argument("-l", "--list", default=False, action="store_true")
    parser.add_argument("api")
    args = parser.parse_args()
    main(args)
