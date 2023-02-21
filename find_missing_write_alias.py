"""
Simple script that will scroll through all aliases/indices patterns and try to locate any that do not have a set write index for the entire set.
"""


from getpass import getpass, getuser
import requests
import argparse


def main(args):

    auth = (args.username, args.password or getpass())
    api = args.api.rstrip("/")
    ignore = args.ignore
    requests.get(api, auth=auth).raise_for_status()

    # Get all aliases, check if each set has at
    # least one index with a write index set to true.
    alias_write_status = {}
    current_aliases = requests.get(f"{api}/_alias", auth=auth).json()
    for index, index_config in current_aliases.items():
        for alias, alias_config in index_config["aliases"].items():
            if alias in ignore:
                continue
            alias_write_status.setdefault(alias, set())
            alias_write_status[alias].add(alias_config.get("is_write_index", False))

    for alias, is_or_not_set in alias_write_status.items():
        if True not in is_or_not_set:
            print(f"{alias} is missing is_write_index=True on one of its indices")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-u", "--username", default=getuser())
    parser.add_argument("-p", "--password", default=None)
    parser.add_argument(
        "-i",
        "--ignore",
        nargs="*",
        default=[
            ".kibana",
        ],
    )
    parser.add_argument("api")
    args = parser.parse_args()
    main(args)
