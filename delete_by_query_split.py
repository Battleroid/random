"""
Issue a delete by query against all indices in a pattern. For when it's too difficult to run a delete by query on an alias or group of indices.

E.g. you issue a query on an alias and get back that you have too many open search contexts.
"""


import argparse
import json
from getpass import getpass, getuser

import requests


def main(args):

    # Setup
    api = args.API.rstrip("/")
    query = json.load(args.QUERY_FILE)
    pattern = args.PATTERN
    auth = (args.es_user, args.es_pass or getpass())
    for_real = args.for_real
    requests.get(api, auth=auth).raise_for_status()

    if not query:
        raise Exception("Need query file with substance!")

    # Issue query across pattern in sequential order; get indices matching first
    indices = requests.get(
        f"{api}/_cat/indices/{pattern}",
        auth=auth,
        params={"h": "index", "expand_wildcards": "open"},
    ).text.splitlines()
    n_pad = len(str(len(indices)))
    print(f"Issuing query against {len(indices)} indices!")
    for i, index in enumerate(indices, 1):
        print(
            f"[{i:>{n_pad}}/{len(indices)}] Delete query on {index} ...",
            end="",
            flush=True,
        )
        if for_real:
            rv = requests.post(
                f"{api}/{index}/_delete_by_query",
                json=query,
                params={"scroll": "1m"},
                auth=auth,
            )
            if rv.status_code == 200:
                print(f" done!", flush=True)
            else:
                print(f" status: {rv.status_code}", flush=True)
        else:
            print(" dry run!", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("API")
    parser.add_argument("PATTERN")
    parser.add_argument("QUERY_FILE", type=argparse.FileType("r"))
    parser.add_argument("-u", "--es-user", default=getuser())
    parser.add_argument("-p", "--es-pass", default=None)
    parser.add_argument("--for-real", default=False, action="store_true")
    args = parser.parse_args()
    main(args)
