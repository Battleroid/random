"""
Automate fixing those absolutely annoying instances where ILM/Logstash/Squiggler screw up and you're left with a concrete index instead of a rollover index and alias.
"""


import argparse
import re
from getpass import getpass, getuser
from urllib.parse import quote_plus

import json
import time
import requests


def main(args):

    # Setup
    api = args.API.rstrip("/")
    for_real = args.for_real
    auth = (args.es_user, args.es_pass or getpass())
    verbose = args.verbose
    batch_size = args.batch_size

    rv = requests.get(api, auth=auth)
    rv.raise_for_status()

    # Get all indices that are concrete
    indices_text = requests.get(
        f"{api}/_cat/indices", params={"h": "index"}, auth=auth
    ).text.splitlines()
    ro_pattern = re.compile(r"\-\d{4}\.\d{2}\.\d{2}\-\d{6}$")
    concrete_indices = []
    for index in indices_text:
        if index.startswith("."):
            continue
        if ro_pattern.search(index):
            continue
        concrete_indices.append(index)

    if not concrete_indices:
        raise SystemExit("No concrete indices to repair!")
    print(f"Found {len(concrete_indices)} concrete indices")

    # Do the entire shuffling garbage of setup new index, reindex then hook up alias
    for i, index in enumerate(concrete_indices, 1):

        ro_index = quote_plus(f"<{index}-{{now/d}}-000001>")
        print(f'[{i}/{len(concrete_indices)}] Fixing "{index}" (using "{ro_index}")')

        if not for_real:
            continue

        # Create new index
        rv = requests.put(f"{api}/{ro_index}", auth=auth).json()
        if "index" not in rv:
            print(f'Could not create new index for "{index}"! Skipping!')
            print("* Reason: ", rv)
            continue
        new_index = rv["index"]

        # Reindex
        print(f'Reindexing from concrete index "{index}" -> "{new_index}"')
        reindex_body = {"source": {"index": index}, "dest": {"index": new_index}}
        # TODO: change batch size maybe? add arg for it?
        rv = requests.post(
            f"{api}/_reindex",
            json=reindex_body,
            params={
                "slices": "auto",
                "wait_for_completion": "false",
                "format": "json",
                "scroll": "10m",
            },
            auth=auth,
        )
        rv.raise_for_status()
        task = rv.json()["task"]
        if verbose:
            print(f'Reindex task for "{index}" is "{task}"')

        # Wait for task to complete
        done = False
        while not done:
            try:
                rv = requests.get(f"{api}/_tasks/{task}", timeout=5)
                # In case I get an annoying as hell 5xx status
                if rv.status_code >= 500 and rv.status_code < 600:
                    done = False
                    continue
                done = rv.json().get("completed", True)
                if done:
                    if rv.json()["error"].get("failed_shards"):
                        print(f"Failed reindex due to error!")
                        print(json.dumps(rv.json(), sort_keys=True, indent=2))
                        raise SystemExit()
            except:
                done = True

            if not done:
                time.sleep(2)

        # Now destroy old, link new
        print(
            f'Destroying old concrete index "{index}", pointing alias to "{new_index}"'
        )
        aliases_body = {
            "actions": [
                {"remove_index": {"index": index}},
                {"add": {"index": new_index, "alias": index, "is_write_index": True}},
            ]
        }
        rv = requests.post(f"{api}/_aliases", json=aliases_body, auth=auth)
        rv.raise_for_status()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("API")
    parser.add_argument("--for-real", default=False, action="store_true")
    parser.add_argument("-u", "--es-user", default=getuser())
    parser.add_argument("-p", "--es-pass", default=None)
    parser.add_argument("-v", "--verbose", default=False, action="store_true")
    parser.add_argument("--batch-size", default=1000, type=int)
    args = parser.parse_args()
    main(args)
