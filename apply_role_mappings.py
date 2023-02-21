"""
Apply role mappings to cluster.
"""

import json
import requests
import argparse


def main(args):

    # Setup
    role_mappings = json.load(args.ROLE_MAPPINGS)
    api_urls = list(map(str.strip, args.API_URLS))
    api_urls = [u.rstrip("/") for u in api_urls]
    auth = (args.es_user, args.es_pass)
    for_real = args.for_real

    # Quick info
    print(f"Applying {len(role_mappings)} across {len(api_urls)} clusters ...")

    # For each cluster, apply all mappings
    for url in api_urls:

        print(f"Applying mappings to {url} ...")

        # Attempt to auth
        try:
            rv = requests.get(url, auth=auth)
            rv.raise_for_status()
            cluster_name = rv.json()["cluster_name"]
        except:
            print(f"Could not auth to {url}! Skipping!")
            continue

        # Put each mapping
        for name, role_mapping in role_mappings.items():
            print(
                f'Applying role mapping "{name}" to {cluster_name} ...',
                end="",
                flush=True,
            )
            if for_real:
                rv = requests.put(
                    f"{api}/_security/role_mapping/{name}", json=role_mapping
                )
                if rv.ok:
                    print(" ok!", flush=True)
            else:
                print(" dry run!", flush=True)

    print(f"Finished!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ROLE_MAPPINGS", type=argparse.FileType("r"))
    parser.add_argument("API_URLS", nargs="+", type=str)
    parser.add_argument("--es-user", default=None, type=str)
    parser.add_argument("--es-pass", default=None, type=str)
    parser.add_argument("--for-real", default=False, action="store_true")
    args = parser.parse_args()
    main(args)
