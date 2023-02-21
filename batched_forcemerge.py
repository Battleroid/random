"""
Forcemerge indices from file for a cluster in batches to avoid issues.
"""

from getpass import getpass, getuser
import time
import requests
import argparse


def main(args):

    # Setup
    username = args.username
    password = args.password or getpass()
    auth = (username, password)
    api = args.api.rstrip("/")
    requests.get(f"{api}/_cluster/health", auth=auth).raise_for_status()
    indices = open(args.indices_file).read().splitlines()
    batch_size = int(args.batch_size)
    sleep_int = int(args.sleep_int)

    # Start force merging
    batches = [indices[i : i + batch_size] for i in range(0, len(indices), batch_size)]
    print(
        f"Forcemerging with batch size of {batch_size} for {len(indices)} for a total of {len(batches)} batches"
    )

    for i, batch in enumerate(batches, 1):
        batch_str = ",".join(batch)
        print(f'[{i}/{len(batches)}] Forcemerging: {", ".join(batch)}')
        requests.post(
            f"{api}/{batch_str}/_forcemerge", auth=auth, params={"max_num_segments": 1}
        )
        time.sleep(int(sleep_int))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", default=getuser(), help="es username")
    parser.add_argument("--password", default=None, help="es password")
    parser.add_argument(
        "--batch-size", type=int, default=10, help="indices to batch forcemerge at once"
    )
    parser.add_argument(
        "--sleep-int", type=int, default=15, help="sleep interval between batches"
    )
    parser.add_argument("api")
    parser.add_argument("indices_file")
    args = parser.parse_args()
    main(args)
