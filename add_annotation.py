"""
Add annotation to cluster.
"""

import json
import argparse
import random
import datetime

import requests


def main(args):

    # Setup
    auth = (args.username, args.password)
    url = args.url.rstrip("/")
    extra = {}
    for prop in args.extra.split(","):
        k, v = prop.split("=")
        extra[k.strip()] = v.strip()

    body = {
        "message": args.message,
        "timestamp_epoch": int(datetime.datetime.utcnow().timestamp() * 1000),
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "extra": extra,
    }

    print(body)

    print(f"Shipping annotation ... ", end="")
    rv = requests.post(f"{url}/annotations/_doc/", json=body, auth=auth)
    if rv.status_code not in [200, 201]:
        print(rv.json())
    rv.raise_for_status()
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("message")
    parser.add_argument("--username", "-u", default=None, help="basic auth username")
    parser.add_argument("--password", "-p", default=None, help="basic auth password")
    parser.add_argument(
        "--extra",
        "-e",
        default="",
        help="extra properties to add (format of k1=v1,k2=v2)",
    )
    args = parser.parse_args()
    main(args)
