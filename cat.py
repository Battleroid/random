"""
Push contents from stdin to es cluster.
"""

import json
import sys
import argparse
from getpass import getuser, getpass
from elasticsearch import Elasticsearch


def do(args):
    """
    Index content from stdin to ES.
    """
    es_host = args.es_host
    es_user = args.es_user
    es_pass = args.es_pass or getpass()
    es_index = args.index
    es = Elasticsearch(
        es_host,
        use_ssl="https" in es_host,
        verify_certs=True,
        http_auth=(es_user, es_pass),
    )
    if not es.ping():
        raise SystemExit("Cannot authenticate with ES")

    cnt = 0
    # for i, line in enumerate(sys.stdin.readlines(), 1):
    for i, line in enumerate(sys.stdin, 1):
        event = json.loads(line.strip())
        doc_type = event.get("type", "_doc")
        es.index(es_index, doc_type, event, ignore=[400])
        cnt += 1

    print(f"Indexed {cnt} docs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-host", required=True, help="es api")
    parser.add_argument("--es-user", default=getuser(), help="es user")
    parser.add_argument("--es-pass", help="es pass")
    parser.add_argument("--index", required=True, help="index name to push content to")
    args = parser.parse_args()
    do(args)
