import requests
import time
import json
import argparse
from pathlib import Path


def main(args):

    # Setup
    DATASOURCE_FILTER = args.datasource_filter
    GRAFANA_API = args.grafana_api
    GRAFANA_TOKEN = {"Authorization": f"Bearer {args.grafana_token}"}
    NEW_USERNAME = args.username
    NEW_PASSWORD = args.password
    IN_SOURCES = args.sources
    dump_path = args.dump
    for_real = args.for_real

    # Get existing sources
    if IN_SOURCES:
        print(f"Using file based sources from {IN_SOURCES}")
        with open(IN_SOURCES, "r") as f:
            current_sources = json.loads(f.read())
    else:
        current_sources = requests.get(
            f"{GRAFANA_API}/api/datasources", headers=GRAFANA_TOKEN
        ).json()
    es_sources = []
    es_logs_sources = []
    for source in current_sources:
        if source["type"] == "elasticsearch":
            es_sources.append(source)
        if source["url"].startswith(DATASOURCE_FILTER):
            es_logs_sources.append(source)

    print(f"Found {len(es_sources)} total elasticsearch sources")
    print(f"Found {len(es_logs_sources)} es logs related sources")

    if dump_path:
        print(f"Saving to {dump_path}")
        with open(dump_path, "w") as f:
            f.write(json.dumps(es_sources, sort_keys=True, indent=2))
            return

    fixed_sources = []
    for source in es_logs_sources:
        new_source = source.copy()
        for field in ["typeLogoUrl", "typeName", "uid"]:
            del new_source[field]
        new_source["basicAuthUser"] = NEW_USERNAME
        new_source["basicAuthPassword"] = NEW_PASSWORD
        fixed_sources.append(new_source)

    total = len(fixed_sources)
    i_s = len(str(total))
    for i, source in enumerate(fixed_sources, 1):
        d_id = source["id"]
        d_name = source["name"]
        print(f"[{i:>{i_s}}/{total}] Updating {d_name} - {d_id} ... ", end="")
        if for_real:
            rv = requests.put(
                f"{GRAFANA_API}/api/datasources/{d_id}",
                headers=GRAFANA_TOKEN,
                json=source,
            )
            if rv.json()["message"] == "Datasource updated" and rv.status_code in [
                200,
                201,
                202,
            ]:
                print(f"ok!", flush=True)
            else:
                print("error: {rv.text}", flush=True)
            time.sleep(0.1)
        else:
            print(f"simulated!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datasource-filter",
        required=True,
        type=str,
        help="Datasources to filter/modify (e.g. https://es-example-api.example.com)",
    )
    parser.add_argument("-g", "--grafana-api", required=True, type=str)
    parser.add_argument("--grafana-token", type=str, required=True)
    parser.add_argument("--username", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--sources", default=None, type=str)
    parser.add_argument("--dump", default=None, type=str)
    parser.add_argument("--for-real", default=False, action="store_true")
    args = parser.parse_args()
    main(args)
