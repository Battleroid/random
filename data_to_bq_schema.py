"""
Take a JSON file, infer the BQ schema from it and return equivalent SQL. Fairly
basic and does not account for repeated structs (somewhat rare in the data I've
encountered so far anyhow). Might be easier, or better at one point to have this
dump as JSON instead of the SQL equivalent.
"""


import argparse
import json


def is_float(n):
    if not isinstance(n, str):
        return False
    try:
        float(n)
        return True
    except ValueError:
        return False


def get_array_type(arr):
    types = list(map(type, arr))
    if len(set(types)) > 1:
        return "????"
    if types[0] == int:
        return "INTEGER"
    elif types[0] == str:
        return "STRING"


def main(args):

    with open(args.SAMPLE, "r") as f:
        sample = dict(sorted(json.loads(f.read()).items()))
    output = args.OUTPUT

    # Recurse through, build key to type dict
    def describe_graph(data, parent_keys=[]):
        indent = " " * (2 * len(parent_keys))
        for key, value in data.items():
            if "-" in key:
                key = key.replace("-", "_")
            if "@" in key:
                key = key.replace("@", "")
            if isinstance(value, str) and value.isnumeric():
                yield (f"{indent}{key} INTEGER")
            elif isinstance(value, int) or isinstance(value, float) or is_float(value):
                yield (f"{indent}{key} INTEGER")
            elif isinstance(value, str):
                yield (f"{indent}{key} STRING")
            elif isinstance(value, list):
                # TODO: crude, does not account for repeated structs
                array_type = get_array_type(value)
                yield (f"{indent}{key} ARRAY<{array_type}>")
            elif isinstance(value, dict):
                yield (f"{indent}{key} STRUCT<")
                parent_keys.append(key)
                yield from describe_graph(value, parent_keys)
                parent_keys = []
                yield (f"{indent}>")

    graph_items = list(describe_graph(sample))
    with open(output, "w") as f:
        for i, k in enumerate(zip(graph_items, graph_items[1::1]), 2):
            line = None
            k1, k2 = k
            if (
                (k2.endswith(">") and not ("<" in k2 and ">" in k2))
                or k1.endswith("<")
                or i == len(graph_items)
            ):
                print(k1)
                line = k1
            else:
                print(f"{k1},")
                line = f"{k1},"

            f.write(line + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("SAMPLE")
    parser.add_argument("OUTPUT")
    args = parser.parse_args()
    main(args)
