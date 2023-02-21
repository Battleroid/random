"""
Take a JSON file, infer the BQ schema from it and return equivalent output for the
BQ logstash output schema. Not perfect. Does NOT understand repeated dictionaries.
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
    def describe_graph(data, parent_keys=[], extra_indent=0):
        indent = " " * ((2 * len(parent_keys)) + extra_indent)
        for key, value in data.items():
            # Fix illegal chars
            if "-" in key:
                key = key.replace("-", "_")
            if "@" in key:
                key = key.replace("@", "")

            # Assume type
            if isinstance(value, str) and value.isnumeric():
                yield (
                    f'{indent}{{\n{indent}  "name" => "{key}",\n{indent}  "type" => "INTEGER"\n{indent}}}'
                )
            elif isinstance(value, int) or isinstance(value, float) or is_float(value):
                yield (
                    f'{indent}{{\n{indent}  "name" => "{key}",\n{indent}  "type" => "INTEGER"\n{indent}}}'
                )
            elif isinstance(value, str):
                yield (
                    f'{indent}{{\n{indent}  "name" => "{key}",\n{indent}  "type" => "STRING"\n{indent}}}'
                )
            elif isinstance(value, list):
                # TODO: crude, does not account for repeated structs
                array_type = get_array_type(value)
                yield (
                    f'{indent}{{\n{indent}  "name" => "{key}",\n{indent}  "type" => "{array_type}",\n{indent}  "mode" => "REPEATED"\n{indent}}}'
                )
            elif isinstance(value, dict):
                yield (
                    f'{indent}{{\n{indent}  "name" => "{key}",\n{indent}  "type" => "RECORD",\n{indent}  "fields" => ['
                )
                parent_keys.append(key)
                yield from describe_graph(value, parent_keys, extra_indent=2)
                parent_keys = []
                yield (f"{indent}  ]\n{indent}}}")

    graph_items = list(describe_graph(sample))
    with open(output, "w") as f:
        for i, k in enumerate(zip(graph_items, graph_items[1::1]), 2):
            line = None
            k1, k2 = k

            # Remove the annoying newline on starts of dicts
            # if "=> [" in k1:
            #     k1 = k1.rstrip()
            # if "=> [" in k2:
            #     k2 = k2.rstrip()

            # If we're at the end, write both k1/k2, we can break out here as well
            punc = "," if k2 else ""
            if i == len(graph_items):
                print(f"{k1}{punc}")
                print(f"{k2}")
                f.write(f"{k1}{punc}" + "\n")
                f.write(f"{k2}" + "\n")
                break
            # Print k1, add punc whether or not we have additional fields after k1
            elif (
                (k2.endswith("}") and not ("{" in k2 and "}" in k2))
                or k1.endswith("{")
                or "=> [" in k1
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
