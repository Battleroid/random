"""
Split corpus material for rally into channels.
"""

import json
import sys
from itertools import cycle

spin = ["\\", "-", "/"]
pb = cycle(spin)

for fname in sys.argv[1:]:
    with open(fname, "r") as f:
        cnt = 1
        for line in f:
            if cnt != 1:
                print("\r", flush=cnt % 1000 == 0, end="")
            print(f"[{next(pb)}] Processing {cnt}", flush=cnt % 1000 == 0, end="")
            data = json.loads(line)
            index = data["elasticsearch_index"] + "-" + data.get("channel", "default")
            with open(index + ".json", "a") as findex:
                findex.write(line)
            cnt += 1
        print(f"\nSplit {cnt - 1} lines for {fname}")
