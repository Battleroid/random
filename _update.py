"""
Update descriptions for scripts.
"""

import pathlib
import code

readmes = {}

for file in pathlib.Path(".").glob("*.py"):
    co = compile(open(file).read(), file, "exec")
    if co.co_consts and isinstance(co.co_consts[0], str):
        doc = co.co_consts[0].replace("\n", " ")
    else:
        doc = "No description provided."
    readmes[file.name] = doc.strip()

with open("readme.md", "w") as f:

    text = f"""# random

Random junk I may or may not want to use again but don't want to rewrite.

# descriptions

| Script | Description |
| --- | --- |
"""

    for script, doc in readmes.items():
        text += f"| `{script}` | {doc} |\n"

    f.write(text)
