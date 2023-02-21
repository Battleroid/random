# random

Random junk I may or may not want to use again but don't want to rewrite.

# descriptions

| Script | Description |
| --- | --- |
| `remove_empties.py` | Find all empties on cluster that are not a write index, then delete them. |
| `split_by_index.py` | Split corpus material for rally into indices. |
| `cat.py` | Push contents from stdin to es cluster. |
| `what_write_indices_on_node.py` | Similar to where_are_write_indices, ties write indices to what node based on the what shards live where. Not exact as multiple nodes may have harbor shards all from the same index. |
| `data_to_bq_schema.py` | Take a JSON file, infer the BQ schema from it and return equivalent SQL. Fairly basic and does not account for repeated structs (somewhat rare in the data I've encountered so far anyhow). Might be easier, or better at one point to have this dump as JSON instead of the SQL equivalent. |
| `fix_concrete_index.py` | Automate fixing those absolutely annoying instances where ILM/Logstash/Squiggler screw up and you're left with a concrete index instead of a rollover index and alias. |
| `apply_role_mappings.py` | Apply role mappings to cluster. |
| `migrate_to_squiggler_ilm.py` | This should assign write aliases to all indices where needed, as well as assign the lifecycle information required. Assumes a cluster is NOT using ILM of any kind, still relying entirely on squiggler for lifecycle movement.  Does NOT turn ILM back on once it starts! |
| `batched_forcemerge.py` | Forcemerge indices from file for a cluster in batches to avoid issues. |
| `random_index.py` | Randomly index random junk to a cluster just for testing. |
| `avg_doc_size_by_shard.py` | Calculates the average doc size per shard for an index. Gives you an idea of how complex or dense an index may be. |
| `reallocation_report.py` | Guesstimate the results of chopping nodes. |
| `update_grafana_sources.py` | No description provided. |
| `move_big_shard.py` | Move shards from node to elsewhere with ease.  Requires $ pip install inquirer requests |
| `find_missing_write_alias.py` | Simple script that will scroll through all aliases/indices patterns and try to locate any that do not have a set write index for the entire set. |
| `delete_by_query_split.py` | Issue a delete by query against all indices in a pattern. For when it's too difficult to run a delete by query on an alias or group of indices.  E.g. you issue a query on an alias and get back that you have too many open search contexts. |
| `split_by_channel.py` | Split corpus material for rally into channels. |
| `kafka_describe_topics.py` | No description provided. |
| `data_to_bq_logstash_schema.py` | Take a JSON file, infer the BQ schema from it and return equivalent output for the BQ logstash output schema. Not perfect. Does NOT understand repeated dictionaries. |
| `where_are_write_indices.py` | Will attempt to tie where a write index may be. E.g. if on hot, the index will have a exclude warm setting. Optional index/tier filters available for checking specific items. You may also provide a list of indices to filter from a file.  Results can be sorted by physical primary store size. |
| `add_annotation.py` | Add annotation to cluster. |
| `_update.py` | Update descriptions for scripts. |
