# graphrag-toolkit-lexical-graph-falkordb

FalkorDB support for the AWS GraphRAG Toolkit lexical graph.

## Running the tests

The unit tests run with the rest of the suite. The integration test in
`tests/test_label_injection_live.py` needs a reachable FalkorDB and is skipped
when none is found, so it is safe to leave in the default run.

To run the integration test, start a FalkorDB and point `FALKORDB_URL` at it:

```bash
docker run -d --name falkordb -p 6379:6379 falkordb/falkordb:latest

FALKORDB_URL=falkordb://localhost:6379 \
  pytest lexical-graph-contrib/falkordb/tests/test_label_injection_live.py -v
```

`FALKORDB_URL` defaults to `falkordb://localhost:6379` when unset. Any
Docker-compatible runtime works in place of `docker`.
