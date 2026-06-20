"""Run a single scalar Trino query and print the value. Used by smoke-test.sh.
Reads the TRINO_* env vars (see web/api/trino_client.py)."""

import sys

sys.path.insert(0, "web/api")

import trino_client  # noqa: E402

rows = trino_client.query(sys.argv[1])
print(list(rows[0].values())[0] if rows else 0)
