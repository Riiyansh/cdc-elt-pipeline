"""
Idempotent raw loader (the "EL" of ELT).

Lands Debezium-style CDC events into DuckDB's `raw` (bronze) schema,
append-only, deduplicated on event_id via a PRIMARY KEY + ON CONFLICT DO NOTHING.

Idempotency guarantees:
  - Re-running the loader never double-loads (same event_id is skipped).
  - Redelivered events from the source (at-least-once delivery) collapse to one row.

before/after payloads are stored as raw JSON strings so that SCHEMA DRIFT
(e.g. the `currency` column appearing midway) never breaks the load — the
transform layer decides how to interpret the evolving schema, not the loader.

Usage:
    python ingestion/load_raw.py --db warehouse.duckdb --events-dir raw_events
"""

import argparse
import json
import os
import duckdb

RAW_TABLES = {
    "orders": "raw_orders_cdc",
    "customers": "raw_customers_cdc",
}

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.{table} (
    event_id     VARCHAR PRIMARY KEY,
    op           VARCHAR,            -- c / u / d / r
    ts_ms        BIGINT,             -- source event time
    source_table VARCHAR,
    lsn          BIGINT,             -- log sequence number (ordering within source)
    before_json  JSON,
    after_json   JSON,
    _loaded_at   TIMESTAMP DEFAULT current_timestamp
);
"""


def load_file(con, jsonl_path: str, raw_table: str) -> tuple[int, int]:
    if not os.path.exists(jsonl_path):
        return 0, 0

    rows = []
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            rows.append((
                e["event_id"],
                e["op"],
                e["ts_ms"],
                e["source"]["table"],
                e["source"]["lsn"],
                json.dumps(e.get("before")),
                json.dumps(e.get("after")),
            ))

    before_count = con.execute(f"SELECT count(*) FROM raw.{raw_table}").fetchone()[0]

    # ON CONFLICT DO NOTHING → idempotent + dedupes redelivered events
    con.executemany(
        f"""INSERT INTO raw.{raw_table}
            (event_id, op, ts_ms, source_table, lsn, before_json, after_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING""",
        rows,
    )

    after_count = con.execute(f"SELECT count(*) FROM raw.{raw_table}").fetchone()[0]
    inserted = after_count - before_count
    skipped = len(rows) - inserted
    return inserted, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--events-dir", default="raw_events")
    args = ap.parse_args()

    con = duckdb.connect(args.db)
    for source_table, raw_table in RAW_TABLES.items():
        con.execute(DDL.format(table=raw_table))

    total_inserted = total_skipped = 0
    for source_table, raw_table in RAW_TABLES.items():
        path = os.path.join(args.events_dir, f"{source_table}_cdc.jsonl")
        inserted, skipped = load_file(con, path, raw_table)
        total_inserted += inserted
        total_skipped += skipped
        print(f"raw.{raw_table:22s} +{inserted:5d} inserted, {skipped:4d} skipped (dupes/idempotent)")

    con.close()
    print(f"\nTotal: {total_inserted} new rows, {total_skipped} skipped.")


if __name__ == "__main__":
    main()
