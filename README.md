# CDC → ELT Pipeline with Data-Quality Gates

A fault-tolerant, end-to-end ELT pipeline that ingests **Change Data Capture (CDC)** events from a simulated Datastream/Debezium replica, lands them raw, transforms them with **dbt**, and orchestrates the whole thing with **Airflow** — wrapped in automated **data-quality gates**, **CI/CD**, and **failure alerting**.

The interesting part isn't "used dbt and Airflow." It's everything the pipeline does to survive a **messy, real-world CDC source**: duplicate deliveries, out-of-order events, soft deletes, schema drift, and type inconsistency.

> **CI status:** ![CI](https://github.com/Riiyansh/cdc-elt-pipeline/actions/workflows/ci.yml/badge.svg)
> **Live dashboard:** deploy to Streamlit Cloud (self-bootstrapping) — see below.

---

## The hard parts (what this project is actually about)

The source is a synthetic CDC stream that **deliberately injects the data-quality problems a production replica throws at you**. The pipeline is built to handle each one:

| Real-world CDC problem | Where it's handled | How |
|---|---|---|
| **At-least-once delivery** (duplicate events) | Raw load | `event_id` PRIMARY KEY + `ON CONFLICT DO NOTHING` → idempotent, dupes collapse |
| **Out-of-order / late-arriving events** | dbt staging | `row_number() over (partition by pk order by ts_ms desc, lsn desc)` → latest state wins |
| **Soft deletes** (tombstones, `op=d`) | dbt staging | deleted keys excluded from current-state models |
| **Schema drift** (`currency` appears mid-stream) | raw load + staging | payloads stored as JSON; staging `coalesce`s missing fields to a default |
| **Type inconsistency** (`amount` string vs number) | dbt staging | `try_cast(... as decimal)` — bad values surface as nulls, not crashes |
| **Parent/child ordering** (orphan orders) | dbt + DQ gate | orphans flagged; a **test fails the build if orphan rate > 5%** |
| **Bad source timestamps** | DQ gate | a test fails if any order is dated in the future |

---

## Architecture

```
┌─────────────────────┐   Debezium-style JSONL    ┌──────────────────────┐
│  CDC Generator      │  (c/u/d events + messiness)│  Idempotent Loader   │
│  (sim. Datastream)  │ ─────────────────────────► │  → DuckDB raw (bronze)│
└─────────────────────┘                            └──────────┬───────────┘
                                                              │
                          ┌───────────────────────────────────▼──────────────────────┐
                          │ dbt                                                        │
                          │  staging   → collapse CDC log to current state (silver)    │
                          │  intermediate → enrich + flag orphans                      │
                          │  marts     → fct_orders (incremental), dim_customers,      │
                          │              agg_daily_revenue (gold)                       │
                          │  snapshots → customers SCD Type-2 history                  │
                          │  tests     → schema tests + custom DQ GATES                │
                          └───────────────────────────────────┬──────────────────────┘
                                                              │
   ┌──────────────────────────────────────────────────────────▼───────────────────┐
   │ Airflow DAG:  land_raw → dbt_deps → dbt_run → dbt_snapshot → DQ GATE → publish │
   │   idempotent · retries w/ exponential backoff · Slack alert on failure         │
   └──────────────────────────────────────────────────────────┬───────────────────┘
                                                              │
                                              ┌────────────────▼────────────────┐
                                              │ Streamlit dashboard (serving)    │
                                              │ business KPIs + DQ scorecard     │
                                              └──────────────────────────────────┘

CI/CD (GitHub Actions): generate → load → sqlfluff lint → dbt build (DQ gate) → idempotency check
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Source (simulated) | Debezium-style CDC envelopes (JSONL) |
| Warehouse | DuckDB |
| Transformation | dbt (`dbt-duckdb`) — staging / intermediate / marts, incremental, SCD2 snapshots |
| Orchestration | Apache Airflow (Docker) |
| Data Quality | dbt tests (`not_null`, `unique`, `relationships`, `accepted_values`, `dbt_utils.accepted_range`) + custom singular tests |
| CI/CD | GitHub Actions (lint + build + DQ gate + idempotency assertion) |
| Serving | Streamlit |

---

## Production-hygiene features

- **Idempotent everywhere** — re-running the loader or the whole DAG for the same interval produces the same result (CI asserts "0 new rows" on re-load).
- **Incremental models** — `fct_orders` only processes events newer than the last watermark, so scheduled runs stay cheap.
- **Data-quality gate** — `dbt test` is a hard gate in the DAG; if DQ fails, downstream is blocked and no bad data is published.
- **Failure alerting** — `on_failure_callback` posts to Slack (or logs if no webhook).
- **Retries with exponential backoff** — transient failures self-heal; the DQ gate intentionally has `retries=0`.
- **Layered, tested, documented** — medallion architecture (raw → staging → marts) with tests and descriptions at every layer.

---

## Run it

### Option A — full pipeline locally (Airflow in Docker)
```bash
docker compose up
# open http://localhost:8080, enable the `cdc_elt_pipeline` DAG, trigger it
```

### Option B — just the ELT (no Airflow)
```bash
pip install duckdb dbt-duckdb
python generator/cdc_generator.py --events 5000 --seed 42   # simulate the CDC source
python ingestion/load_raw.py --db warehouse.duckdb          # idempotent raw load
cd dbt && DUCKDB_PATH=../warehouse.duckdb dbt build --profiles-dir .   # transform + DQ gate
```

### Option C — the dashboard (self-bootstrapping)
```bash
pip install -r requirements.txt
streamlit run dashboard/app.py
```

---

## Resume bullets this supports

- Built a fault-tolerant CDC→ELT pipeline (DuckDB · dbt · Airflow) processing ~5K change events/run with **idempotent loads** and **automated data-quality gates** that block bad data from reaching serving tables.
- Engineered dbt models to reconcile **out-of-order, duplicated, and soft-deleted CDC events** into correct current-state marts, with **incremental processing** and **SCD Type-2** history.
- Added **CI/CD** (GitHub Actions) running lint, `dbt build`, the DQ gate, and an **idempotency assertion** on every PR; wired **Slack failure alerting** into the Airflow DAG.

---

## License
MIT
