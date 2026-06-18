"""
Synthetic CDC event generator — simulates a Datastream / Debezium replica
of an e-commerce operational database (customers + orders).

Emits Debezium-style change envelopes as JSONL. Crucially, it injects the
real-world data-quality problems a production pipeline must survive:

  1. Duplicate events        — at-least-once delivery (Kafka/Datastream redelivery)
  2. Out-of-order timestamps — events for a key arriving older than ones already seen
  3. Late-arriving updates    — an update landing long after the row moved on
  4. Soft deletes (tombstones)— op="d" with `after=null`
  5. Null floods              — required-ish fields suddenly null
  6. Schema drift             — a new column ("currency") appears partway through
  7. Type inconsistency       — `amount` sometimes number, sometimes string

The downstream dbt layer is responsible for making this clean and correct.
This file is intentionally pure-stdlib so it runs on any Python 3.9+.

Usage:
    python generator/cdc_generator.py --events 5000 --seed 42 --out raw_events
"""

import argparse
import json
import os
import random
import hashlib
from datetime import datetime, timedelta, timezone

# ── Reference data ────────────────────────────────────────────────────────────

FIRST = ["Aarav", "Diya", "Vivaan", "Ananya", "Aditya", "Ishita", "Kabir",
         "Saanvi", "Arjun", "Myra", "Reyansh", "Aadhya", "Vihaan", "Anika"]
LAST = ["Sharma", "Patel", "Reddy", "Iyer", "Khan", "Nair", "Bose", "Gupta",
        "Mehta", "Rao", "Das", "Chopra", "Menon", "Joshi"]
COUNTRIES = ["IN", "US", "GB", "AE", "SG", "AU", "DE", "CA"]
TIERS = ["bronze", "silver", "gold", "platinum"]
STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"]
CURRENCIES = ["INR", "USD", "GBP", "AED", "SGD"]

EPOCH_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _event_id(table: str, pk: str, op: str, ts_ms: int, lsn: int) -> str:
    """Deterministic id so the loader can dedupe redelivered events."""
    raw = f"{table}|{pk}|{op}|{ts_ms}|{lsn}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


class CDCGenerator:
    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.lsn = 1000
        self.customers: dict[int, dict] = {}
        self.orders: dict[int, dict] = {}
        self.events: list[dict] = []
        self.schema_drift_after = None  # set later: when `currency` starts appearing

    # ── envelope helpers ──────────────────────────────────────────────────────

    def _next_lsn(self) -> int:
        self.lsn += self.rng.randint(1, 5)
        return self.lsn

    def _emit(self, table: str, op: str, pk: str, before, after, ts_ms: int):
        lsn = self._next_lsn()
        envelope = {
            "event_id": _event_id(table, pk, op, ts_ms, lsn),
            "op": op,  # c=create, u=update, d=delete, r=snapshot read
            "ts_ms": ts_ms,
            "source": {"db": "shop_prod", "table": table, "lsn": lsn,
                       "connector": "datastream"},
            "before": before,
            "after": after,
        }
        self.events.append(envelope)

        # ── INJECT MESSINESS ──────────────────────────────────────────────────
        # 1. Duplicate delivery (~4% of events redelivered, sometimes twice)
        if self.rng.random() < 0.04:
            self.events.append(dict(envelope))
            if self.rng.random() < 0.2:
                self.events.append(dict(envelope))

    # ── customer lifecycle ────────────────────────────────────────────────────

    def create_customer(self, cid: int, ts: datetime):
        row = {
            "customer_id": cid,
            "full_name": f"{self.rng.choice(FIRST)} {self.rng.choice(LAST)}",
            "email": f"user{cid}@example.com",
            "country": self.rng.choice(COUNTRIES),
            "tier": "bronze",
            "created_at": ts.isoformat(),
            "updated_at": ts.isoformat(),
        }
        # 5. Null flood — ~3% of new customers missing email
        if self.rng.random() < 0.03:
            row["email"] = None
        self.customers[cid] = row
        self._emit("customers", "c", str(cid), None, dict(row), _ts_ms(ts))

    def upgrade_customer(self, cid: int, ts: datetime):
        if cid not in self.customers:
            return
        before = dict(self.customers[cid])
        cur_idx = TIERS.index(before["tier"]) if before["tier"] in TIERS else 0
        new_tier = TIERS[min(cur_idx + 1, len(TIERS) - 1)]
        after = dict(before)
        after["tier"] = new_tier
        after["updated_at"] = ts.isoformat()
        self.customers[cid] = after
        self._emit("customers", "u", str(cid), before, after, _ts_ms(ts))

    # ── order lifecycle ───────────────────────────────────────────────────────

    def create_order(self, oid: int, ts: datetime):
        if not self.customers:
            return
        cid = self.rng.choice(list(self.customers.keys()))
        amount = round(self.rng.uniform(50, 5000), 2)

        # 7. Type inconsistency — ~5% emit amount as a string
        amount_val = str(amount) if self.rng.random() < 0.05 else amount

        row = {
            "order_id": oid,
            "customer_id": cid,
            "status": "pending",
            "amount": amount_val,
            "created_at": ts.isoformat(),
            "updated_at": ts.isoformat(),
        }
        # 6. Schema drift — `currency` field only appears after the drift point
        if self.schema_drift_after is not None and ts >= self.schema_drift_after:
            row["currency"] = self.rng.choice(CURRENCIES)

        self.orders[oid] = row
        self._emit("orders", "c", str(oid), None, dict(row), _ts_ms(ts))

    def advance_order(self, oid: int, ts: datetime):
        if oid not in self.orders:
            return
        before = dict(self.orders[oid])
        cur = before["status"]
        # progression
        flow = {"pending": ["confirmed", "cancelled"],
                "confirmed": ["shipped", "cancelled"],
                "shipped": ["delivered"],
                "delivered": ["refunded"],
                "cancelled": [], "refunded": []}
        nxts = flow.get(cur, [])
        if not nxts:
            return
        after = dict(before)
        after["status"] = self.rng.choice(nxts)
        after["updated_at"] = ts.isoformat()
        self.orders[oid] = after
        self._emit("orders", "u", str(oid), before, after, _ts_ms(ts))

    def delete_order(self, oid: int, ts: datetime):
        """Soft delete → tombstone (op=d, after=null)."""
        if oid not in self.orders:
            return
        before = dict(self.orders[oid])
        self._emit("orders", "d", str(oid), before, None, _ts_ms(ts))
        del self.orders[oid]

    # ── orchestration of the simulation ───────────────────────────────────────

    def run(self, n_events: int):
        ts = EPOCH_START
        next_cid = 1
        next_oid = 1
        # schema drift kicks in ~60% of the way through the time window
        self.schema_drift_after = EPOCH_START + timedelta(days=int(n_events * 0.6 / 50))

        for i in range(n_events):
            # advance event time by a random gap
            ts = ts + timedelta(minutes=self.rng.randint(1, 90))

            # 2 & 3. Out-of-order / late-arriving — occasionally rewind the clock
            event_ts = ts
            if self.rng.random() < 0.06:
                event_ts = ts - timedelta(hours=self.rng.randint(2, 72))

            r = self.rng.random()
            if r < 0.18 or not self.customers:
                self.create_customer(next_cid, event_ts)
                next_cid += 1
            elif r < 0.24:
                self.upgrade_customer(self.rng.choice(list(self.customers.keys())), event_ts)
            elif r < 0.55:
                self.create_order(next_oid, event_ts)
                next_oid += 1
            elif r < 0.92 and self.orders:
                self.advance_order(self.rng.choice(list(self.orders.keys())), event_ts)
            elif self.orders:
                self.delete_order(self.rng.choice(list(self.orders.keys())), event_ts)

        return self.events

    def write(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        orders = [e for e in self.events if e["source"]["table"] == "orders"]
        customers = [e for e in self.events if e["source"]["table"] == "customers"]
        with open(os.path.join(out_dir, "orders_cdc.jsonl"), "w") as f:
            for e in orders:
                f.write(json.dumps(e) + "\n")
        with open(os.path.join(out_dir, "customers_cdc.jsonl"), "w") as f:
            for e in customers:
                f.write(json.dumps(e) + "\n")
        return len(orders), len(customers)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="raw_events")
    args = ap.parse_args()

    gen = CDCGenerator(seed=args.seed)
    events = gen.run(args.events)
    n_orders, n_customers = gen.write(args.out)

    dupes = len(events) - len({e["event_id"] for e in events})
    print(f"Generated {len(events)} CDC events → {args.out}/")
    print(f"  orders:    {n_orders} events")
    print(f"  customers: {n_customers} events")
    print(f"  duplicate (redelivered) events injected: {dupes}")
    print(f"  schema drift (currency) begins: {gen.schema_drift_after.date()}")


if __name__ == "__main__":
    main()
