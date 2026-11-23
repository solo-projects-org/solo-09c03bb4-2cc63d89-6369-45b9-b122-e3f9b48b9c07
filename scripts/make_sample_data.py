#!/usr/bin/env python3
"""Generate demo parquet files for the Trusted Data Gateway.

Usage:
    python scripts/make_sample_data.py [output-dir]

Outputs customers.parquet plus three orders batches that exercise the main
flows: a good batch, a batch with several quarantined rows, and a batch with
rows for a partition that is later superseded.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "sample_data")
    out.mkdir(parents=True, exist_ok=True)

    customers = pa.table(
        {
            "customer_id": ["C-100", "C-101", "C-102"],
            "customer_name": ["Ada", "Grace", "Linus"],
            "region": ["EU", "US", "US"],
        }
    )
    pq.write_table(customers, out / "customers.parquet")

    def orders(order_ids, dates, customer_ids, amounts, currencies, statuses):
        return pa.table(
            {
                "order_id": order_ids,
                "order_date": [dt.date.fromisoformat(value) for value in dates],
                "customer_id": customer_ids,
                "amount": amounts,
                "currency": currencies,
                "status": statuses,
            }
        )

    pq.write_table(
        orders(
            ["O-1", "O-2"],
            ["2026-11-20", "2026-11-20"],
            ["C-100", "C-101"],
            [19.5, 42.0],
            ["EUR", "USD"],
            ["paid", "pending"],
        ),
        out / "orders_batch1_good.parquet",
    )

    # Mixed batch: bad reference, negative amount, bad currency, unknown status,
    # duplicate primary key, and a null required value.
    pq.write_table(
        orders(
            ["O-3", "O-4", "O-5", "O-6", "O-6", None],
            ["2026-11-21", "2026-11-21", "2026-11-21", "2026-11-21", "2026-11-21", "2026-11-21"],
            ["C-102", "C-999", "C-100", "C-101", "C-101", "C-100"],
            [5.0, 9.0, -2.0, 11.0, 12.0, 7.0],
            ["USD", "USD", "usd", "USD", "USD", "USD"],
            ["paid", "paid", "paid", "unknown", "paid", "paid"],
        ),
        out / "orders_batch2_mixed.parquet",
    )

    # A later batch for the same day, including a key already trusted from
    # batch 1 (cross-batch uniqueness conflict).
    pq.write_table(
        orders(
            ["O-2", "O-10"],
            ["2026-11-20", "2026-11-22"],
            ["C-101", "C-102"],
            [45.0, 3.5],
            ["USD", "EUR"],
            ["refunded", "paid"],
        ),
        out / "orders_batch3_newer.parquet",
    )

    print(f"sample parquet files written to {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
