#!/usr/bin/env python3
"""
Recompute ``diamond_value`` and ``usd_value`` for every row in ``gifts`` using the same
rules as ``app.py`` / ``money.py`` (offline: empty catalog + ``gift_diamond_rates.json``).

Safe to run multiple times (idempotent when data is stable).

Usage::

    python scripts/repair_gift_rows.py
    python scripts/repair_gift_rows.py --db /app/data/gifts.db
    python scripts/repair_gift_rows.py --dry-run

Environment: ``DATA_DIR`` (default ``./data`` next to project root) selects ``gifts.db``
when ``--db`` is omitted.

**Limitations:** The default schema only stores ``gift_name`` and the historical total in
``diamond_value`` (no ``gift_id``, ``repeat_count``, or WebSocket per-unit diamonds).
Rows are repaired using ``repair_row_diamonds_usd`` (infer repeat when the total is
divisible by the resolved per-unit rate; otherwise see ``money.py`` docstring). Gifts
not in ``gift_diamond_rates.json`` keep their stored total and get USD recomputed from
that total.
"""
from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from money import repair_gifts_database  # noqa: E402


def _default_db_path() -> str:
    data_dir = os.path.abspath(
        os.environ.get("DATA_DIR", os.path.join(ROOT, "data"))
    )
    return os.path.join(data_dir, "gifts.db")


def main() -> None:
    p = argparse.ArgumentParser(description="Repair gifts.diamond_value and usd_value")
    p.add_argument(
        "--db",
        default=_default_db_path(),
        help="Path to gifts.db (default: DATA_DIR/gifts.db)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute changes but rollback (no writes)",
    )
    args = p.parse_args()
    db_path = os.path.abspath(args.db)
    if not os.path.isfile(db_path):
        raise SystemExit(f"Database file not found: {db_path}")

    stats = repair_gifts_database(db_path, dry_run=args.dry_run)
    mode = "dry-run" if args.dry_run else "commit"
    print(f"[repair] db={db_path} mode={mode}")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
