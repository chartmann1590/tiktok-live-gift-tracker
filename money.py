"""Diamond → USD helpers (deterministic cents rounding).

USD shown is diamonds × a fixed approximate cash-out rate ($0.005 / diamond). That is
**not** the viewer's coin purchase price (coins cost ~$0.014–0.02 each on the web, more
in-app) and not a guaranteed creator net payout (region, fees, and policy vary).

Diamond totals come from TikTok payloads; when the WebSocket gift message has a wrong
or zero ``diamond_count``, we prefer the HTTP gift catalog (same source as the in-app
gift panel) keyed by stable gift ``id``, then optional local overrides in
``gift_diamond_rates.json``, then the payload value.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import traceback
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Mapping, Optional, Tuple

# Approximate USD per diamond for display (see module docstring).
_DIAMOND_USD = Decimal("0.005")
_CENTS = Decimal("0.01")

_RATES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gift_diamond_rates.json")


def normalize_gift_name(name: str) -> str:
    """Lowercase, trim, collapse internal whitespace (for fallback name matching)."""
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _load_json_rates() -> Tuple[Dict[int, int], Dict[str, int]]:
    by_id: Dict[int, int] = {}
    by_name: Dict[str, int] = {}
    if not os.path.isfile(_RATES_PATH):
        return by_id, by_name
    try:
        with open(_RATES_PATH, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return by_id, by_name
    if not isinstance(raw, dict):
        return by_id, by_name
    for k, v in (raw.get("by_gift_id") or {}).items():
        try:
            iv = int(v)
            if iv > 0:
                by_id[int(k)] = iv
        except (TypeError, ValueError):
            continue
    for k, v in (raw.get("by_normalized_name") or {}).items():
        try:
            iv = int(v)
            if iv > 0 and isinstance(k, str):
                by_name[normalize_gift_name(k)] = iv
        except (TypeError, ValueError):
            continue
    return by_id, by_name


_JSON_BY_ID, _JSON_BY_NAME = _load_json_rates()


def index_gift_catalog(gift_info: Any) -> Dict[int, int]:
    """
    Build ``gift_id -> diamond_count`` (single unit) from TikTokLive's ``gift_info``
    blob (response ``data`` from ``/webcast/gift/list/``). Shape varies slightly; we
    accept common layouts.
    """
    out: Dict[int, int] = {}
    if gift_info is None:
        return out
    if isinstance(gift_info, list):
        candidates = gift_info
    elif isinstance(gift_info, dict):
        candidates = gift_info.get("gifts")
        if candidates is None and isinstance(gift_info.get("data"), dict):
            candidates = gift_info["data"].get("gifts")
        if candidates is None:
            candidates = gift_info.get("gift_list")
    else:
        return out
    if not isinstance(candidates, list):
        return out
    for g in candidates:
        if not isinstance(g, dict):
            continue
        gid = g.get("id")
        if gid is None:
            gid = g.get("gift_id")
        dc = g.get("diamond_count")
        if dc is None:
            dc = g.get("diamondCount")
        try:
            iid = int(gid)
            idc = int(dc)
        except (TypeError, ValueError):
            continue
        if iid > 0 and idc > 0:
            out[iid] = idc
    return out


def resolve_diamonds_per_unit(
    gift_id: int,
    gift_name: str,
    payload_diamonds: int,
    catalog: Optional[Mapping[int, int]] = None,
) -> int:
    """
    Diamonds for one gift unit (before ``repeat_count``).

    Resolution order:
    1. ``catalog[gift_id]`` when present and > 0 (HTTP gift list; preferred over WS).
    2. ``gift_diamond_rates.json`` by gift id.
    3. ``gift_diamond_rates.json`` by normalized gift name.
    4. ``payload_diamonds`` when > 0 (WebSocket ``Gift.diamond_count``).
    5. ``0`` if nothing else matched (caller may still multiply by repeat_count).
    """
    cat = catalog or {}
    if gift_id > 0:
        v = cat.get(gift_id)
        if v is not None and v > 0:
            return int(v)
        v = _JSON_BY_ID.get(gift_id)
        if v is not None and v > 0:
            return int(v)
    name_key = normalize_gift_name(gift_name)
    if name_key:
        v = _JSON_BY_NAME.get(name_key)
        if v is not None and v > 0:
            return int(v)
    if payload_diamonds > 0:
        return int(payload_diamonds)
    return 0


def diamonds_to_usd(diamonds: int) -> float:
    """Convert diamond count to USD, rounded half-up to cents (avoids binary float drift)."""
    if diamonds < 0:
        raise ValueError("diamonds must be non-negative")
    amount = Decimal(diamonds) * _DIAMOND_USD
    return float(amount.quantize(_CENTS, rounding=ROUND_HALF_UP))


def repair_row_diamonds_usd(
    gift_name: str,
    stored_diamond_total: int,
    *,
    gift_id: int = 0,
    repeat_count: Optional[int] = None,
    payload_diamond_per_unit: Optional[int] = None,
    catalog: Optional[Mapping[int, int]] = None,
) -> Tuple[int, float, str]:
    """
    Recompute total diamonds and USD for one persisted gift row using the same resolution
    rules as the live listener (``resolve_diamonds_per_unit`` × repeat, then
    ``diamonds_to_usd``). Intended for **offline** repair: pass ``catalog={}`` (default)
    so only ``gift_diamond_rates.json`` and optional columns apply—no live TikTok.

    **Legacy rows** (only ``gift_name`` + ``stored_diamond_total``): ``repeat_count`` is
    inferred as ``stored_diamond_total // per_unit`` when ``per_unit > 0`` and the
    remainder is zero (matches ``per_unit * repeat`` with the stored total). If the
    remainder is non-zero, the stored diamond total cannot be reproduced without the
    original ``repeat_count`` / payload; the row keeps the stored total and USD is
    refreshed from that total.

    **Optional columns** (when present in SQLite): pass ``gift_id``, ``repeat_count``,
    and ``payload_diamond_per_unit`` to match runtime logic exactly.

    Returns ``(diamonds_total, usd_value, status)`` where ``status`` is one of:
    ``ok_explicit_repeat``, ``ok_inferred_repeat``, ``payload_total``,
    ``ambiguous_indivisible``, ``zero``.
    """
    cat: Mapping[int, int] = catalog if catalog is not None else {}
    gid = int(gift_id)
    name = gift_name or ""
    stored = int(stored_diamond_total)
    if stored < 0:
        raise ValueError("stored_diamond_total must be non-negative")

    if repeat_count is not None:
        rc = max(1, int(repeat_count))
        pload = int(payload_diamond_per_unit or 0)
        per = resolve_diamonds_per_unit(gid, name, pload, cat)
        d = per * rc
        return d, diamonds_to_usd(d), "ok_explicit_repeat"

    per0 = resolve_diamonds_per_unit(gid, name, 0, cat)
    if per0 > 0:
        if stored == 0:
            return 0, diamonds_to_usd(0), "zero"
        if stored % per0 == 0:
            return stored, diamonds_to_usd(stored), "ok_inferred_repeat"
        return stored, diamonds_to_usd(stored), "ambiguous_indivisible"

    per_payload = resolve_diamonds_per_unit(gid, name, stored, cat)
    if per_payload == stored and stored > 0:
        return stored, diamonds_to_usd(stored), "payload_total"
    if per_payload > 0 and stored > 0 and stored % per_payload == 0:
        return stored, diamonds_to_usd(stored), "ok_inferred_repeat"
    if stored == 0:
        return 0, diamonds_to_usd(0), "zero"
    return stored, diamonds_to_usd(stored), "payload_total"


def _gift_table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(gifts)").fetchall()
    return {str(r[1]) for r in rows}


def _row_optional_int(row: sqlite3.Row, colset: set[str], name: str) -> Optional[int]:
    if name not in colset:
        return None
    v = row[name]
    if v is None:
        return None
    return int(v)


def repair_gifts_database(
    db_path: str,
    *,
    dry_run: bool = False,
    catalog: Optional[Mapping[int, int]] = None,
) -> dict[str, int]:
    """
    Recompute ``diamond_value`` and ``usd_value`` for all rows in ``gifts`` (single
    transaction; WAL-friendly). See ``repair_row_diamonds_usd`` for resolution rules.

    Optional columns (if migrated later): ``gift_id``, ``repeat_count``,
    ``payload_diamond_count`` — when present and non-NULL they are passed through.
    """
    cat: Mapping[int, int] = catalog if catalog is not None else {}
    stats: dict[str, int] = {
        "scanned": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "status_ok_explicit_repeat": 0,
        "status_ok_inferred_repeat": 0,
        "status_payload_total": 0,
        "status_ambiguous_indivisible": 0,
        "status_zero": 0,
    }

    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    colset = _gift_table_columns(conn)
    if not colset or "diamond_value" not in colset:
        conn.close()
        raise ValueError("Table gifts missing or has no diamond_value column")

    cur = conn.cursor()
    cur.execute("SELECT * FROM gifts ORDER BY id ASC")
    rows = cur.fetchall()

    try:
        conn.execute("BEGIN" if dry_run else "BEGIN IMMEDIATE")

        for row in rows:
            stats["scanned"] += 1
            rid = int(row["id"])
            gift_name = str(row["gift_name"])
            stored_d = int(row["diamond_value"])
            stored_usd = float(row["usd_value"])

            gift_id = 0
            if "gift_id" in colset:
                gv = row["gift_id"]
                if gv is not None:
                    gift_id = int(gv)

            repeat_count = _row_optional_int(row, colset, "repeat_count")
            payload_pu = _row_optional_int(row, colset, "payload_diamond_count")

            try:
                new_d, new_usd, status = repair_row_diamonds_usd(
                    gift_name,
                    stored_d,
                    gift_id=gift_id,
                    repeat_count=repeat_count,
                    payload_diamond_per_unit=payload_pu,
                    catalog=cat,
                )
            except Exception:
                stats["errors"] += 1
                print(
                    f"[repair_gifts_database] id={rid} error:\n{traceback.format_exc()}",
                    file=sys.stderr,
                )
                continue

            sk = f"status_{status}"
            if sk in stats:
                stats[sk] += 1

            same_d = new_d == stored_d
            same_u = round(float(new_usd), 2) == round(float(stored_usd), 2)
            if same_d and same_u:
                stats["unchanged"] += 1
                continue

            stats["updated"] += 1
            conn.execute(
                "UPDATE gifts SET diamond_value = ?, usd_value = ? WHERE id = ?",
                (new_d, new_usd, rid),
            )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats
