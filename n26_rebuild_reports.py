#!/Library/Frameworks/Python.framework/Versions/3.14/bin/python3
# # -*- coding: utf-8 -*-

"""
Re-normalize partner names in existing N26 state and rebuild reports.

What it does:
- Loads N26 state: ~/Library/Application Support/Finanzen/N26/n26_state.json
- Loads partner rules: /Users/joachimthomas/Finanzverwaltung/Programme/N26/partner_regeln.csv
- Renormalizes partner for ALL tx records using the same normalize_partner() logic.
- Recomputes record IDs (so duplicates can collapse).
- Writes back state atomically (creates a timestamped backup first).
- Rebuilds reports for given year(s) without needing new CSV input.

Usage:
  python3 n26_rebuild_reports.py --year 2026
  python3 n26_rebuild_reports.py --all-years
  python3 n26_rebuild_reports.py --years 2025,2026
"""

import csv
import json
import re
import hashlib
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

STATE_PATH = Path.home() / "Library" / "Application Support" / "Finanzen" / "N26" / "n26_state.json"
PARTNER_RULES_PATH = Path("/Users/joachimthomas/Finanzverwaltung/Programme/N26/partner_regeln.csv")

BASE_OUT = Path("/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Reports")
LOG_PATH = Path("/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

DELIM_GUESS = ";"


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_iso_local() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def log(msg: str) -> None:
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] {msg}\n")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def ymd_ok(s: str) -> bool:
    try:
        datetime.strptime((s or "").strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def detect_delimiter(sample: str) -> str:
    if sample.count(";") >= sample.count(","):
        return ";"
    return ","


_PARTNER_RULES_CACHE: Optional[List[Tuple[str, str]]] = None


def load_partner_rules() -> List[Tuple[str, str]]:
    global _PARTNER_RULES_CACHE
    if _PARTNER_RULES_CACHE is not None:
        return _PARTNER_RULES_CACHE

    rules: List[Tuple[str, str]] = []
    if not PARTNER_RULES_PATH.exists():
        _PARTNER_RULES_CACHE = []
        return _PARTNER_RULES_CACHE

    txt = PARTNER_RULES_PATH.read_text(encoding="utf-8", errors="replace")
    sample = "\n".join(txt.splitlines()[:5])
    delim = detect_delimiter(sample) if sample else ";"

    with PARTNER_RULES_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        for row in reader:
            if not row:
                continue
            first = (row[0] or "").strip() if len(row) > 0 else ""
            if not first or first.startswith("#"):
                continue
            pat = first
            canon = (row[1] or "").strip() if len(row) > 1 else ""
            if not canon:
                continue
            rules.append((pat, canon))

    _PARTNER_RULES_CACHE = rules
    return _PARTNER_RULES_CACHE


def normalize_partner(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return "UNBEKANNT"

    rules = load_partner_rules()
    if not rules:
        return s

    for pat, canon in rules:
        p = (pat or "").strip()
        c = (canon or "").strip()
        if not p or not c:
            continue
        try:
            if re.search(p, s, flags=re.IGNORECASE):
                return c
        except Exception:
            if p.lower() in s.lower():
                return c
    return s


def record_id(booking_date: str, partner: str, amount: float, memo: str) -> str:
    key = "|".join([booking_date, partner, f"{amount:.8f}", (memo or "").strip()])
    return sha1_hex(key)[:16]


def ensure_dirs(year: str) -> None:
    (BASE_OUT / year / "Monatlich").mkdir(parents=True, exist_ok=True)
    (BASE_OUT / year / "Jährlich").mkdir(parents=True, exist_ok=True)


def fmt_de(n: float) -> str:
    return f"{n:.2f}".replace(".", ",")


def yyyymm_from_ymd(ymd: str) -> str:
    return ymd[:7]


def yyyymm_to_ddmmyyyy(yyyymm: str) -> str:
    if len(yyyymm) == 7 and yyyymm[4] == "-":
        return f"01.{yyyymm[5:7]}.{yyyymm[0:4]}"
    return "01.01.1900"


def write_csv(path: Path, header: List[str], rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=DELIM_GUESS)
        w.writerow(header)
        w.writerows(rows)
    tmp.replace(path)


def build_reports_for_year(state: Dict[str, Any], year: str) -> None:
    ensure_dirs(year)
    tx = state.get("tx", {}) or {}
    if not isinstance(tx, dict):
        return

    items: List[Dict[str, Any]] = []
    for rec in tx.values():
        if not isinstance(rec, dict):
            continue
        d = (rec.get("bookingDate") or "").strip()
        if not ymd_ok(d):
            continue
        if d.startswith(year + "-"):
            items.append(rec)

    items.sort(key=lambda r: ((r.get("bookingDate") or ""), (r.get("id") or "")))

    year_dir = BASE_OUT / year / "Jährlich"
    month_dir = BASE_OUT / year / "Monatlich"
    # Zusätzlich (Legacy/Bequemlichkeit): manche schauen in den Root oder in "Jahresübersicht"
    legacy_year_dir = BASE_OUT / "Jahresübersicht"
    legacy_year_dir.mkdir(parents=True, exist_ok=True)

    # YearOverview
    ledger_path = year_dir / f"{year}_YearOverview.csv"
    ledger_path_root = BASE_OUT / f"{year}_YearOverview.csv"
    ledger_path_legacy = legacy_year_dir / f"{year}_YearOverview.csv"

    log(f"YEAROVERVIEW | items={len(items)} | write={ledger_path}")
    log(f"YEAROVERVIEW | also_write={ledger_path_root}")
    log(f"YEAROVERVIEW | also_write={ledger_path_legacy}")

    ledger_rows: List[List[str]] = []
    for r in items:
        d = (r.get("bookingDate") or "").strip()
        partner_raw = (r.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        partner = normalize_partner(partner_raw)
        amt = float(r.get("amountValue", 0.0) or 0.0)
        memo = (
            r.get("memo")
            or (r.get("raw") or {}).get("Verwendungszweck", "")
            or (r.get("raw") or {}).get("purpose", "")
            or ""
        ).strip()
        ledger_rows.append([d, partner, fmt_de(amt), memo])
    write_csv(ledger_path, ["BookingDate", "Partner", "Amount_EUR", "Memo"], ledger_rows)
    write_csv(ledger_path_root, ["BookingDate", "Partner", "Amount_EUR", "Memo"], ledger_rows)
    write_csv(ledger_path_legacy, ["BookingDate", "Partner", "Amount_EUR", "Memo"], ledger_rows)

    # Monthly Partnerauswertung
    by_month_exp = defaultdict(lambda: defaultdict(float))
    by_month_inc = defaultdict(lambda: defaultdict(float))

    for r in items:
        d = (r.get("bookingDate") or "").strip()
        if not ymd_ok(d):
            continue
        yyyymm = yyyymm_from_ymd(d)
        partner_raw = (r.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        partner = normalize_partner(partner_raw)
        amt = float(r.get("amountValue", 0.0) or 0.0)
        if amt < 0:
            by_month_exp[yyyymm][partner] += -amt
        elif amt > 0:
            by_month_inc[yyyymm][partner] += amt

    for yyyymm in sorted(set(list(by_month_exp.keys()) + list(by_month_inc.keys()))):
        log(
            f"MONTHLY | yyyymm={yyyymm} | exp_partners={len(by_month_exp.get(yyyymm, {}))} | inc_partners={len(by_month_inc.get(yyyymm, {}))}"
        )
        out_csv = month_dir / f"{yyyymm}_Partnerauswertung.csv"
        month_date = yyyymm_to_ddmmyyyy(yyyymm)

        expenses = by_month_exp.get(yyyymm, {})
        income = by_month_inc.get(yyyymm, {})

        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=DELIM_GUESS)
            w.writerow(["Monat", "Partner", "Ausgaben EUR"])
            for partner, total in sorted(expenses.items(), key=lambda x: x[1], reverse=True):
                w.writerow([month_date, partner, fmt_de(total)])

            w.writerow([])
            w.writerow([])
            w.writerow([])

            w.writerow(["Monat", "Partner", "Einnahmen EUR"])
            for partner, total in sorted(income.items(), key=lambda x: x[1], reverse=True):
                w.writerow([month_date, partner, fmt_de(total)])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=str, default="")
    ap.add_argument("--years", type=str, default="")
    ap.add_argument("--all-years", action="store_true")
    args = ap.parse_args()

    if not STATE_PATH.exists():
        print(f"State not found: {STATE_PATH}")
        return 1

    # Load
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    tx = state.get("tx", {}) or {}
    if not isinstance(tx, dict):
        print("Invalid state: tx is not dict")
        return 1

    rules_cnt = len(load_partner_rules())
    log(f"START | rules={rules_cnt} path={PARTNER_RULES_PATH}")

    # Backup state
    backup = STATE_PATH.with_suffix(f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    backup.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"BACKUP | {backup}")

    # Renormalize + rekey
    new_tx: Dict[str, Dict[str, Any]] = {}
    collisions = 0
    changed_partner = 0
    total = 0

    for old_id, rec in tx.items():
        if not isinstance(rec, dict):
            continue
        total += 1

        booking = (rec.get("bookingDate") or "").strip()
        if not ymd_ok(booking):
            # keep as-is, but ensure unique key
            nid = rec.get("id") or old_id or sha1_hex(str(rec))[:16]
            while nid in new_tx:
                nid = sha1_hex(nid + "x")[:16]
            new_tx[nid] = rec
            continue

        partner_old = (rec.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        partner_new = normalize_partner(partner_old)
        if partner_new != partner_old:
            changed_partner += 1
            rec["partner"] = partner_new

        amt = float(rec.get("amountValue", 0.0) or 0.0)

        raw = rec.get("raw") or {}
        memo = ""
        if isinstance(raw, dict):
            memo = (
                raw.get("Verwendungszweck") or raw.get("purpose") or raw.get("memo") or ""
            ).strip()

        nid = record_id(booking, partner_new, amt, memo)
        rec["id"] = nid

        if nid in new_tx:
            # collision -> keep older ingestedAt if possible
            collisions += 1
            a = new_tx[nid]
            a_t = a.get("ingestedAt") or ""
            b_t = rec.get("ingestedAt") or ""
            # if b is older -> replace
            if b_t and a_t and b_t < a_t:
                new_tx[nid] = rec
        else:
            new_tx[nid] = rec

    state["tx"] = new_tx
    state["updatedAt"] = now_iso_local()
    log(
        f"REKEY | total={total} partner_changed={changed_partner} collisions={collisions} new_count={len(new_tx)}"
    )

    # Save atomically
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)
    log("STATE_SAVED")

    # Decide years to rebuild
    years: Set[str] = set()
    if args.all_years:
        for rec in new_tx.values():
            d = (rec.get("bookingDate") or "").strip()
            if ymd_ok(d):
                years.add(d[:4])
    if args.year:
        years.add(args.year.strip())
    if args.years:
        for y in args.years.split(","):
            y = y.strip()
            if y:
                years.add(y)

    if not years:
        # sensible default: current year
        years.add(str(datetime.now().year))

    for y in sorted(years):
        build_reports_for_year(state, y)
        log(f"REPORTS_OK | year={y}")

    log("DONE")
    print(
        f"OK. State renormalized. partner_changed={changed_partner}, collisions={collisions}. Reports rebuilt for {', '.join(sorted(years))}."
    )
    print(f"Log: {LOG_PATH}")
    print(f"Backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
