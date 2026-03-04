#!/Library/Frameworks/Python.framework/Versions/3.14/bin/python3
# # -*- coding: utf-8 -*-

"""N26 CSV -> State -> Reports (single-run, no watcher)

Behavior:
- Input: CSV path as argv[1]. If missing, picks the newest *.csv from a few sensible locations.
- Updates N26 state (~/Library/Application Support/Finanzen/N26/n26_state.json).
- Exit codes:
    0  = state changed (added/updated records) and reports were rebuilt for affected years
    10 = no-op (state unchanged) -> no reports
    1  = error
- Notifications go through finance_notify.sh
- Logging: /Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_state_and_reports.log

Notes:
- The script tries to be robust to different N26 CSV header variants.
- Reports are year-scoped: only years affected by added/changed records are rebuilt.
"""

import csv
import hashlib
import json
import os
import sys
import subprocess
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ============================================================
# CONFIG
# ============================================================

STATE_PATH = Path.home() / "Library" / "Application Support" / "Finanzen" / "N26" / "n26_state.json"


BASE_OUT = Path("/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Reports")

# Archive root for processed CSVs
ARCH_ROOT = Path("/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Kontobewegungen")

LOG_PATH = Path("/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

FIN_NOTIFY = "/Users/joachimthomas/Finanzverwaltung/Programme/Global/finance_notify.sh"

DELIM_GUESS = ";"  # N26 is usually ';' in DE exports

#
# Single inbox folder for N26 CSVs (expected: exactly one CSV at a time)
CSV_INBOX_DIR = Path("/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Kontobewegungen")

# Partner normalization rules (optional)
PARTNER_RULES_PATH = Path("/Users/joachimthomas/Finanzverwaltung/Programme/N26/partner_regeln.csv")

SCHEMA_VERSION = 1

# ============================================================
# Logging / Notify
# ============================================================


def now_iso_local() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] {msg}\n")


def n(code: str, msg: str) -> None:
    """finance_notify(app, code, message, source)"""
    try:
        subprocess.run(
            [FIN_NOTIFY, "N26", code, msg, __file__],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


# ============================================================
# State
# ============================================================


def ensure_state_exists() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if STATE_PATH.exists():
        return
    payload = {
        "schema": SCHEMA_VERSION,
        "updatedAt": "",
        "meta": {
            "lastImport": {
                "sourceFile": "",
                "sourcePath": "",
                "sourceUid": "",
                "sourceHash": "",
                "importedAt": "",
                "period": {"from": "", "to": ""},
                "affectedYears": [],
            }
        },
        "stats": {"txCount": 0, "minBookingDate": "", "maxBookingDate": ""},
        "tx": {},
    }
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def load_state() -> Dict[str, Any]:
    ensure_state_exists()
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        state = {}

    if not isinstance(state, dict):
        state = {}

    state.setdefault("schema", SCHEMA_VERSION)
    state.setdefault("updatedAt", "")
    state.setdefault("meta", {})
    if not isinstance(state["meta"], dict):
        state["meta"] = {}

    state["meta"].setdefault(
        "lastImport",
        {
            "sourceFile": "",
            "sourcePath": "",
            "sourceUid": "",
            "sourceHash": "",
            "importedAt": "",
            "period": {"from": "", "to": ""},
            "affectedYears": [],
        },
    )

    state.setdefault("stats", {"txCount": 0, "minBookingDate": "", "maxBookingDate": ""})
    if not isinstance(state["stats"], dict):
        state["stats"] = {"txCount": 0, "minBookingDate": "", "maxBookingDate": ""}

    state.setdefault("tx", {})
    if not isinstance(state["tx"], dict):
        state["tx"] = {}

    return state


def save_state(state: Dict[str, Any]) -> None:
    state["updatedAt"] = now_iso_local()
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def update_stats(state: Dict[str, Any]) -> None:
    tx = state.get("tx", {}) or {}
    if not isinstance(tx, dict):
        tx = {}

    dates: List[str] = []
    for rec in tx.values():
        if not isinstance(rec, dict):
            continue
        d = (rec.get("bookingDate") or "").strip()
        if ymd_ok(d):
            dates.append(d)

    st = state.setdefault("stats", {})
    if not isinstance(st, dict):
        st = {}
        state["stats"] = st

    st["txCount"] = len(tx)
    if dates:
        st["minBookingDate"] = min(dates)
        st["maxBookingDate"] = max(dates)


# ============================================================
# CSV parsing helpers
# ============================================================


def ymd_ok(s: str) -> bool:
    try:
        datetime.strptime((s or "").strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def parse_date_to_ymd(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""

    # already ISO?
    if ymd_ok(s):
        return s

    # common DE format: dd.mm.yyyy
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # common EN format: yyyy-mm-dd or dd/mm/yyyy
    for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


def de_money_to_float(s: str) -> float:
    s = (s or "").strip()
    if not s:
        return 0.0
    s = s.replace("€", "").replace("EUR", "").replace(" ", "").replace("+", "")
    # allow both 1.234,56 and 1234,56 and 1234.56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def sha1_file_full(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_header(h: str) -> str:
    return (h or "").strip().lower().replace(" ", "").replace("_", "")


def detect_delimiter(sample: str) -> str:
    # quick heuristic
    if sample.count(";") >= sample.count(","):
        return ";"
    return ","


# ============================================================
# Partner rules (optional)
# ============================================================

_PARTNER_RULES_CACHE: Optional[List[Tuple[str, str]]] = None


def load_partner_rules() -> List[Tuple[str, str]]:
    """Load partner normalization rules from PARTNER_RULES_PATH.

    Expected CSV format (tolerant): two columns per row -> pattern ; canonical
    - pattern is treated as a case-insensitive regex if it compiles.
    - if regex compilation fails, it falls back to a simple substring match.

    Returns a list of (pattern, canonical) in file order.
    """
    global _PARTNER_RULES_CACHE
    if _PARTNER_RULES_CACHE is not None:
        return _PARTNER_RULES_CACHE

    rules: List[Tuple[str, str]] = []

    try:
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
                # skip comments / empty rows
                first = (row[0] or "").strip() if len(row) > 0 else ""
                if not first or first.startswith("#"):
                    continue

                pat = first
                canon = (row[1] or "").strip() if len(row) > 1 else ""
                if not canon:
                    continue

                rules.append((pat, canon))

    except Exception:
        rules = []

    _PARTNER_RULES_CACHE = rules
    return _PARTNER_RULES_CACHE


def normalize_partner(name: str) -> str:
    """Normalize partner using partner_regeln.csv (best-effort)."""
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

        # Try regex match first
        try:
            if re.search(p, s, flags=re.IGNORECASE):
                return c
        except Exception:
            # Fallback: simple substring match (case-insensitive)
            if p.lower() in s.lower():
                return c

    return s


@dataclass
class ParsedRow:
    booking_date: str
    partner: str
    amount: float
    raw: Dict[str, str]


def parse_csv_rows(csv_path: Path) -> List[ParsedRow]:
    txt = csv_path.read_text(encoding="utf-8", errors="replace")
    sample = "\n".join(txt.splitlines()[:5])
    delim = detect_delimiter(sample) if sample else DELIM_GUESS

    rows: List[ParsedRow] = []

    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        if reader.fieldnames is None:
            return []

        # map headers
        hdr_map = {normalize_header(h): h for h in reader.fieldnames if h is not None}

        def pick(*candidates: str) -> Optional[str]:
            for c in candidates:
                k = normalize_header(c)
                if k in hdr_map:
                    return hdr_map[k]
            return None

        h_date = pick("bookingDate", "Buchungstag", "Datum", "Date")
        h_partner = pick("partner", "Empfänger", "Zahlungsempfänger", "Merchant", "Partner", "Name")
        h_amount = pick(
            "amountValue",
            "Betrag",
            "Betrag(EUR)",
            "Betrag(Euro)",
            "Amount",
            "Amount(EUR)",
        )

        # fallback heuristics
        if h_date is None:
            for k, v in hdr_map.items():
                if "datum" in k or "date" in k:
                    h_date = v
                    break

        if h_partner is None:
            for k, v in hdr_map.items():
                if "empf" in k or "partner" in k or "merchant" in k or "name" == k:
                    h_partner = v
                    break

        if h_amount is None:
            for k, v in hdr_map.items():
                if "betrag" in k or "amount" in k:
                    h_amount = v
                    break

        for rec in reader:
            if not isinstance(rec, dict):
                continue

            d_raw = (rec.get(h_date) if h_date else "") or ""
            d = parse_date_to_ymd(str(d_raw))
            if not d:
                continue

            partner = (rec.get(h_partner) if h_partner else "") or ""
            partner = str(partner).strip() or "UNBEKANNT"
            partner = normalize_partner(partner)

            amt_raw = (rec.get(h_amount) if h_amount else "") or ""
            amt = de_money_to_float(str(amt_raw))

            # keep raw as strings
            raw: Dict[str, str] = {}
            for k, v in rec.items():
                if k is None:
                    continue
                raw[str(k)] = "" if v is None else str(v)

            rows.append(ParsedRow(booking_date=d, partner=partner, amount=amt, raw=raw))

    return rows


def record_id(pr: ParsedRow) -> str:
    # stable-ish id from core signals + a few common raw fields if available
    raw = pr.raw
    candidates = [
        raw.get("Verwendungszweck", ""),
        raw.get("Reference", ""),
        raw.get("Kategorie", ""),
        raw.get("Category", ""),
        raw.get("Type", ""),
        raw.get("Typ", ""),
    ]
    key = "|".join(
        [
            pr.booking_date,
            pr.partner,
            f"{pr.amount:.8f}",
            "|".join(c.strip() for c in candidates if c is not None),
        ]
    )
    return sha1_hex(key)[:16]


def core_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    # compare the minimal fields we rely on for reports
    return (
        str(a.get("bookingDate", "")) == str(b.get("bookingDate", ""))
        and str(a.get("partner", "")) == str(b.get("partner", ""))
        and float(a.get("amountValue", 0.0) or 0.0) == float(b.get("amountValue", 0.0) or 0.0)
    )


# ============================================================
# Reports (year-scoped)
# ============================================================


def fmt_de(n: float) -> str:
    return f"{n:.2f}".replace(".", ",")


def yyyymm_from_ymd(ymd: str) -> str:
    return ymd[:7]


def yyyymm_to_ddmmyyyy(yyyymm: str) -> str:
    # 'YYYY-MM' -> '01.MM.YYYY'
    if len(yyyymm) == 7 and yyyymm[4] == "-":
        return f"01.{yyyymm[5:7]}.{yyyymm[0:4]}"
    return "01.01.1900"


# =======================
# Archive helpers
# =======================
import re

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def ymd_to_ddmmyy(ymd: str) -> str:
    ymd = (ymd or "").strip()
    if not DATE_RE.match(ymd):
        return ""
    y, m, d = ymd.split("-")
    return f"{d}.{m}.{y[2:]}"


def ensure_arch_dirs(year: str, month: str) -> None:
    (ARCH_ROOT / year / month).mkdir(parents=True, exist_ok=True)


def unique_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suf = dest.stem, dest.suffix
    i = 2
    while True:
        cand = dest.with_name(f"{stem}_{i}{suf}")
        if not cand.exists():
            return cand
        i += 1


def archive_csv(src_csv: Path, min_ymd: str, max_ymd: str) -> Optional[Path]:
    """Copy the processed CSV into the N26 archive tree.

    Target:
      /Users/joachimthomas/Finanzverwaltung/Archiv/N26/Kontobewegungen/YYYY/MM/

    File name:
      Kontobewegungen DD.MM.YY bis DD.MM.YY.csv

    Month folders are numeric (01..12) for Finder sorting.
    """
    try:
        if not src_csv or not src_csv.exists():
            return None
        max_ymd = (max_ymd or "").strip()
        min_ymd = (min_ymd or "").strip()
        if not DATE_RE.match(max_ymd):
            return None

        year, month, _ = max_ymd.split("-")  # month already "01".."12"
        ensure_arch_dirs(year, month)

        min_ddmmyy = ymd_to_ddmmyy(min_ymd)
        max_ddmmyy = ymd_to_ddmmyy(max_ymd)

        base_name = (
            f"Kontobewegungen {min_ddmmyy} bis {max_ddmmyy}.csv"
            if (min_ddmmyy and max_ddmmyy)
            else src_csv.name
        )

        dest = ARCH_ROOT / year / month / base_name
        dest = unique_dest(dest)

        dest.parent.mkdir(parents=True, exist_ok=True)
        import shutil

        shutil.copy2(str(src_csv), str(dest))
        return dest
    except Exception:
        return None


def ensure_dirs(year: str) -> None:
    (BASE_OUT / year / "Monatlich").mkdir(parents=True, exist_ok=True)
    (BASE_OUT / year / "Jährlich").mkdir(parents=True, exist_ok=True)


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
        tx = {}

    # collect tx for year
    items: List[Dict[str, Any]] = []
    for rec in tx.values():
        if not isinstance(rec, dict):
            continue
        d = (rec.get("bookingDate") or "").strip()
        if not ymd_ok(d):
            continue
        if d.startswith(year + "-"):
            items.append(rec)

    # sort by bookingDate, then id if present
    items.sort(key=lambda r: ((r.get("bookingDate") or ""), (r.get("id") or "")))

    year_dir = BASE_OUT / year / "Jährlich"
    month_dir = BASE_OUT / year / "Monatlich"

    # 1) YearOverview / Ledger (continuous)
    ledger_path = year_dir / f"{year}_YearOverview.csv"
    ledger_rows: List[List[str]] = []
    for r in items:
        d = (r.get("bookingDate") or "").strip()
        partner = (r.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        amt = float(r.get("amountValue", 0.0) or 0.0)
        memo = (r.get("memo") or r.get("Verwendungszweck") or r.get("purpose") or "").strip()
        ledger_rows.append([d, partner, fmt_de(amt), memo])
    write_csv(ledger_path, ["BookingDate", "Partner", "Amount_EUR", "Memo"], ledger_rows)

    # 2) Monthly partner report (income + expenses)
    by_month_exp = defaultdict(lambda: defaultdict(float))
    by_month_inc = defaultdict(lambda: defaultdict(float))

    for r in items:
        d = (r.get("bookingDate") or "").strip()
        if not ymd_ok(d):
            continue
        yyyymm = yyyymm_from_ymd(d)
        partner = (r.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        amt = float(r.get("amountValue", 0.0) or 0.0)
        if amt < 0:
            by_month_exp[yyyymm][partner] += -amt
        elif amt > 0:
            by_month_inc[yyyymm][partner] += amt

    for yyyymm in sorted(set(list(by_month_exp.keys()) + list(by_month_inc.keys()))):
        out_csv = month_dir / f"{yyyymm}_Partnerauswertung.csv"
        month_date = yyyymm_to_ddmmyyyy(yyyymm)

        expenses = by_month_exp.get(yyyymm, {})
        income = by_month_inc.get(yyyymm, {})

        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=DELIM_GUESS)

            # Ausgaben
            w.writerow(["Monat", "Partner", "Ausgaben EUR"])
            for partner, total in sorted(expenses.items(), key=lambda x: x[1], reverse=True):
                w.writerow([month_date, partner, fmt_de(total)])

            w.writerow([])
            w.writerow([])
            w.writerow([])

            # Einnahmen
            w.writerow(["Monat", "Partner", "Einnahmen EUR"])
            for partner, total in sorted(income.items(), key=lambda x: x[1], reverse=True):
                w.writerow([month_date, partner, fmt_de(total)])

    # 3) Year partner aggregates (Bislang + Kumuliert)
    by_partner_by_month = defaultdict(lambda: defaultdict(float))
    months_seen: Set[str] = set()
    latest_month = "1900-01"

    for r in items:
        d = (r.get("bookingDate") or "").strip()
        if not ymd_ok(d):
            continue
        m = yyyymm_from_ymd(d)
        partner = (r.get("partner") or "UNBEKANNT").strip() or "UNBEKANNT"
        amt = float(r.get("amountValue", 0.0) or 0.0)
        by_partner_by_month[partner][m] += amt
        months_seen.add(m)
        if m > latest_month:
            latest_month = m

    months = sorted(months_seen)
    latest_dd = yyyymm_to_ddmmyyyy(latest_month)

    # Bislang
    out_bis = year_dir / f"{year}_Partner_Bislang.csv"
    sums = {p: sum(mv.values()) for p, mv in by_partner_by_month.items()}
    with out_bis.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=DELIM_GUESS)
        w.writerow([f"Jahr {year} – Buchungen bislang (Monate bis {latest_dd})"])
        w.writerow(["Partner", "Summe EUR"])
        for p, total in sorted(sums.items(), key=lambda x: abs(x[1]), reverse=True):
            w.writerow([p, fmt_de(total)])

    # Kumuliert
    out_kum = year_dir / f"{year}_Partner_Kumuliert.csv"
    month_headers = [yyyymm_to_ddmmyyyy(m) for m in months]
    with out_kum.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=DELIM_GUESS)
        w.writerow(["Partner"] + month_headers)
        for p in sorted(by_partner_by_month.keys()):
            running = 0.0
            row = [p]
            for m in months:
                running += by_partner_by_month[p].get(m, 0.0)
                row.append(fmt_de(running))
            w.writerow(row)


# ============================================================
# Main
# ============================================================


def main() -> int:
    log("RUN start")
    n("INFO", "State & Reports Start")
    try:
        rules_cnt = len(load_partner_rules())
        log(f"STEP partner_rules_loaded | count={rules_cnt} path={PARTNER_RULES_PATH}")
    except Exception:
        log("STEP partner_rules_loaded | count=0")

    try:
        csv_path: Optional[Path] = None

        # argv[1] wins (explicit test runs)
        if len(sys.argv) >= 2:
            csv_path = Path(sys.argv[1]).expanduser()
            if not csv_path.exists() or not csv_path.is_file():
                log(f"STEP argv_csv_invalid | arg={sys.argv[1]}")
                csv_path = None
            else:
                log(f"STEP argv_csv_used | csv={csv_path}")

        # Otherwise: take the single CSV from the inbox folder (no 'latest' logic)
        if csv_path is None:
            try:
                CSV_INBOX_DIR.mkdir(parents=True, exist_ok=True)
                candidates = sorted([p for p in CSV_INBOX_DIR.glob("*.csv") if p.is_file()])
                csv_path = candidates[0] if candidates else None
            except Exception:
                csv_path = None

        if csv_path is None:
            log("STEP no_input_csv")
            n("WARN", "Kein N26 CSV gefunden (Inbox leer).")
            log("RUN end | result=no_input")
            return 0

        log(f"STEP start | csv={csv_path}")

        # Parse CSV
        rows = parse_csv_rows(csv_path)
        n("INFO", "N26-CSV eingelesen !")
        log(f"STEP csv_read_ok | rows={len(rows)}")

        # Archive the input CSV (best-effort).
        # IMPORTANT: do NOT delete the inbox file until AFTER state+reports,
        # because we still need it for hashing/meta in this run.
        min_ymd = min((r.booking_date for r in rows), default="")
        max_ymd = max((r.booking_date for r in rows), default="")

        archived: Optional[Path] = None
        delete_inbox_after = False

        try:
            archived = archive_csv(csv_path, min_ymd, max_ymd)
            if archived:
                log(f"STEP archive_ok | archived={archived}")
                n("OK", f"N26-CSV archiviert: {archived.name}")
                # delete later (after reports) to avoid FileNotFoundError in hashing/meta
                delete_inbox_after = True
            else:
                log("STEP archive_skip")
        except Exception:
            log("STEP archive_fail")

        if not rows:
            n("FAIL", "CSV gelesen, aber keine verwertbaren Buchungen gefunden.")
            log("STEP csv_empty_or_unusable")
            log("RUN end | result=fail")
            return 0

        # State update
        state = load_state()
        tx = state.setdefault("tx", {})
        if not isinstance(tx, dict):
            tx = {}
            state["tx"] = tx

        imported_at = now_iso_local()

        # Prefer archived file for hashing/meta if available, otherwise use inbox CSV.
        source_path_for_hash = (
            archived if (archived is not None and archived.exists()) else csv_path
        )
        source_hash = sha1_file_full(source_path_for_hash)
        source_uid = source_hash[:16]

        added = 0
        changed = 0
        skipped = 0
        bad = 0
        affected_years: Set[str] = set()

        min_d = "9999-99-99"
        max_d = "0000-00-00"

        for pr in rows:
            try:
                rid = record_id(pr)

                rec: Dict[str, Any] = {
                    "id": rid,
                    "bookingDate": pr.booking_date,
                    "partner": pr.partner,
                    "amountValue": round(float(pr.amount), 8),
                    "raw": pr.raw,
                    "ingestedAt": imported_at,
                    "sourceUid": source_uid,
                    "sourceFile": csv_path.name,
                }

                if pr.booking_date < min_d:
                    min_d = pr.booking_date
                if pr.booking_date > max_d:
                    max_d = pr.booking_date

                year = pr.booking_date[:4]

                if rid not in tx:
                    tx[rid] = rec
                    added += 1
                    affected_years.add(year)
                    continue

                existing = tx.get(rid)
                if isinstance(existing, dict) and core_equal(existing, rec):
                    skipped += 1
                    continue

                # update record in place
                tx[rid] = rec
                changed += 1
                affected_years.add(year)

            except Exception:
                bad += 1

        state_changed = (added + changed) > 0

        update_stats(state)

        # update meta
        li = (state.get("meta", {}) or {}).get("lastImport", {})
        if not isinstance(li, dict):
            li = {}
        li["sourceFile"] = source_path_for_hash.name
        li["sourcePath"] = str(source_path_for_hash)
        li["sourceUid"] = source_uid
        li["sourceHash"] = source_hash
        li["importedAt"] = imported_at
        li["period"] = {
            "from": "" if min_d == "9999-99-99" else min_d,
            "to": "" if max_d == "0000-00-00" else max_d,
        }
        li["affectedYears"] = sorted(affected_years)

        meta = state.setdefault("meta", {})
        if not isinstance(meta, dict):
            meta = {}
            state["meta"] = meta
        meta["lastImport"] = li

        save_state(state)

        if not state_changed:
            log(f"STEP state_unchanged | skipped={skipped} bad={bad}")
            n("INFO", "State unverändert (keine neuen/änderten Buchungen).")
            log("RUN end | result=unchanged")
            return 0

        log(
            f"STEP state_changed | added={added} changed={changed} skipped={skipped} bad={bad} years={sorted(affected_years)}"
        )
        n(
            "OK",
            f"State aktualisiert: +{added} neu, {changed} geändert (Jahre: {', '.join(sorted(affected_years))})",
        )

        # Reports (only affected years)
        for y in sorted(affected_years):
            build_reports_for_year(state, y)
            log(f"STEP reports_ok | year={y}")

        # Delete inbox CSV only after we are completely done (state + reports)
        if delete_inbox_after:
            try:
                # Only delete if the original inbox file still exists
                if csv_path.exists():
                    csv_path.unlink(missing_ok=True)
                log(f"STEP inbox_delete_ok | csv={csv_path}")
            except Exception:
                log(f"STEP inbox_delete_fail | csv={csv_path}")

        n("OK", "Reports erstellt – Pipeline Ende")
        log("RUN end | result=updated")
        return 0

    except Exception as e:
        log(f"FAIL exception | {type(e).__name__}: {e}")
        n("FAIL", f"Fehler: {type(e).__name__} (siehe Log)")
        log("RUN end | result=fail")
        return 0


if __name__ == "__main__":
    # Automation requires RC=0 always
    try:
        main()
    except Exception:
        pass
    raise SystemExit(0)
