#!/Library/Frameworks/Python.framework/Versions/3.14/bin/python3
# # -*- coding: utf-8 -*-

import re
import json
import sys
import subprocess
import traceback
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple

import pdfplumber

# ============================================================
# CONFIG
# ============================================================

STATE_PATH = Path(
    "/Users/joachimthomas/Library/Application Support/Finanzen/global_finance_state.json"
)

FIN_NOTIFY = "/Users/joachimthomas/Finanzverwaltung/Programme/Global/finance_notify.sh"

LOG_PATH = Path("/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log")

# ✅ FIX: INBOX ist die einzige Quelle (genau 1 PDF)
N26_INBOX = Path("/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Auszüge/INBOX")

# Archiv (Finanzverwaltung)
N26_ARCHIVE_BASE = Path("/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Auszüge")

# ✅ FIX: Bank-Ordner Basis (darin existiert: "Auszüge 2026/01..12")
N26_DOCS_BASE = Path("/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Auszüge")

LABEL_NEW_BALANCE = "Dein neuer Kontostand"

AMOUNT_RE = re.compile(r"([+-]?\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*€")
PERIOD_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})\s+bis\s+(\d{2}\.\d{2}\.\d{4})")


# ============================================================
# Helpers
# ============================================================


def now_iso_local() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def ddmmyyyy_to_ymd(s: str) -> str:
    s = (s or "").strip()
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if not m:
        return ""
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd}"


def ymd_to_year_month(ymd: str) -> Tuple[str, str]:
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", (ymd or "").strip())
    if not m:
        return ("unknown", "00")
    return (m.group(1), m.group(2))


def unique_dest_path(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suf = dest.stem, dest.suffix
    i = 2
    while True:
        cand = dest.with_name(f"{stem}_{i}{suf}")
        if not cand.exists():
            return cand
        i += 1


def _ensure_log_dir() -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def log_line(msg: str) -> None:
    _ensure_log_dir()
    try:
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def n(code: str, msg: str) -> None:
    try:
        subprocess.run(
            [FIN_NOTIFY, "N26", code, msg, __file__],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def ymd_key(ymd: str) -> Tuple[int, int, int]:
    ymd = (ymd or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", ymd):
        return (0, 0, 0)
    try:
        dt = datetime.strptime(ymd, "%Y-%m-%d")
        return (dt.year, dt.month, dt.day)
    except Exception:
        return (0, 0, 0)


def parse_de_money_to_float(s: str) -> float:
    s = (s or "").strip()
    if not s:
        return 0.0
    s = s.replace("€", "").replace("EUR", "").replace(" ", "").replace("+", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def extract_text(pdf_path: Path) -> str:
    chunks = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                chunks.append(txt)
    return "\n".join(chunks)


def detect_period_info(lines: list[str]) -> Tuple[str, str, str]:
    joined = " ".join(lines)
    m = PERIOD_RE.search(joined)
    if not m:
        return ("", "", "")
    start_ddmmyyyy = (m.group(1) or "").strip()
    end_ddmmyyyy = (m.group(2) or "").strip()
    end_ymd = ddmmyyyy_to_ymd(end_ddmmyyyy)
    return (start_ddmmyyyy, end_ddmmyyyy, end_ymd)


def find_labeled_amount(lines: list[str], label: str) -> Optional[str]:
    label_l = (label or "").lower()
    for i, ln in enumerate(lines):
        if label_l in ln.lower():
            m = AMOUNT_RE.search(ln.replace("\u00a0", " "))
            if m:
                return m.group(1).replace(" ", "")
            for j in range(i + 1, min(i + 3, len(lines))):
                m2 = AMOUNT_RE.search(lines[j].replace("\u00a0", " "))
                if m2:
                    return m2.group(1).replace(" ", "")
    return None


def wait_until_file_stable(p: Path, checks: int = 3, delay_s: float = 0.7) -> None:
    """Warte bis Dateigröße stabil ist (Download/Sync fertig)."""
    last = -1
    stable = 0
    while stable < checks:
        try:
            sz = p.stat().st_size
        except Exception:
            sz = -1
        if sz == last and sz > 0:
            stable += 1
        else:
            stable = 0
            last = sz
        import time

        time.sleep(delay_s)


# ============================================================
# global_finance_state.json (atomic)
# ============================================================


def ensure_state_exists():
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if STATE_PATH.exists():
        return
    payload = {
        "updatedAt": "",
        "accounts": {
            "N26": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
            "TR_Cash": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
            "TR_Invested": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
            "ZERO": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
            "IG": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
            "BAR": {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""},
        },
    }
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def update_state_account_if_newer(
    key: str, value: float, as_of_date: str, ingested_at: str
) -> bool:
    ensure_state_exists()

    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {"updatedAt": "", "accounts": {}}

    if "accounts" not in data or not isinstance(data["accounts"], dict):
        data["accounts"] = {}

    if key not in data["accounts"] or not isinstance(data["accounts"].get(key), dict):
        data["accounts"][key] = {"value": 0.0, "currency": "EUR", "asOfDate": "", "updatedAt": ""}

    acc = data["accounts"][key]
    existing_asof = (acc.get("asOfDate") or "").strip()
    existing_upd = (acc.get("updatedAt") or "").strip()

    new_asof = (as_of_date or "").strip()
    new_upd = (ingested_at or now_iso_local()).strip()

    if ymd_key(existing_asof) != (0, 0, 0) and ymd_key(new_asof) != (0, 0, 0):
        if ymd_key(new_asof) < ymd_key(existing_asof):
            return False
        if ymd_key(new_asof) == ymd_key(existing_asof):
            if existing_upd and new_upd and new_upd <= existing_upd:
                return False

    acc["value"] = round(float(value), 2)
    acc["currency"] = acc.get("currency", "EUR")
    acc["asOfDate"] = new_asof
    acc["updatedAt"] = new_upd
    data["accounts"][key] = acc
    data["updatedAt"] = new_upd

    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)
    return True


# ============================================================
# Archiv + Bank-Ablage
# ============================================================


def archive_and_sort_pdf(
    src_pdf: Path, start_ddmmyyyy: str, end_ddmmyyyy: str, end_ymd: str
) -> Optional[Path]:
    """
    1) Move src PDF -> Finanzverwaltung-Archiv YYYY/MM
    2) Copy archived PDF -> Bankordner: N26_DOCS_BASE / "Auszüge YYYY" / MM
    """
    try:
        yy, mm = ymd_to_year_month(end_ymd)
        if yy == "unknown" or mm == "00":
            return None

        if (start_ddmmyyyy or "").strip() and (end_ddmmyyyy or "").strip():
            fname = f"N26 Auszug_{start_ddmmyyyy}_bis_{end_ymd}.pdf"
        else:
            fname = f"N26 Auszug vom {end_ymd}.pdf"

        # 1) Archiv
        arch_dir = N26_ARCHIVE_BASE / yy / mm
        arch_dir.mkdir(parents=True, exist_ok=True)
        arch_dest = unique_dest_path(arch_dir / fname)

        src_pdf = src_pdf.expanduser()
        if not src_pdf.exists() or not src_pdf.is_file():
            return None
        src_pdf.replace(arch_dest)

        # 2) Bank-Ablage: "Auszüge 2026/01..12"
        docs_dir = N26_DOCS_BASE / f"Auszüge {yy}" / mm
        docs_dir.mkdir(parents=True, exist_ok=True)
        docs_dest = unique_dest_path(docs_dir / fname)

        import shutil

        try:
            shutil.copy2(str(arch_dest), str(docs_dest))
        except Exception:
            pass

        return arch_dest
    except Exception:
        return None


def inbox_single_pdf(inbox: Path) -> Optional[Path]:
    if not inbox.exists() or not inbox.is_dir():
        return None
    pdfs = sorted(inbox.glob("*.pdf"))
    if len(pdfs) != 1:
        return None
    return pdfs[0]


# ============================================================
# Main processing
# ============================================================


def process_one_pdf(pdf_path: Path) -> Tuple[int, str]:
    log_line(f"STEP start | pdf={pdf_path}")
    n("INFO", f"Start: N26-Auszug verarbeiten ({pdf_path.name})")

    wait_until_file_stable(pdf_path)

    try:
        text = extract_text(pdf_path)
    except Exception as e:
        log_line(f"ERROR extract_text: {e}")
        n("FAIL", f"PDF lesen fehlgeschlagen: {pdf_path.name}")
        return (1, "pdf_read_failed")

    lines = [ln.strip() for ln in text.splitlines() if (ln or "").strip()]
    log_line(f"STEP pdf_read_ok | lines={len(lines)}")

    start_ddmmyyyy, end_ddmmyyyy, end_ymd = detect_period_info(lines)
    if not end_ymd:
        today = datetime.now(timezone.utc).astimezone()
        end_ymd = today.strftime("%Y-%m-%d")
        start_ddmmyyyy = today.strftime("%d.%m.%Y")
        end_ddmmyyyy = start_ddmmyyyy
        log_line(f"WARN period_not_found -> fallback_today end_ymd={end_ymd}")
        n("WARN", f"PDF-Periode nicht gefunden – Fallback: {end_ymd}")

    archived_pdf = archive_and_sort_pdf(pdf_path, start_ddmmyyyy, end_ddmmyyyy, end_ymd)
    if archived_pdf:
        log_line(f"STEP archive_ok | archived={archived_pdf}")
        n("OK", f"PDF archiviert: {archived_pdf.name}")
        pdf_path = archived_pdf
    else:
        log_line("WARN archive_failed_or_skipped")
        n("WARN", "PDF konnte nicht archiviert/einsortiert werden")

    amt_raw = find_labeled_amount(lines, LABEL_NEW_BALANCE)
    if not amt_raw:
        log_line("ERROR labeled_amount_not_found")
        n("FAIL", f"Kontostand nicht gefunden (Label: {LABEL_NEW_BALANCE})")
        return (1, "balance_not_found")

    value = parse_de_money_to_float(amt_raw)
    ingested_at = now_iso_local()
    log_line(f"STEP parsed_ok | asOf={end_ymd} value={value:.2f}")

    try:
        updated = update_state_account_if_newer("N26", value, end_ymd, ingested_at)
    except Exception as e:
        log_line(f"ERROR state_update_exception: {e}")
        n("FAIL", "State-Update fehlgeschlagen (Exception)")
        return (1, "state_update_failed")

    if updated:
        log_line("STEP state_update_ok | updated=true")
        n("OK", f"State aktualisiert: {end_ymd} ({value:.2f} EUR)")
        log_line("STEP end | result=updated")
        n("DONE", "Ende: N26-Auszugerfassung beendet!")
        return (0, "updated")

    log_line("STEP state_unchanged | updated=false")
    n("SKIP", f"State unverändert (nicht neuer): {end_ymd}")
    log_line("STEP end | result=unchanged")
    n("DONE", "Ende: N26-Auszug geprüft (keine Änderung)")
    return (10, "unchanged")


def main():
    log_line("RUN start")

    # ✅ Keine argv-Übergabe mehr: nur INBOX
    p = inbox_single_pdf(N26_INBOX)
    if not p:
        # Kein oder mehr als 1 PDF in der INBOX ist KEIN Fehler (Launchd-Trigger kann ohne Datei feuern)
        try:
            cnt = len(list(N26_INBOX.glob("*.pdf"))) if N26_INBOX.exists() else -1
        except Exception:
            cnt = -1
        log_line(f"SKIP: expected exactly 1 PDF in INBOX. found={cnt} inbox={N26_INBOX}")
        if cnt == 0:
            n("SKIP", "INBOX: kein PDF gefunden (nichts zu tun).")
        else:
            n("WARN", f"INBOX: erwartet genau 1 PDF, gefunden={cnt} (nichts zu tun).")
        log_line("RUN end | result=skipped_no_pdf")
        sys.exit(0)

    if not p.exists() or not p.is_file():
        log_line(f"SKIP: not found/file: {p}")
        n("SKIP", "INBOX: PDF ist nicht (mehr) vorhanden (nichts zu tun).")
        log_line("RUN end | result=skipped_missing_pdf")
        sys.exit(0)

    try:
        rc, reason = process_one_pdf(p)
    except Exception as e:
        log_line(f"FATAL: {e}")
        log_line(traceback.format_exc())
        n("FAIL", "Unerwarteter Fehler (siehe Log)")
        sys.exit(1)

    if rc in (0, 10):
        sys.exit(0)

    log_line(f"FAIL: processing failed ({reason})")
    sys.exit(1)


if __name__ == "__main__":
    main()
