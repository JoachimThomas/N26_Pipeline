# PROJECT_Anchor — N26_Pipeline

LAST_CHANGE: 2026-03-04 17:39 (Europe/Berlin)

## Ziel + Scope
Ziel: N26-Daten automatisiert in eine robuste State-Struktur überführen und daraus Reports erzeugen — plus separater Balance-Import aus N26-PDF-Auszügen ins globale Finance-State.

Scope (dieses Repo):
- N26 CSV → `n26_state.json` (dedup/idempotent) → Reports pro Jahr/Monat
- Partner-Normalisierung über `partner_regeln.csv`
- Re-Normalisierung + Rebuild ohne neue CSVs
- N26 Konto-Balance aus PDF-Auszug extrahieren → `global_finance_state.json` aktualisieren (nur wenn neuer)

Nicht Scope:
- TR/IG Pipelines (nur indirekt über `global_finance_state.json`-Zielstruktur)

## Entry / Trigger
### A) CSV Import (Kontobewegungen)
Script: `n26_state_and_reports.py`
Trigger: Single-Run mit CSV-Pfad als argv[1] oder Auto-Pick “neueste CSV”
Erwarteter Inbox-Ordner:
- `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Kontobewegungen`

Archivierung (processed CSVs):
- `/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Kontobewegungen`

### B) Partner-Rebuild ohne neue CSV
Script: `n26_rebuild_reports.py`
Trigger: manuell (CLI) für `--year`, `--years`, `--all-years`
Usecase: Regeln in `partner_regeln.csv` geändert → alles neu normalisieren + Reports neu bauen

### C) Balance-Update aus PDF-Auszug
Script: `N26_BalanceUpdate.py`
Trigger: Single-Run; erwartet genau 1 PDF in INBOX
Inbox:
- `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Auszüge/INBOX`
Archiv:
- `/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Auszüge`
Ablage (Bank-Ordner-Struktur):
- `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Auszüge` (Unterordner z.B. „Auszüge 2026/01..12“)

## Module / Pipeline-Übersicht
### 1) n26_state_and_reports.py (CSV → State → Reports)
- Input: CSV (explizit oder Auto-Discover)
- Parsing: robust gegen Header-Varianten, Delimiter (Default “;”)
- Normalisierung:
  - Datum → YYYY-MM-DD
  - Betrag → float (DE/EN Money tolerant)
  - Partner → optional via Regeln
- Dedup: Record-ID via sha1(key) gekürzt
- State:
  - `~/Library/Application Support/Finanzen/N26/n26_state.json`
  - enthält `tx` dict + `meta.lastImport` + `stats`
- Reports:
  - nur betroffene Jahre (affectedYears) werden rebuildet
- Logging:
  - `/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log`
- Notifications:
  - `/Users/joachimthomas/Finanzverwaltung/Programme/Global/finance_notify.sh` (Scope “N26”)

### 2) n26_rebuild_reports.py (State → Re-Normalize → Rebuild)
- Lädt State + Partner-Regeln
- Normalisiert Partner für ALLE Records erneut
- Recomputes Record-IDs (Duplikate können kollabieren)
- Backup vom State vor Write
- Rebuild Reports für ausgewählte Jahre

### 3) N26_BalanceUpdate.py (PDF → global_finance_state)
- PDF Text via `pdfplumber`
- Extract:
  - Zeitraum (dd.mm.yyyy bis dd.mm.yyyy)
  - Label: „Dein neuer Kontostand“ → Betrag
- Update-Ziel:
  - `/Users/joachimthomas/Library/Application Support/Finanzen/global_finance_state.json`
  - `accounts.N26.value/asOfDate/updatedAt`
- Update-Regel: nur schreiben, wenn `asOfDate` neuer (oder gleich aber updatedAt neuer)
- Logging:
  - `/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log`
- Notifications:
  - finance_notify (Scope “N26”)

## Pfade (IN / OUT / STATE / ARCHIV / LOG)
### State
- N26 State:
  - `~/Library/Application Support/Finanzen/N26/n26_state.json`
- Global Finance State:
  - `/Users/joachimthomas/Library/Application Support/Finanzen/global_finance_state.json`

### IN
- CSV Inbox:
  - `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Kontobewegungen`
- PDF Inbox:
  - `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Auszüge/INBOX`

### OUT
- Reports Base:
  - `/Users/joachimthomas/Documents/Joachim privat/Banken/N26/Reports`
- Report-Struktur (typisch):
  - `<YEAR>/Jährlich/<YEAR>_YearOverview.csv`
  - `<YEAR>/Monatlich/<YYYY-MM>_Partnerauswertung.csv`
- Zusätzlich “Legacy/Bequemlichkeit” (n26_rebuild_reports.py):
  - Root: `/Users/.../Reports/<YEAR>_YearOverview.csv`
  - `/Users/.../Reports/Jahresübersicht/<YEAR>_YearOverview.csv`

### ARCHIV
- CSV:
  - `/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Kontobewegungen`
- PDF:
  - `/Users/joachimthomas/Finanzverwaltung/Archiv/N26/Auszüge`

### LOG
- `/Users/joachimthomas/Finanzverwaltung/Programme/Logs/N26/n26_.log`

### Regeln / Konfig
- Partner-Regeln:
  - `/Users/joachimthomas/Finanzverwaltung/Programme/N26/partner_regeln.csv`

## Returncodes / Statussignale
### n26_state_and_reports.py
- 0  = State geändert (Records added/updated) + Reports für betroffene Jahre rebuilt
- 10 = No-op (State unverändert) → keine Reports
- 1  = Fehler

### n26_rebuild_reports.py / N26_BalanceUpdate.py
- (Returncodes noch nicht im README standardisiert; aktuell: “best effort”/exceptions → Fehlerfall via notify/log sichtbar)

## Leitprinzipien
- Idempotenz: erneutes Laufen mit gleicher Quelle produziert keinen Drift
- Dedup: stabile IDs (sha1-key) verhindern Duplikate
- Atomare Writes: State via tmp → replace
- Year-scoped rebuild: Reports nur für betroffene Jahre
- Single-Inbox: genau 1 Input-Datei je Lauf (CSV/PDF) als Pipeline-Disziplin
- Logging + Notify: Diagnose muss ohne “print-debugging” möglich sein

## Offene Punkte / Beobachtungen
- LOG_PATH ist in mehreren Scripts `.../n26_.log` (Platzhaltername): ggf. später vereinheitlichen
- `n26_rebuild_reports.py` schreibt bewusst zusätzliche Legacy-Paths (Root + Jahresübersicht)
- Partner-Regeln: Regex-Patterns + canonical, delimiter automatisch ermittelt

