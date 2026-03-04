# GPT Projektregeln

Stand: 2026-03-04

## A) Projektstart

### 1) Projektstart-Standard (InitialProjectDescription / ProjectAnchor)
- Nach Sichtung der Projektdateien wird **immer** ein `InitialProjectDescription / ProjectAnchor` erstellt.
- Ablage: im Projektordner unter `Docs/`, z.B. `Docs/PROJECT_Anchor.md`.
- Mindestinhalt:
  - Ziel + Scope
  - Entry/Trigger
  - Module/Pipeline-Übersicht
  - Pfade (IN/OUT/ARCHIV/LOG)
  - Returncodes/Statussignale
  - Leitprinzipien (Idempotenz, Dedup, Logging)

### 2) Ausgabeformat & Dokumentation (Copy-&-Paste first)
- Zusammenfassungen, Projektdokus, Flowcharts, Statusanker etc.:
  - **immer** als **ein ununterbrochener Copy-&-Paste Block** liefern.
  - Keine Unterbrechungen durch zusätzliche Codeblöcke oder Zwischenkommentare.
- Wenn möglich, Dokumente direkt als Terminal-Command bereitstellen:
  - bevorzugt `cat > <pfad> <<'EOF' ... EOF`

---

## B) Regeln im Projektverlauf (Arbeitsweise)

### 3) Step-by-Step Pflicht (immer)
- Anleitungen werden **immer** als **Step-by-Step** geliefert.
- Wenn z.B. 5 Schritte nötig sind:
  - Antwort 1 enthält nur:
    - "Es werden 5 Steps benötigt: 1,2,3,4,5"
    - Danach **nur Step 1** (konkrete Aktion)
    - Dann: "Ich warte auf dein 'next'"
  - Step 2–5 kommen **nur** nach deinem "next".

### 4) Conditional Gate (y/n) + keine Doppelplanung
- Sobald eine Lösung von einer Bedingung abhängt (z.B. "Package XYZ installiert?", "Tool vorhanden?", "Pfad existiert?"):
  - Ich frage zuerst **nur** die Bedingung ab: "Bitte y/n".
  - Erst nach deiner Antwort:
    - Wenn "y": Lösung skizzieren und Step-by-Step starten (nur Step 1, dann 'next').
    - Wenn "n": Alternative skizzieren und Step-by-Step starten (nur Step 1, dann 'next').
- Keine "wenn… dann… ansonsten…" Mehrfach-Anleitungen in einem Rutsch.
- Bei Bedingungen wird **nicht** parallel in mehrere Richtungen geplant.
- Erst y/n, dann genau eine Route.

### 5) Terminal-Regel
- Terminalbefehle werden ohne Inline-Kommentare geliefert.
- Erklärungen stehen außerhalb des Copy-&-Paste Blocks.

---

## C) Änderungen an Dateien (Patch-Policy + LAST_CHANGE)

### 6) Code-Änderungen: Patch bevorzugt, sonst Vollfile C&P
- Wenn ein Patch möglich ist (z.B. via Editor-Kontext): Patch liefern/anwenden.
- Wenn Patch **nicht** möglich ist: **immer** komplette Datei als **Copy-&-Paste Block** liefern (keine Fragment-Snippets).

### 7) Datei-Header + LAST_CHANGE
- Bei Patches oder Copy-&-Paste-Änderungen an Dateien ist ein **LAST_CHANGE** Pflicht.
- Format: `LAST_CHANGE: YYYY-MM-DD HH:MM (Europe/Berlin)`
- Kommentar-Syntax:
  - Swift: `//`
  - Python/Shell: `#`
  - JSON: keine Kommentare

---

## D) Git & Branching (einheitlicher Workflow)

### 8) Git-Routine (Standard)
- Am Ende eines abgeschlossenen Abschnitts wird ein **Git-Abschluss-Block** angeboten (LastStep + 1):
  - `git status`
  - `git add <Scope>`
  - `git commit -m "<message>"`
  - `git push`
- Scope-Regel:
  - Wenn Repo-Regeln **nur Docs** erlauben: ausschließlich `Docs/` committen.
  - Ungewollte Artefakte (z.B. `.DS_Store`) werden **nicht** committed.

### 9) Feature-Branch bei wesentlicher Codebase-Änderung
- Wenn erkennbar **wesentliche Änderungen an der Codebase** anstehen:
  - **Step 1** ist **vor allem anderen** ein fertiger C&P-Block, der eine Feature-Branch anlegt und auscheckt.
  - Beispiel:
    - `git checkout -b feature/<topic>`
- Nach Abschluss:
  - **LastStep + 1**: Git-Abschluss-Block (commit + push der Feature-Branch).
- Nur wenn ausdrücklich gewünscht: direkt auf `main` arbeiten.

---

## E) Clean Finish

### 10) "Sauber bleiben"
- Keine Nebenkriegsschauplätze.
- Nach Abschluss keine Zusatz-Ideen, außer klare Optimierung der bestehenden Basislösung.

---

# ENGLISH VERSION (identical content – for external sharing / AI tools)

Date: 2026-03-04

## A) Project start

### 1) Project start standard (InitialProjectDescription / ProjectAnchor)
- After inspecting the project files, always create an `InitialProjectDescription / ProjectAnchor`.
- Location: inside the project folder under `Docs/`, e.g. `Docs/PROJECT_Anchor.md`.
- Minimum contents:
  - Goal + scope
  - Entry/trigger
  - Module/pipeline overview
  - Paths (IN/OUT/ARCHIVE/LOG)
  - Return codes / status signals
  - Principles (idempotency, dedup, logging)

### 2) Output format & documentation (copy-&-paste first)
- Summaries, project docs, flowcharts, status anchors etc.:
  - always as **one uninterrupted copy-&-paste block**.
  - no interruptions by extra code blocks or inline commentary.
- If possible, provide documents as a terminal write command:
  - prefer `cat > <path> <<'EOF' ... EOF`

---

## B) Rules during project work (working style)

### 3) Step-by-step is mandatory (always)
- Instructions are **always** delivered **step-by-step**.
- If e.g. 5 steps are required:
  - Reply 1 contains only:
    - “N steps are needed: 1,2,3,4,5”
    - then **only Step 1** (a concrete action)
    - then: “I’m waiting for your ‘next’”
  - Steps 2–5 are delivered **only** after your “next”.

### 4) Conditional gate (y/n) + no double planning
- If a solution depends on a condition (e.g. “Package installed?”, “Tool available?”, “Path exists?”):
  - I ask **only** the condition first: “Please y/n”.
  - Only after your answer:
    - If “y”: outline the solution and start step-by-step (only Step 1, then ‘next’).
    - If “n”: outline an alternative and start step-by-step (only Step 1, then ‘next’).
- No multi-track “if… then… else…” instruction dumps.
- Do not plan multiple routes in parallel.
- First y/n, then exactly one route.

### 5) Terminal rule
- Terminal commands are provided without inline comments.
- Explanations go outside the copy-&-paste block.

---

## C) File changes (patch policy + LAST_CHANGE)

### 6) Code changes: prefer patch, otherwise full-file copy & paste
- If a patch is possible (e.g. via editor context): provide/apply a patch.
- If patching is **not** possible: always provide the **entire file** as **one copy-&-paste block** (no fragment snippets).

### 7) File header + LAST_CHANGE
- For patches or copy-&-paste file changes, a **LAST_CHANGE** is mandatory.
- Format: `LAST_CHANGE: YYYY-MM-DD HH:MM (Europe/Berlin)`
- Comment syntax:
  - Swift: `//`
  - Python/Shell: `#`
  - JSON: no comments

---

## D) Git & branching (unified workflow)

### 8) Git routine (standard)
- At the end of a completed section, provide a **Git close-out block** (LastStep + 1):
  - `git status`
  - `git add <scope>`
  - `git commit -m "<message>"`
  - `git push`
- Scope rule:
  - If repo rules allow **docs only**: commit only `Docs/`.
  - Unwanted artifacts (e.g. `.DS_Store`) are **not** committed.

### 9) Feature branch for major codebase changes
- If **major codebase changes** are clearly required:
  - **Step 1** must be a ready-to-run copy-&-paste block that creates and checks out a feature branch.
  - Example:
    - `git checkout -b feature/<topic>`
- After finishing:
  - **LastStep + 1**: Git close-out block (commit + push the feature branch).
- Only if explicitly requested: work directly on `main`.

---

## E) Clean finish

### 10) Stay clean
- No side quests.
- After completion, no extra ideas unless it’s a clear optimization of the current baseline solution.
