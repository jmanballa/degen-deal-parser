# AGENTS.md

## Project

Degen Collectibles Discord deal / ledger parser.

This project ingests Discord deal-log messages, stores raw messages in SQLite, parses them into structured transactions, normalizes them for financial reporting, and provides a FastAPI/Jinja review UI.

Current stack:
- Python
- FastAPI
- discord.py
- SQLite
- OpenAI API
- HTML/Jinja templates

## Run

Use the venv Python directly on Windows:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Main pages:
- `/table`
- `/review-table`
- `/reports`
- `/bookkeeping`

## Core Architecture

Main flow:
1. `discord_ingest.py`
   - listens to Discord
   - stores raw `DiscordMessage` rows
   - can auto-import public Google Sheets bookkeeping links
2. `worker.py`
   - processes queued/failed rows
   - stitches nearby messages into one deal when appropriate
   - calls parser + financial normalization
3. `parser.py`
   - rule-first parsing
   - OpenAI fallback for ambiguous/image-heavy cases
   - store-specific override logic
4. `transactions.py`
   - syncs normalized `Transaction` and `TransactionItem` rows
5. `reporting.py`
   - financial/report summaries
6. `bookkeeping.py`
   - bookkeeping sheet import + reconciliation

Design principle:
- `DiscordMessage` is the raw audit trail
- `Transaction` is the normalized reporting layer
- parser rules should beat AI when store shorthand is explicit

## Important Store Rules

These conventions should be preserved unless the user explicitly asks to change them:

- `out` means items leaving the store
- `in` means items coming into the store
- `top out bottom in` means a trade
- `plus 195 zelle` in a trade usually means money to the store
- `tap` means card
- payment-only logs like `$11 zelle` or `zelle $11` default to a sell unless stronger context says otherwise
- image-first + follow-up payment/message can be one transaction
- unrelated nearby deals should not be stitched together
- card buys/sells/trades should use `expense_category = inventory`

## Current UX / Data Behavior

- main queue is `/table`
- review queue is `/review-table`
- review table supports inline row editing for common fields
- grouped/stiched messages are shown in the table
- child rows in stitched groups should be cleared and marked `ignored`
- filter channel list should only include watched channels or channels with stored messages

## Bookkeeping

Current bookkeeping behavior:
- `/bookkeeping` supports `.csv` and `.xlsx` upload
- public Google Sheets links posted in watched Discord channels should auto-import
- bookkeeping imports are used as ground truth for reconciliation, not direct model training

Important:
- prefer sheet export/import data over screenshot previews
- reconciliation should compare against normalized `Transaction` rows

## Module Ownership Suggestions

When using multiple agents, keep write scopes separate.

Parser / Stitching agent:
- `app/parser.py`
- `app/worker.py`

UI / Review Workflow agent:
- `app/templates/messages_table.html`
- `app/templates/message_detail.html`
- `app/templates/reports.html`
- `app/templates/bookkeeping.html`

Data / Reporting agent:
- `app/models.py`
- `app/transactions.py`
- `app/reporting.py`
- `app/bookkeeping.py`
- `app/financials.py`

Infra / Routing agent:
- `app/main.py`
- `app/channels.py`
- `app/discord_ingest.py`
- `app/db.py`
- `app/config.py`

Do not assign overlapping files to multiple agents at the same time.

## Safe Change Priorities

Preferred order of operations for parser changes:
1. improve rule-based detection
2. improve stitch heuristics
3. improve explicit text overrides
4. improve correction memory usage
5. only then adjust prompts / AI reliance

Preferred order for review/reporting changes:
1. preserve current working review flow
2. keep the main table readable
3. add reporting/reconciliation without breaking ingestion

## Testing / Verification

Minimum verification after code changes:

```powershell
.\.venv\Scripts\python.exe -m compileall app
```

When touching parser/stitching logic, also sanity-check:
- image-first then text
- payment-only sell default
- explicit buy/sell text overriding trade-like image guesses
- grouped child rows no longer producing duplicate transactions

When touching bookkeeping:
- upload `.xlsx`
- upload `.csv`
- public Google Sheets auto-import path
- reconciliation page loads

## Notes For Future Agents

- SQLite schema has evolved additively; avoid reset-based development if possible
- do not break working buys/sells/trades while improving expense handling
- if a row looks wrong in the UI, always check whether the real issue is:
  - bad stitching
  - image-only AI guess
  - stale child grouped row data
  - transaction sync not being removed
- if debugging parser results, prefer fixing deterministic rules before making the AI prompt more complex
