# AGENTS.md

## Project

Degen Collectibles — Discord deal parser + TikTok Shop livestream platform.

This project has two major verticals:
1. **Discord deal parsing** — ingests Discord deal-log messages, stores raw messages, parses them into structured transactions, normalizes them for financial reporting
2. **TikTok Shop livestream tools** — order sync, live streamer dashboard, analytics, and product management

Current stack:
- Python 3.14, FastAPI, Uvicorn
- discord.py for Discord ingestion
- PostgreSQL (production on Machine B) / SQLite (local dev on Machine A) with SQLModel ORM
- OpenAI API (parser fallback only)
- TikTok Shop Open Platform API (orders, products, live analytics)
- TikSync SDK (live chat WebSocket)
- Jinja2 templates with vanilla JS + Chart.js
- Role-based auth (admin, reviewer, viewer)
## Core Principles (VERY IMPORTANT)

### 1. Source of truth
- `DiscordMessage` is immutable audit log
- All parsing must be reproducible from raw messages

### 2. Determinism first
- Prefer rule-based parsing over AI
- AI is fallback, not primary logic

### 3. No silent failures
- Every failure must be visible via logs or UI

### 4. Do not guess
- If behavior is unclear, inspect and explain before coding

### 5. No broad refactors unless explicitly requested

## Run

**Local dev (web-only, SQLite, no Discord/worker):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1
```

**Local dev against production Postgres (Machine B DB via Tailscale, no Discord/worker):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web_pg.ps1
```

**Production (Machine B — web + worker + auto-restart):**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1
```

### Script reference

| Script | Purpose |
|---|---|
| `scripts/run_local_web.ps1` | Local dev server on Machine A with SQLite. Discord ingest, worker, and backfill disabled. |
| `scripts/run_local_web_pg.ps1` | Local dev server on Machine A connected to Machine B's PostgreSQL via Tailscale. Same disabled services as above. |
| `scripts/run_hosted.ps1` | Production server on Machine B. Runs web + Discord ingest + worker with auto-restart. |
| `scripts/tiktok_backfill.py` | TikTok API utility — order/product backfill, analytics API calls, HMAC signing. |
| `scripts/sig_debug.py` | Webhook signature debugger — tests signing algorithms against a captured webhook body. |

**Quick compile check after code changes:**
```powershell
.\.venv\Scripts\python.exe -m compileall app
```

### Main pages

Discord side:
- `/table` — main deal queue
- `/review-table` — review queue
- `/reports` — financial reports
- `/bookkeeping` — sheet import + reconciliation

TikTok side:
- `/tiktok/orders` — order listing with livestream filter
- `/tiktok/streamer` — live streamer dashboard (real-time orders, GMV, chat, goal bar, alerts)
- `/tiktok/analytics` — stream analytics, buyer tracking, product performance, stream comparison
- `/tiktok/streamer/config` — stream time config + GMV goal + alert thresholds

Admin:
- `/admin/home` — admin dashboard
- `/admin/debug` — system diagnostics
- `/admin/users` — user management

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
- image-first + follow-up payment/message can be one transaction (up to 45s gap)
- unrelated nearby deals should not be stitched together
- card buys/sells/trades should use `expense_category = inventory`
- `2x`, `3x` etc. are quantity multipliers, NOT dollar amounts — the amount extractor skips these

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

TikTok / Streamer agent:
- `app/tiktok_ingest.py`
- `app/tiktok_auth_refresh.py`
- `app/tiktok_live_chat.py`
- `scripts/tiktok_backfill.py`
- `app/templates/tiktok_streamer.html`
- `app/templates/tiktok_analytics.html`
- `app/templates/tiktok_orders.html`

Do not assign overlapping files to multiple agents at the same time.

**WARNING: `app/main.py` is 10,000+ lines.** It contains routes for both Discord and TikTok features, plus all background task logic. This is the highest-priority refactoring target. Be careful when editing — changes can have wide blast radius.

## Stitching Behavior (CRITICAL)

The worker stitches nearby messages from the same author into a single deal. Key parameters:

- `stitch_window_seconds` (default 30) — how far back/forward to look for candidates
- `stitch_max_messages` (default 3) — max messages per stitch group
- `has_large_gap` (default 12s) — general inter-message gap limit for stitching
- `should_force_stitch` — overrides gap limits for high-confidence patterns

### Force-stitch patterns (bypass general gap limit)

- **Image + explicit buy/sell text** (either order): up to **45 seconds** gap. This covers the common Discord pattern where someone posts a photo then types "Buy 1856 cash" 20-30 seconds later.
- Other force-stitch patterns: 8 seconds max.

### Stitch profile categories

The stitching logic classifies each message into profile categories to decide what context is missing:
- `images` — message has image attachments
- `descriptions` — message has descriptive text (>= 8 chars, or explicit buy/sell text regardless of length)
- `payment_fragments` — message is a payment-only shorthand like "zelle $50"
- `trade_fragments` — message has in/out trade keywords

A group "needs more context" until it has a useful combination of these (e.g., description + payment, or image + description + payment).

### Back-to-back deals from the same author

When an author posts multiple deals in quick succession (e.g., image+text, image+text within 12 seconds), the system must pair each image with its NEAREST text message. This relies on:
1. `candidate_improves_group` sorting candidates by absolute time distance
2. `has_descriptive_text` recognizing short explicit buy/sell text (like "Buy 450", 7 chars) so it doesn't get skipped in favor of a farther-away longer message

### Anti-stitch guards

- 2+ complete deals in the same group → reject stitch
- 2+ image posts (unless other rows are all short fragments) → reject stitch
- 2+ payment fragments → reject stitch
- Large gap between messages → reject stitch (unless force-stitch applies)

## Non-Transaction Detection

The parser auto-ignores messages that are clearly not deals:

### Currently detected noise patterns
- Blank / empty messages
- Date markers (e.g., "March 15", "Monday")
- Internal cash transfers / partner loans
- Non-transaction keywords ("profit overview", "summary", "spreadsheet", etc.)
- **Emoji-only messages** (no alphanumeric content)
- **"Wrong chat/channel/image" messages**
- **Short conversational filler** (< 25 chars, no digits, no images) — "Noice", "Lmao", "Perfect!!", etc.

### Preserved (NOT auto-ignored)
- Payment method words alone ("cash", "zelle") — these may be stitch fragments
- Anything with digits — could be a price or amount
- Anything with images — could be a deal photo
- Anything matching `has_transaction_signal` — buy/sell/trade/payment patterns

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
- image-first then text (up to 45s gap for explicit buy/sell)
- payment-only sell default
- explicit buy/sell text overriding trade-like image guesses
- grouped child rows no longer producing duplicate transactions
- back-to-back deals from same author pair correctly (nearest image to nearest text)
- short explicit text like "Buy 450" (< 8 chars) still counts as descriptive for stitching
- multiplier notation like "2x" is not extracted as a dollar amount
- emoji-only and "wrong chat" messages are auto-ignored, not stuck in review_required

When touching bookkeeping:
- upload `.xlsx`
- upload `.csv`
- public Google Sheets auto-import path
- reconciliation page loads

## TikTok Shop Integration

**Full API reference: see `TIKTOK_API.md` in project root.**

That file documents every endpoint, auth flow, response shape, and known gotcha.
Read it before touching any TikTok code.

### Current state (as of 2026-04-08)

Everything is working:
- OAuth token exchange + auto-refresh (background loop every 30 min)
- Order sync (backfill + periodic poll + webhook enrichment)
- Live streamer dashboard with real-time orders, GMV, chat, refund alerts
- Live analytics (stream session list, per-stream GMV, per-minute charts)
- Analytics page with daily trends and growth metrics
- Product sync and management

### Key files

| File | Role |
|---|---|
| `scripts/tiktok_backfill.py` | API signing, order/product fetching, all analytics API calls |
| `app/tiktok_ingest.py` | OAuth token exchange, webhook parsing, order normalization |
| `app/tiktok_auth_refresh.py` | Background token refresh logic |
| `app/tiktok_live_chat.py` | TikSync WebSocket for live chat + room ID capture |
| `app/models.py` | `TikTokOrder`, `TikTokAuth`, `TikTokProduct`, `AppSetting` models |
| `app/reporting.py` | TikTok order reporting/summary functions, buyer insights, product performance |
| `app/main.py` | All TikTok routes, streamer dashboard, analytics, background pollers |
| `app/templates/tiktok_streamer.html` | Streamer dashboard (orders, GMV, chat, goal bar, sparkline, alerts, summary) |
| `app/templates/tiktok_analytics.html` | Analytics page (charts, stream selector, buyers, products, comparison) |
| `app/templates/tiktok_orders.html` | Order listing with livestream filter |
| `TIKTOK_API.md` | Complete API reference for all endpoints |

### Two token types

1. **Shop token** (Seller, `user_type=0`) — used for orders, products, shop analytics. Auth at `auth.tiktok-shops.com`.
2. **Creator token** (`user_type=1`) — used for real-time `live_core_stats`. Auth at `open.tiktokapis.com`. Separate OAuth flow.

Both are stored in the `TikTokAuth` DB table and auto-refreshed.

### Critical rules

- **DO NOT use `open.tiktokapis.com` for Shop auth** — Shop uses `auth.tiktok-shops.com`
- **DO NOT mix token types** — Shop token cannot call Creator endpoints and vice versa
- **DO NOT use V1 API paths** — they return 410
- **DO NOT use `today=true`** on overview_performance — causes 66007001 Rpc error
- **GMV = `subtotal_price`** (product value only), not `total_price` (includes tax + shipping)
- **Webhook payloads are incomplete** — always fetch full details from API after receiving
- See `TIKTOK_API.md` for the complete list of gotchas

### TikTok Webhook Signature — DO NOT MODIFY

**The webhook signature verification algorithm in `app/tiktok_ingest.py` is correct and must not be changed.** It was reverse-engineered from live production traffic and confirmed working. TikTok's own documentation is wrong/incomplete about this.

**The proven algorithm:** `HMAC-SHA256(app_secret, app_key + raw_body)`

- **Key** = `app_secret` (from env `TIKTOK_APP_SECRET`)
- **Message** = `app_key` (from env `TIKTOK_APP_KEY`) concatenated with the raw HTTP request body (bytes)
- **Digest** = SHA-256 hex
- **Signature arrives** in the `Authorization` or `X-TT-Signature` header (NOT the `Tiktok-Signature: t=...,s=...` header described in generic TikTok docs)

**Files involved (DO NOT refactor the signing logic in these):**
- `app/tiktok_ingest.py` — `_build_webhook_signature_candidates()`, `verify_tiktok_webhook_signature()`, `parse_tiktok_webhook_headers()`
- `app/main.py` — `tiktok_orders_webhook()` handler at `POST /webhooks/tiktok/orders`

**What NOT to do:**
- Do NOT change the HMAC algorithm or the order of `app_key + raw_body`
- Do NOT remove fallback candidates (they exist for robustness)
- Do NOT change which headers are checked for the signature
- Do NOT switch `strict_signature` to `False` — it is `True` in production and working
- Do NOT follow TikTok's generic webhook docs blindly — their Shop V2 webhooks use a different algorithm than documented
- Do NOT add new signing candidates unless you have captured a real failing webhook and proven the new algorithm matches

**History:** This was cracked on 2026-04-07 after extensive debugging. TikTok's official docs describe `HMAC-SHA256(app_secret, timestamp + raw_body)` but that does NOT match what their servers actually send. The correct algorithm was found by capturing live webhook payloads on Machine B and testing every plausible combination until `HMAC-SHA256(app_secret, app_key + raw_body)` matched. This was independently confirmed against an open-source PHP SDK.

## Streamer Dashboard Features

The streamer dashboard (`/tiktok/streamer`) includes:

- **GMV Goal Bar** — progress bar showing stream GMV vs a configurable goal. Click-to-edit inline. Pulsing glow at 100%+. Stored as `AppSetting` key `stream_gmv_goal`.
- **High-Value Order Alerts** — gold toast with "Big Order!" label and distinct chime when order exceeds threshold. `AppSetting` key `high_value_threshold` (default $100).
- **VIP Buyer Alerts** — purple toast with "VIP Buyer!" label and lifetime spend badge when buyer's all-time spend exceeds threshold. `AppSetting` key `vip_buyer_threshold` (default $5,000). Each poll response includes `buyer_lifetime_spent` per order card.
- **Order Velocity Sparkline** — SVG sparkline in the GMV hero showing per-minute order counts for the last 60 minutes, with a "X orders/min" rate label.
- **Post-Stream Summary Card** — full-screen overlay triggered when `is_live` transitions from true to false. Shows GMV, orders, items, AOV, customers, orders/hr, top sellers/buyers. "Copy Summary" for pasting into Discord.

### AppSetting keys used by the dashboard

| Key | Default | Purpose |
|---|---|---|
| `stream_gmv_goal` | `"0"` (disabled) | Dollar target for the GMV goal bar |
| `high_value_threshold` | `"100"` | Minimum order $ to trigger gold alert toast |
| `vip_buyer_threshold` | `"5000"` | Minimum lifetime buyer $ to trigger VIP toast |
| `stream_start_utc` | | Persisted stream range start |
| `stream_end_utc` | | Persisted stream range end |
| `stream_range_source` | | `"auto"` or `"manual"` |

All three thresholds are editable from `/tiktok/streamer/config` and via `POST /tiktok/streamer/goal`.

## Analytics Page Features

The analytics page (`/tiktok/analytics`) includes:

- **Daily GMV Trend** — Chart.js line chart with 7d/30d/60d/90d picker
- **Live Streams Table** — session list with GMV, duration, revenue/hr, % change vs previous
- **Stream Detail Panel** — per-minute GMV chart, top sellers/buyers
- **Repeat Buyer Tracking** — sortable table of all buyers with total spent, order count, streams, avg order, first/last seen. Repeat buyers get a cyan badge. API: `GET /tiktok/analytics/api/buyers?days=90`
- **Product Performance Ranking** — sortable table of products with revenue, qty, orders, avg price, and live% / non-live% split. API: `GET /tiktok/analytics/api/products?days=30`
- **Stream-over-Stream Comparison** — side-by-side cards with delta pills (green/red % change). Supports "Pick Streams" and "Week vs Week" modes. API: `GET /tiktok/analytics/api/compare?stream_a=X&stream_b=Y` or `?mode=weekly`

## Infrastructure: Machine A / Machine B

- **Machine A** (desktop-va88cfb) — local dev, runs SQLite, Tailscale IP `100.122.56.32`
- **Machine B** (desktop-ppf7vk9) — production server, runs PostgreSQL + Discord bot + worker, Tailscale IP `100.110.34.106`

Both machines are connected via **Tailscale** mesh VPN. From Machine A you can:
- Query production Postgres: `postgresql+psycopg://degen:degen42069@100.110.34.106:5432/degen_live`
- SSH: `ssh Degen@100.110.34.106`

PostgreSQL on Machine B is at `C:\Program Files\PostgreSQL\17\data\` and allows connections from the Tailscale subnet (`100.64.0.0/10` in `pg_hba.conf`).

## Database: Dual-Engine Support (CRITICAL)

Production (Machine B) runs **PostgreSQL**. Local dev (Machine A) runs **SQLite**. The app auto-detects via `DATABASE_URL`.

### Schema migrations are additive, NOT reset-based

New tables are created automatically by `SQLModel.metadata.create_all()`. But **new columns on existing tables** require explicit additive migrations in `app/db.py`:

- `SQLITE_ADDITIVE_MIGRATIONS` — runs in `ensure_sqlite_schema()` using `ALTER TABLE ... ADD COLUMN`
- `POSTGRES_ADDITIVE_MIGRATIONS` — runs in `ensure_postgres_schema()` using `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

**You MUST add new columns to BOTH dicts.** If you only add to one, the other environment will crash with `UndefinedColumn` or `no such column` errors at runtime.

SQLite uses `REAL` and `BOOLEAN DEFAULT 0`. PostgreSQL uses `DOUBLE PRECISION` and `BOOLEAN DEFAULT FALSE`. Check existing entries for the correct type mapping.

### Checklist when adding/changing a model field

1. Add the field to the SQLModel class in `app/models.py`
2. Add the column to `SQLITE_ADDITIVE_MIGRATIONS` in `app/db.py`
3. Add the column to `POSTGRES_ADDITIVE_MIGRATIONS` in `app/db.py` (with Postgres types)
4. If adding an index, add to both `SQLITE_INDEX_MIGRATIONS` and `POSTGRES_INDEX_MIGRATIONS`
5. Run `compileall` and test locally
6. After deploy, verify the column exists on production

### Type differences between engines

| Python/SQLModel | SQLite migration | PostgreSQL migration |
|---|---|---|
| `float` | `REAL` | `DOUBLE PRECISION` |
| `bool` (default False) | `BOOLEAN DEFAULT 0` | `BOOLEAN DEFAULT FALSE` |
| `bool` (default True) | `BOOLEAN DEFAULT 1` | `BOOLEAN DEFAULT TRUE` |
| `int` | `INTEGER` | `INTEGER` |
| `str` | `TEXT` | `TEXT` |
| `datetime` | `TIMESTAMP` | `TIMESTAMP` |

## Notes For Future Agents

- Database schema has evolved additively; avoid reset-based development if possible
- Production uses PostgreSQL; local dev uses SQLite. The app auto-detects via `DATABASE_URL`
- Both engines are fully supported — `app/db.py` handles connection config, pooling, and migrations for each
- do not break working buys/sells/trades while improving expense handling
- if a row looks wrong in the UI, always check whether the real issue is:
  - bad stitching
  - image-only AI guess
  - stale child grouped row data
  - transaction sync not being removed
- if debugging parser results, prefer fixing deterministic rules before making the AI prompt more complex

## Queue / Processing State Model (CRITICAL)

Each DiscordMessage MUST have a clear processing state.

Valid states:
- `pending` → waiting to be processed
- `processing` → currently being worked on
- `parsed` → successfully parsed
- `failed` → parsing or transaction failed
- `review_required` → needs human review
- `ignored` → intentionally skipped

Rules:
- no message should remain indefinitely in `processing`
- failures must move to `failed` with error reason
- parser changes should allow reprocessing of `parsed` rows
- worker must not silently skip rows without logging why

Definition of "stuck":
- message remains in `pending` or `processing` without progress
- message repeatedly fails without visibility

## Reparse / Replay Rules (CRITICAL)

The system MUST support reprocessing old messages.

Important distinction:
- "seen before" != "correct under latest parser logic"

Requirements:
- allow reprocessing of previously parsed messages
- reparsing must NOT create duplicate transactions
- parser output must be replaceable or refreshable
- reparsing should be possible:
  - by date range
  - by channel
  - by explicit selection

Preferred approach:
- raw DiscordMessage remains source of truth
- normalized Transaction layer is derived and replaceable

Never assume parsed data is final.

## Observability / Logging (CRITICAL)

Logging must make debugging possible without reading code.

Every processing step MUST log:

- message_id
- channel
- current state
- action being performed
- success/failure
- error message (if any)
- timestamp

Required log events:
- message queued
- message picked up by worker
- parsing started
- parsing success
- parsing failure
- transaction sync started
- transaction sync success
- transaction sync failure
- message marked for review

No silent skips:
- if a message is skipped, log WHY

System must support:
- viewing recent failures
- counting messages by state
- identifying stuck messages