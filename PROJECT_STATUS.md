# Project Status

Last updated: 2026-04-16

## What Is This

**Degen Collectibles** — a Discord deal parser + TikTok Shop livestream platform for a collectibles/trading card business.

Two main verticals:
1. **Discord deal parsing** — ingests Discord deal-log messages, parses them into structured transactions, normalizes for financial reporting
2. **TikTok Shop livestream tools** — order sync, real-time streamer dashboard, analytics, product management

Stack: Python 3.14, FastAPI, Uvicorn, PostgreSQL (prod) / SQLite (dev), discord.py, AI (OpenAI or NVIDIA Inference Hub — Claude Opus for vision, Haiku for lightweight tasks), Ximilar Collectibles API, multi-TCG card data APIs (TCGdex, PokemonTCG, Scryfall, YGOPRODeck, OPTCG, Lorcast, TCGTracking), TikTok Shop Open Platform API, TikSync SDK (live chat WebSocket), Jinja2 + vanilla JS + Chart.js, role-based auth.

---

## Deployment

### Machine A — Local Dev (Windows PC, travels with owner)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1`
- Run with production Postgres: `powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web_pg.ps1`
- Web-only mode: Discord ingest, parser worker, and backfill are **disabled**
- **SQLite** database at `data/degen_live.db` (local copy, not synced with production)
- Access at `http://127.0.0.1:8000`
- Tailscale IP: `100.122.56.32`

### Machine B — Production (Windows PC, runs 24/7)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1`
- Runs web server + worker process with auto-restart and liveness watchdog
- Discord bot, parser worker, TikTok order sync, webhook listener, live chat all run here
- **PostgreSQL** database (`postgresql+psycopg://degen:degen42069@localhost:5432/degen_live`)
- Exposed via **Cloudflare tunnel** at `ops.degencollectibles.com`
- Automated Postgres backup via `scripts/backup_pg.ps1` (pg_dump → OneDrive via rclone)
- Tailscale IP: `100.110.34.106`

Both machines share the same codebase via git. Machine B auto-deploys on push to `main` via GitHub Actions.

---

## Architecture (Post-Refactor)

As of April 2026, `app/main.py` was refactored from ~12,000 lines into a modular router architecture:

| Layer | Files | Description |
|---|---|---|
| **App init** | `app/main.py` (~720 lines) | FastAPI app creation, middleware, static files, router includes |
| **Shared helpers** | `app/shared.py` (~5,000 lines) | Background tasks, state management, polling loops, shared utilities |
| **Route handlers** | `app/routers/` (16 modules) + `app/inventory.py` | One file per feature area |
| **Core logic** | `app/*.py` (42+ files total) | Models, parser, worker, ingestion, reporting, inventory, etc. |
| **Templates** | `app/templates/` (30+ files) | Jinja2 HTML with inline JS/CSS |
| **Scripts** | `scripts/` (11 files) | Backfill, migration, scraping, backup utilities |
| **Tests** | `tests/` (25 files) | 233 passing tests |

### Router Modules

| Router | Path prefix | Purpose |
|---|---|---|
| `admin.py` | `/admin` | Admin pages, debug, health |
| `admin_actions.py` | `/admin` | Channel config, parser reruns, learned rules |
| `bookkeeping.py` | `/bookkeeping` | Sheet import, reconciliation |
| `channels_api.py` | `/channels`, `/messages` | Channel/message JSON APIs |
| `dashboard.py` | `/dashboard`, `/status`, `/ops-log` | Dashboard, status, ops log |
| `deals.py` | `/deals` | Deal detail, attachment routes |
| `hits.py` | `/hits` | Live hit tracker |
| `messages.py` | `/table`, `/review-table`, `/review` | Message tables, review workflow |
| `reports.py` | `/reports`, `/finance`, `/pnl` | Financial reports |
| `shopify.py` | `/shopify` | Shopify order sync, OAuth |
| `stream_manager.py` | `/stream-manager` | Stream team management |
| `tiktok_analytics.py` | `/tiktok/analytics`, `/tiktok/clients` | Analytics charts, client & product intelligence |
| `tiktok_orders.py` | `/tiktok/orders` | Order listing, webhook, sync |
| `tiktok_products.py` | `/tiktok/products` | Product management |
| `tiktok_streamer.py` | `/tiktok/streamer` | Live streamer dashboard |
| `inventory.py` (top-level) | `/inventory` | Inventory CRUD, scanning, labels, Shopify push |

### Key Backend Modules

| File | Purpose |
|---|---|
| `models.py` | SQLModel ORM classes (DiscordMessage, Transaction, TikTokOrder, User, etc.) |
| `db.py` | Database engine, session management, additive migrations (Postgres + SQLite) |
| `parser.py` | Rule-based deal parsing with OpenAI fallback |
| `worker.py` | Message queue processor, stitching logic |
| `tiktok_ingest.py` | OAuth token exchange, webhook parsing, order normalization |
| `tiktok_auth_refresh.py` | Background token refresh (every 30 min) |
| `tiktok_live_chat.py` | TikSync WebSocket for live chat + room ID capture |
| `reporting.py` | Financial summaries, TikTok order reporting |
| `transactions.py` | Normalized Transaction and TransactionItem sync |
| `discord_ingest.py` | Discord bot listener, raw message storage |
| `bookkeeping.py` | Bookkeeping sheet import + reconciliation |
| `config.py` | All env vars and app settings |
| `auth.py` | Role-based auth (admin, reviewer, viewer) |
| `inventory.py` | Inventory routes — listing, CRUD, scanning, labels |
| `inventory_barcode.py` | Barcode generation (DGN-XXXXXX format) |
| `inventory_pricing.py` | Auto-pricing lookups (Scryfall, 130point) |
| `inventory_shopify.py` | Shopify product sync, mark-sold-from-order |
| `card_scanner.py` | Legacy AI-powered card identification from camera images |
| `pokemon_scanner.py` | Multi-TCG card scanner: Ximilar + OCR pipeline, text search, per-TCG lookup (Pokemon/Magic/Yu-Gi-Oh/One Piece/Lorcana/+), TCGTracking price enrichment |
| `ai_client.py` | AI provider factory (OpenAI / NVIDIA Inference Hub); `get_model()` heavy, `get_fast_model()` lightweight |
| `cert_lookup.py` | Grading cert number lookup (PSA, BGS, CGC, SGC) |

### Key Scripts

| Script | Purpose |
|---|---|
| `scripts/tiktok_backfill.py` (~1,700 lines) | TikTok API signing, order/product fetching, analytics API calls |
| `scripts/backup_pg.ps1` | Automated Postgres backup → OneDrive via rclone |
| `scripts/run_hosted.ps1` | Production launch with auto-restart + liveness watchdog |
| `scripts/run_local_web.ps1` | Local dev launch (SQLite, web-only) |
| `scripts/run_local_web_pg.ps1` | Local dev launch connected to production Postgres via Tailscale |
| `scripts/shopify_backfill.py` | Shopify historical order import |
| `scripts/migrate_sqlite_to_postgres.py` | One-time migration tool |
| `scripts/scrape_tiktok_docs.py` | TikTok API docs scraper (685 pages → markdown/PDF) |
| `scripts/tiktok_giveaway_login.py` | browser-use agent for TikTok giveaway automation (WIP) |

---

## What's Working

### Discord Side
- Discord message ingestion from watched channels
- Rule-based parsing with OpenAI fallback for ambiguous cases
- Message stitching (grouping nearby related messages into one deal)
- Transaction normalization and financial reporting
- Review/approval workflow (`/table`, `/review-table`)
- Bookkeeping import (CSV, XLSX, Google Sheets auto-import) and reconciliation
- Ops log with filtering and pagination

### TikTok Shop Side
- OAuth flow for Shop tokens (auto-refresh every 30 min)
- Separate Creator OAuth flow (for real-time live analytics)
- Order sync: startup backfill + periodic polling + webhook enrichment
- **Streamer dashboard** (`/tiktok/streamer`):
  - Real-time order feed with toast notifications and sound alerts
  - Today's GMV + Stream GMV (manual or auto-detected range)
  - TikTok official GMV from API
  - Top Sellers and Top Buyers with Today/Stream toggle
  - Live chat panel (via TikSync WebSocket), collapsible
  - Refund alerts highlighted in red
  - Stream dividers between orders from different livestreams
  - LIVE/OFFLINE badge + "Now Streaming: [name]" display
  - "Log a Hit" feature for streamers
  - Copy-to-clipboard for customer labels
  - **GMV Goal Bar** — progress bar with click-to-edit target
  - **High-Value Order Alerts** — gold toast + distinct chime (default $100+)
  - **VIP Buyer Alerts** — purple toast with lifetime spend badge (default $5,000+)
  - **Order Velocity Sparkline** — SVG per-minute order rate
  - **Post-Stream Summary Card** — auto-triggered overlay with full stream stats + copy-to-clipboard
  - **Leaderboard Drilldowns** — click a top buyer to see their orders; click a top seller to see buyer breakdown. Duplicate line items merged with quantity display.
  - **Stream Dividers** — visual separators between orders from different livestreams
  - **Dynamic LIVE/OFFLINE Badge** — pulsing red when streaming, gray when offline
  - **Collapsible Live Chat** — side panel (desktop) or bottom panel (mobile), persists open/closed state
- **Client Intelligence** (`/tiktok/clients`):
  - Buyer list with total spent, order count, streams, avg order, first/last seen, repeat badges
  - Click-to-expand buyer drilldown with order details and product images
  - Product list with revenue, qty, order count, avg price
  - Click-to-expand product drilldown showing all buyers
  - Mobile-friendly with responsive columns
- **Analytics page** (`/tiktok/analytics`):
  - Daily GMV trend chart (7d/30d/60d/90d)
  - Stream session list with GMV, duration, revenue/hour, % change
  - Per-minute GMV chart for individual streams
  - Top sellers/buyers per stream (scrollable, default 20 shown)
  - **Repeat Buyer Tracking** — sortable buyer table with repeat badges, lifetime stats
  - **Product Performance Ranking** — revenue, qty, live/non-live split
  - **Stream-over-Stream Comparison** — side-by-side delta cards, "Pick Streams" or "Week vs Week" modes
- **Orders page** (`/tiktok/orders`):
  - Full order listing with date filters and livestream filter
- Product sync and management
- TikTok webhook with HMAC-SHA256 signature verification

### Inventory Side
- Inventory item management (singles and slabs) with `InventoryItem` model
- Camera-based card identification via AI (`/inventory/scan`)
- Slab cert number lookup (PSA, BGS, CGC, SGC)
- Batch card scanning with review step
- Auto-pricing from Scryfall, 130point, and other sources
- DGN-XXXXXX barcode generation with Avery-compatible label printing
- Shopify integration — push items to Shopify, auto-mark sold from incoming orders
- Item status tracking (in_stock, listed, sold, returned, missing)

### Degen Eye Multi-TCG Scanner (`/degen_eye`)
- **Confidence-tiered image pipeline** — Ximilar Collectibles API first; if confidence >= 0.85, return immediately; otherwise merge with legacy OCR + AI vision disambiguation
- **Auto-detects card game** from Ximilar tags (Pokemon, Magic, Yu-Gi-Oh, One Piece, Lorcana, Dragon Ball, etc.) for correct pricing category
- **Manual card addition via text search** — `POST /degen_eye/text-search` parses free-text queries (AI + heuristic fallback) and returns ranked candidates with images and prices
- **Multi-TCG text search routing** — queries route to dedicated card-name APIs by TCGTracking category:
  - Pokemon → TCGdex + PokemonTCG waterfall (with set-filtered supplementary lookup and image backfill)
  - Magic → Scryfall
  - Yu-Gi-Oh → YGOPRODeck
  - One Piece → OPTCG API
  - Lorcana → Lorcast
  - Other TCGs (Dragon Ball, etc.) → TCGTracking product search fallback
- **Edit scanned cards inline** — tap a scanned card to edit fields or trigger "Search for Different Card" to swap the match
- **TCGTracking price enrichment across all TCGs** — fetches TCGPlayer market + low prices, variant list (Normal, Holofoil, Reverse, 1st Edition, etc.), and condition-specific prices (NM/LP/MP/HP/DMG) via `/skus` endpoint
- **Variant + condition selectors** — picked variant/condition updates the displayed price immediately from pre-fetched `conditions_pricing`
- **Rapid-fire camera scan queue** — client-side batching with `localStorage`, mobile-first UI
- **Configurable AI provider** — defaults to OpenAI (`gpt-5-nano`); can switch to NVIDIA Inference Hub (Claude Opus for vision, Haiku for fast query parsing) via `AI_PROVIDER` env var

### Stream Management
- Multi-stream account support — tabbed schedule interface (e.g., "Main Stream", "Second Stream")
- Team scheduling with AM/PM time display
- Overnight shift support (shifts crossing midnight tagged as "next day")
- Auto-detection of current streamer based on schedule + stream account
- Hit tracker defaults to scheduled streamer for main account

### Infrastructure
- Role-based auth (admin, reviewer, viewer)
- User management UI (`/admin/users`)
- PostgreSQL with connection pooling (QueuePool, pool_size=5, max_overflow=10), TCP keepalives, pool_pre_ping, 30s statement timeout
- SQLite with WAL mode, busy_timeout, retry logic
- Liveness watchdog in production (auto-restarts unresponsive processes)
- Automated Postgres backups via rclone → OneDrive
- 233 passing tests across 25 test files
- CI workflow (GitHub Actions) with auto-deploy to Machine B

---

## Known Issues / Technical Debt

### Important
- **`app/templates/tiktok_streamer.html` is ~2,700 lines** with all CSS and JS inline. Should be split into separate assets.
- **`scripts/tiktok_backfill.py` is ~1,700 lines** and could be split into modules.
- **TikTok analytics data is delayed ~2 days** for the `overview_performance` endpoint.
- **Webhook payloads arrive incomplete** ($0.00, "Guest", no items). The app fetches full details async, but there's a brief window where incomplete data shows.
- **Creator token** for `live_core_stats` requires manual OAuth and a `live_room_id` from TikSync WebSocket. Fragile.
- **Buyer @username not available** — TikTok Shop API only returns `buyer_nickname` (display name) and `recipient_address.name` (shipping name), not the TikTok @handle. See "Planned Features" below.

### Nice to Have
- No automated end-to-end tests for the streamer dashboard or inventory scanning
- Chat panel connection depends on TikSync third-party service availability
- Inventory auto-pricing sources could be expanded (currently Scryfall + 130point)
- `app/templates/tiktok_streamer.html` is ~2,700 lines — CSS/JS should be extracted to separate assets

---

## Planned Features / Under Discussion

### Buyer @Username Resolution
TikTok Shop API does not expose buyer TikTok usernames. Only `buyer_nickname` (display name like "Ross") and `recipient_address.name` (shipping name like "Ross Edward") are available. The @handle (e.g., "cashycash77") that TikTok shows in Seller Center is internal data not exposed via the API.

Options explored:
1. **Official TikTok Display API** (`/v2/user/info/`) — returns `username` but only for users who OAuth into your app. Can't look up arbitrary buyers.
2. **Third-party scraper APIs** — [TikLiveAPI](https://www.tikliveapi.com/documentation/users/info-by-id) accepts `user_id` → returns `uniqueId` (@handle). $9.90/100K requests. We have `user_id` from every order.
3. **browser-use agent** — use AI browser automation to scrape buyer username from TikTok Seller Center order detail page. Free, already have browser-use installed. Slower but no API cost.

### Streamer Dashboard Ideas (Backlog)
- **Product queue / run sheet** — pre-plan which products to showcase, advance through list
- **Countdown timer for flash deals** — streamer sets a timer, creates urgency
- **Comment-to-sale conversion tracker** — correlate chat spikes with order spikes
- **Live audience sentiment** — positive/negative gauge from chat keywords
- **"Who's watching" panel** — known buyers in chat with lifetime spend
- **First-time buyer alerts** — distinct notification for first-ever purchase
- **Inventory alerts** — warning when showcased product drops below threshold
- **Stream health monitor** — viewer count trend, connection quality
- **Quick sound effects board** — configurable clips for sales/milestones
- **Pinned notes / talking points** — streamer-only notepad
- **Milestone celebrations** — visual effects at 50th order, $10K GMV, etc.
- **Leaderboard race** — real-time "top spender this stream"
- **Streak tracker** — "12 orders in the last 5 minutes!" momentum counter
- **Clip bookmarking** — timestamp markers during stream for highlights

### TikTok Giveaway Automation (WIP)
Using `browser-use` AI agent to automate TikTok's native live giveaway feature through Seller Center. Login script exists at `scripts/tiktok_giveaway_login.py`.

### Multi-Tenancy Prep
Future goal: make the platform multi-tenant SaaS. No work started yet.

---

## Recent Commits (Last 20)

```
8b6bbd2 add multi-TCG text search: Scryfall (Magic), YGOPRODeck (Yu-Gi-Oh), OPTCG (One Piece), Lorcast (Lorcana), TCGTracking fallback
439671c auto-detect card game category from Ximilar tags for correct pricing
2f1e7c3 fix pricing for non-Pokemon games: flexible number matching + name-only fallback
6af460c fix: always fetch condition pricing from TCGTracking even if base price exists
9d06f37 fix search sheet: update price on variant/condition change
6d0278c fix text search: remove response_format, add heuristic set extraction, fix TCGdex sort key
a2e1020 improve text search: set-filtered PokemonTCG lookup, image backfill, TCGdex set prioritization
b3bfb75 fix fast model name to claude-haiku-4-5-v1, fall through to heuristic on empty AI result
6c9d648 add NVIDIA_FAST_MODEL for lightweight AI tasks (defaults to Haiku)
4d3378d fix text search crash: wrong settings attribute name
782dc9b Merge duplicate line items per order in buyer detail drilldown
8a276b5 Fix leaderboard UX: add buyer scroll arrows, mobile-friendly detail panels with product images
23e0e44 Add click-to-expand drilldowns on streamer leaderboard
bb30f37 Make client intelligence page mobile-friendly with horizontal scroll and responsive columns
1ca21fa Fix .00 revenue in product table - summary JSON uses unit_price not sale_price
ecf4566 Fix naive vs aware datetime comparisons in client intelligence functions
f14eb11 Pin Jinja2 to 3.1.5 to fix unhashable cache key bug in 3.1.6
ec9229f Fix three data accuracy bugs found by Codex audit
755b506 Add Client & Product Intelligence page at /tiktok/clients
370c316 Simplify Log Hit modal: remove hit value, stream label, and extra fields
```

---

## Key Documentation

| File | What It Covers |
|---|---|
| `AGENTS.md` | Project rules, architecture, coding conventions for AI tools |
| `TIKTOK_API.md` | Every TikTok endpoint, auth flow, response shape, known gotchas |
| `TIKTOK_WEBHOOK_SIGNATURE.md` | Webhook HMAC-SHA256 algorithm (DO NOT MODIFY) |
| `PROJECT_STATUS.md` | This file — current state, deployment, known issues |
| `README.md` | Setup instructions, env vars |

## Env Vars Quick Reference

Essential for TikTok features:
```
TIKTOK_APP_KEY, TIKTOK_APP_SECRET, TIKTOK_REDIRECT_URI
TIKTOK_SHOP_CIPHER (needed for analytics APIs)
TIKTOK_LIVE_API_KEY, TIKTOK_LIVE_USERNAME (for live chat)
```

Tokens are stored in the DB after initial OAuth — not needed in .env after first auth.

Essential for Degen Eye card scanner:
```
XIMILAR_API_TOKEN      # visual card recognition (Ximilar Collectibles)
POKEMON_TCG_API_KEY    # PokemonTCG API (higher rate limits)
```
TCGTracking's public API is unauthenticated — variant/condition pricing works without a key.

AI provider configuration (both parser and scanner use this):
```
AI_PROVIDER=openai     # or "nvidia"
# If using NVIDIA Inference Hub:
NVIDIA_API_KEY
NVIDIA_BASE_URL=https://integrate.api.nvidia.com/v1
NVIDIA_MODEL=aws/anthropic/bedrock-claude-opus-4-6     # heavy (vision identification)
NVIDIA_FAST_MODEL=aws/anthropic/claude-haiku-4-5-v1    # fast (query parsing)
NVIDIA_TIEBREAKER_MODEL=gcp/google/gemini-3.1-pro-preview  # ensemble tiebreaker (only fires on Ximilar+vision disagreement)
```

See `app/config.py` for the complete list of all settings.
