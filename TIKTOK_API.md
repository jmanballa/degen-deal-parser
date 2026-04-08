# TikTok Shop API Reference

This document captures every TikTok Shop API endpoint, authentication flow, known quirk, and hard-won lesson used in this project. It exists so that any AI coding tool or developer can pick up the integration without re-discovering these details from scratch.

---

## Table of Contents

1. [Authentication](#authentication)
2. [API Request Signing](#api-request-signing)
3. [Orders API](#orders-api)
4. [Product API](#product-api)
5. [Live Analytics API](#live-analytics-api)
6. [Webhooks](#webhooks)
7. [Live Chat (TikSync)](#live-chat-tiksync)
8. [Environment Variables](#environment-variables)
9. [Known Quirks and Gotchas](#known-quirks-and-gotchas)

---

## Authentication

There are **two separate OAuth flows** used in this project, for two different token types.

### 1. Shop Token (Seller)

Used for: orders, products, shop-level analytics, webhooks.

| Detail | Value |
|---|---|
| Auth base URL | `https://auth.tiktok-shops.com` |
| Token get path | `/api/v2/token/get` |
| Token refresh path | `/api/v2/token/refresh` |
| HTTP method | `GET` with query params |
| Grant type (initial) | `authorized_code` |
| Grant type (refresh) | `refresh_token` |

**Token exchange query params:**

```
app_key=<TIKTOK_APP_KEY>
app_secret=<TIKTOK_APP_SECRET>
auth_code=<code from callback>
grant_type=authorized_code
```

**Token refresh query params:**

```
app_key=<TIKTOK_APP_KEY>
app_secret=<TIKTOK_APP_SECRET>
refresh_token=<stored refresh token>
grant_type=refresh_token
```

**Response shape (success):**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "access_token": "TTP_...",
    "access_token_expire_in": 86400,
    "refresh_token": "TTP_...",
    "refresh_token_expire_in": 15552000,
    "open_id": "...",
    "seller_name": "...",
    "seller_base_region": "US",
    "user_type": 0
  }
}
```

**Important:**
- The auth code expires within seconds of being issued. The callback handler must exchange it immediately.
- `user_type=0` means Seller token. This is the primary token for all Shop API calls.
- Access tokens last ~24 hours. Refresh tokens last ~180 days.
- Once exchanged, the token is stored in the `TikTokAuth` database table. The app uses DB-stored tokens at runtime, not `.env` values (those are fallbacks only).
- After initial OAuth, the app auto-refreshes tokens in a background loop (`tiktok_token_refresh_interval_minutes`, default 30 min).
- You do NOT need to re-authorize after restarting the app — tokens persist in the database.

### 2. Creator Token

Used for: real-time live stream analytics (`live_core_stats`).

| Detail | Value |
|---|---|
| Auth base URL | `https://open.tiktokapis.com` |
| Token get path | `/v2/oauth/token/` |
| Token refresh path | `/v2/oauth/token/` |
| HTTP method | `POST` with form body |
| Grant type (initial) | `authorization_code` |
| Grant type (refresh) | `refresh_token` |
| Required scope | `data.analytics.public.read` |

**Critical difference from Shop token:**
- Creator auth uses `user_type=1` in the TikTok authorization URL.
- Uses a **different base URL** (`open.tiktokapis.com` vs `auth.tiktok-shops.com`).
- Uses **POST with form body** (not GET with query params).
- Requires a separate callback route (`/integrations/tiktok/creator-callback`).
- Stored in separate DB columns: `creator_access_token`, `creator_refresh_token`, `creator_token_expires_at`.

**When you need this:** Only if you want real-time GMV via `live_core_stats` (requires a `live_room_id` captured from the TikSync WebSocket). The Shop token **cannot** call Creator-scoped endpoints.

---

## API Request Signing

All TikTok Shop v2 API calls (orders, products, analytics) require HMAC-SHA256 request signing.

**Signature algorithm:**

1. Collect all query params (except `sign` and `access_token`).
2. Sort them alphabetically by key.
3. Concatenate as: `{app_secret}{path}{key1}{value1}{key2}{value2}...{body_json}{app_secret}`
   - `path` = the API path (e.g., `/order/202309/orders/search`)
   - `body_json` = JSON-serialized request body for POST, empty string for GET
4. HMAC-SHA256 the concatenated string with `app_secret` as the key.
5. Hex-encode the digest (lowercase).
6. Add `sign=<hex_digest>` and `access_token=<token>` to query params.

**Headers required:**
- `x-tts-access-token: <access_token>`
- `Content-Type: application/json` (for most endpoints)

**Implementation:** See `build_tiktok_request()` in `scripts/tiktok_backfill.py`.

---

## Orders API

Base URL: `https://open-api.tiktokglobalshop.com`

### Search Orders

| Detail | Value |
|---|---|
| Path | `/order/202309/orders/search` |
| Method | `POST` |
| Pagination | `page_size` (max 50), `page_token` |
| Date filter body | `create_time_ge`, `create_time_lt` (epoch seconds) |

**Request body example:**

```json
{
  "create_time_ge": 1711929600,
  "create_time_lt": 1712016000
}
```

**Response shape:**

```json
{
  "code": 0,
  "data": {
    "orders": [...],
    "next_page_token": "...",
    "total_count": 123
  }
}
```

### Get Order Details

| Detail | Value |
|---|---|
| Path | `/order/202309/orders` |
| Method | `GET` |
| Query param | `ids=<comma-separated order IDs>` |

Returns full order objects with line items, pricing, shipping, and status.

### Key Order Fields

| Field | Description |
|---|---|
| `id` | TikTok order ID (string of digits) |
| `status` | e.g., `AWAITING_SHIPMENT`, `COMPLETED`, `CANCELLED` |
| `payment.total_amount` | Total including tax + shipping |
| `payment.sub_total` | Product subtotal only (this is what TikTok calls "GMV") |
| `buyer_message` | Customer note |
| `create_time` | Epoch seconds |
| `update_time` | Epoch seconds |
| `line_items[].product_name` | Item title |
| `line_items[].sku_name` | Variant name |
| `line_items[].sku_image.url` | Product image URL |
| `line_items[].quantity` | Quantity ordered |

**GMV note:** TikTok's seller console reports GMV as `subtotal_price` (product value only, excluding tax and shipping). If your numbers don't match, check whether you're using `total_price` vs `subtotal_price`.

---

## Product API

Base URL: `https://open-api.tiktokglobalshop.com`

### Search Products

| Detail | Value |
|---|---|
| Path | `/product/202309/products/search` |
| Method | `POST` |
| Pagination | `page_size`, `page_token` |

### Get Product Details

| Detail | Value |
|---|---|
| Path | `/product/202309/products` |
| Method | `GET` |
| Query param | `ids=<comma-separated product IDs>` |

### Other Product Endpoints

| Path | Method | Purpose |
|---|---|---|
| `/product/202309/products` | `POST` | Create product |
| `/product/202309/products` | `PUT` | Edit product |
| `/product/202309/images/upload` | `POST` | Upload image |
| `/product/202309/categories` | `GET` | List categories |
| `/product/202309/brands` | `GET` | List brands |

---

## Live Analytics API

These endpoints provide livestream performance data. They are the trickiest part of the integration.

### 1. Overview Performance (Shop token)

| Detail | Value |
|---|---|
| Path | `/analytics/202509/shop_lives/overview_performance` |
| Method | `GET` |
| Auth | Shop access token |
| Required scope | `data.shop_analytics.public.read` |

**Query params:**

| Param | Required | Example | Notes |
|---|---|---|---|
| `start_date_ge` | Yes | `2026-03-25` | YYYY-MM-DD format |
| `end_date_lt` | Yes | `2026-04-03` | Exclusive upper bound |
| `granularity` | Yes | `1D` | Only `1D` is documented |
| `currency` | No | `USD` | Defaults to shop currency |

**Response shape:**

```json
{
  "code": 0,
  "data": {
    "latest_available_date": "2026-03-31",
    "performance": {
      "intervals": [
        {
          "start_date": "2026-03-25",
          "gmv": { "amount": "12345.67", "currency": "USD" },
          "items_sold": 150,
          "sku_orders": 120,
          "customers": 95,
          "click_to_order_rate": "0.05",
          "click_through_rate": "0.12"
        }
      ]
    }
  }
}
```

**Known issues:**
- Data is delayed by **~2 days**. `latest_available_date` tells you the most recent date with data.
- `today=true` param caused `66007001 Rpc error` — do not use it.
- Error code `66007001` = "Rpc error. Please try again." — this is a transient TikTok-side error. Catch it and retry or return gracefully.
- Returns 403 if the app doesn't have the `data.shop_analytics.public.read` scope activated.

### 2. Performance List — Stream Sessions (Shop token)

| Detail | Value |
|---|---|
| Path | `/analytics/202509/shop_lives/performance` |
| Method | `GET` |
| Auth | Shop access token |
| Required scope | `data.shop_analytics.public.read` |

**Query params:**

| Param | Required | Example |
|---|---|---|
| `start_date_ge` | Yes | `2026-03-25` |
| `end_date_lt` | Yes | `2026-04-03` |
| `sort_field` | No | `gmv` |
| `sort_order` | No | `DESC` |
| `currency` | No | `USD` |
| `account_type` | No | `OFFICIAL_ACCOUNTS` |
| `page_size` | No | `20` |

**Response shape:**

```json
{
  "code": 0,
  "data": {
    "live_stream_sessions": [
      {
        "id": "7342...",
        "title": "LIVE: Opening packs!",
        "username": "degencollectibles",
        "start_time": 1711929600,
        "end_time": 1711972800,
        "sales_performance": {
          "gmv": { "amount": "48200.50", "currency": "USD" },
          "items_sold": 1250,
          "sku_orders": 980,
          "customers": 450
        }
      }
    ]
  }
}
```

**This is the most useful endpoint for:**
- Listing all recent streams
- Getting per-stream GMV, items sold, customers
- Determining stream start/end times for auto-detecting the active stream
- A stream is considered "currently live" if its `end_time` is within the last ~15 minutes (the API updates `end_time` as the stream progresses)

**Known issues:**
- May return 403 "no schema found" if the scope is not properly activated in TikTok Partner Center. Check your app's approved scopes.

### 3. Per-Minute Performance (Shop token)

| Detail | Value |
|---|---|
| Path | `/analytics/202510/shop_lives/{live_id}/performance_per_minutes` |
| Method | `GET` |
| Auth | Shop access token |

**Query params:**

| Param | Required | Example |
|---|---|---|
| `currency` | No | `USD` |

**Response shape:**

```json
{
  "code": 0,
  "data": {
    "overall": {
      "start_time": 1711929600,
      "end_time": 1711972800,
      "duration": 43200,
      "gmv": { "amount": "48200.50", "currency": "USD" }
    },
    "intervals": [
      {
        "start_time": 1711929600,
        "end_time": 1711929660,
        "gmv": { "amount": "150.00", "currency": "USD" },
        "items_sold": 3,
        "sku_orders": 2,
        "customers": 2
      }
    ]
  }
}
```

**Only works after a stream has ended.** Not usable for real-time data during a live session.

### 4. Live Core Stats (Creator token)

| Detail | Value |
|---|---|
| Path | `/analytics/202502/live_rooms/{live_room_id}/core_stats` |
| Method | `GET` |
| Auth | **Creator** access token (not Shop token) |
| Required scope | `data.analytics.public.read` |

**This is the only real-time GMV endpoint**, but it requires:
1. A **Creator OAuth token** (separate auth flow, `user_type=1`)
2. A **`live_room_id`** which must be captured from the TikSync WebSocket during the live session

**Known issues:**
- Returns 403 "no schema found" with a Shop token — must use Creator token.
- The `live_room_id` is different from the `live_id` used in other endpoints.
- Scope `data.analytics.public.read` may not be available in all TikTok Partner Center app configurations.

---

## Webhooks

### Order Webhook

| Detail | Value |
|---|---|
| Endpoint | `/webhooks/tiktok` (configured in this app) |
| Signature header | `x-tiktok-signature`, `x-tt-signature`, or `x-signature` |
| Timestamp header | `x-tiktok-timestamp`, `x-tt-timestamp`, or `x-signature-timestamp` |
| Signature algorithm | HMAC-SHA256 of request body with `TIKTOK_APP_SECRET` |

**Webhook payload issues:**
- Webhook order payloads often arrive with **incomplete data**: `$0.00` total, `"Guest"` customer name, no line items.
- The app works around this by using the webhook as a trigger, then **fetching full order details** from the Order Detail API asynchronously.
- This enrichment happens in a background thread (`_start_tiktok_webhook_enrichment`).

---

## Live Chat (TikSync)

Live chat messages are captured via the **TikSync SDK**, a third-party WebSocket service.

| Detail | Value |
|---|---|
| Library | `tiksync` (Python package) |
| Config | `TIKTOK_LIVE_API_KEY` and `TIKTOK_LIVE_USERNAME` in `.env` |
| Connection | WebSocket to TikSync servers |

**Capabilities:**
- Real-time chat messages from the TikTok live stream
- Viewer count updates
- Captures `live_room_id` from the WebSocket connection (needed for `live_core_stats`)

**Implementation:** See `app/tiktok_live_chat.py`.

---

## Environment Variables

### Required for TikTok integration

| Variable | Description |
|---|---|
| `TIKTOK_APP_KEY` | TikTok Partner Center app key |
| `TIKTOK_APP_SECRET` | TikTok Partner Center app secret |
| `TIKTOK_REDIRECT_URI` | OAuth callback URL (e.g., `https://yourapp.com/integrations/tiktok/callback`) |

### Optional / auto-populated

| Variable | Description |
|---|---|
| `TIKTOK_SHOP_ID` | TikTok shop ID (populated from OAuth response) |
| `TIKTOK_SHOP_CIPHER` | Shop cipher (populated from OAuth response, needed for analytics APIs) |
| `TIKTOK_ACCESS_TOKEN` | Fallback access token (DB is primary source) |
| `TIKTOK_REFRESH_TOKEN` | Fallback refresh token (DB is primary source) |
| `TIKTOK_SHOP_API_BASE_URL` | Override API base URL (default: `https://open-api.tiktokglobalshop.com`) |
| `TIKTOK_BASE_URL` | Creator API base URL (default: `https://open.tiktokapis.com`) |
| `TIKTOK_SYNC_ENABLED` | Enable/disable background order sync (default: `true`) |
| `TIKTOK_SYNC_INTERVAL_MINUTES` | Poll interval for order sync (default: `15`) |
| `TIKTOK_STARTUP_BACKFILL_DAYS` | Days to look back on startup (default: `30`) |
| `TIKTOK_LIVE_API_KEY` | TikSync SDK API key for live chat |
| `TIKTOK_LIVE_USERNAME` | TikTok username for live chat connection |

---

## Known Quirks and Gotchas

### Authentication
- **Auth codes expire in seconds.** The callback must exchange immediately — any delay causes `invalid_grant` / `Authorization code is expired`.
- **Shop vs Creator tokens are completely separate.** You cannot use a Shop token for Creator endpoints or vice versa. They use different OAuth flows, different base URLs, and different scopes.
- **Tokens persist in the database** (`TikTokAuth` table). The app auto-refreshes. You only need to OAuth once unless the refresh token expires (~180 days).

### API Signing
- The `access_token` is **excluded** from the signature but included as a query param.
- The `sign` param is **excluded** from the signature.
- For GET requests, `body` in the signature string is an empty string.
- For POST requests, `body` is the JSON-serialized request body with keys sorted and no whitespace (`separators=(",", ":")`).
- Analytics endpoints use `api_version=""` (empty) in the signing — they have their own version in the path.

### Analytics
- **Overview performance is delayed ~2 days.** Do not expect today's data.
- **Performance list (`/performance`)** is the best endpoint for listing streams and getting per-stream GMV. It's near-real-time for active streams.
- **Per-minute data** (`/performance_per_minutes`) only works after a stream ends.
- **`today=true` parameter causes 66007001 Rpc error.** Never use it.
- **Error code 66007001** is a generic TikTok server error. Handle it gracefully and retry.
- **403 "no schema found"** means the endpoint requires a scope your app doesn't have, or you're using the wrong token type (Shop vs Creator).

### Orders
- **Webhook payloads are incomplete.** Always fetch full details from the API after receiving a webhook.
- **`subtotal_price` ≠ `total_price`.** TikTok's GMV = subtotal (product value). Total includes tax and shipping.
- **Backfill on restart** should start from `max(TikTokOrder.created_at) - 2 hours`, not from the beginning, to avoid re-importing thousands of orders.
- **`database is locked`** errors occur under concurrent SQLite writes. The app uses WAL mode + busy_timeout + retry logic with exponential backoff.

### Timestamps
- TikTok uses **epoch seconds** (not milliseconds) in most places, but some fields use milliseconds (>10 billion = likely ms).
- Always store and compare datetimes as **timezone-aware UTC**. Mixing naive and aware datetimes causes `can't compare offset-naive and offset-aware datetimes`.

### Rate Limits
- TikTok Shop API has rate limits but they are generally generous for a single shop.
- Analytics endpoints are more restrictive — don't poll more than every few minutes.

---

## API Base URLs Summary

| Purpose | Base URL |
|---|---|
| Shop API (orders, products, analytics) | `https://open-api.tiktokglobalshop.com` |
| Shop OAuth (token get/refresh) | `https://auth.tiktok-shops.com` |
| Creator API (live_core_stats) | `https://open.tiktokapis.com` |
| TikTok authorization page | `https://auth.tiktok-shops.com/oauth/authorize` |

---

## Endpoint Quick Reference

| Endpoint | Method | Token | Purpose |
|---|---|---|---|
| `/api/v2/token/get` | GET | — | Exchange auth code for Shop token |
| `/api/v2/token/refresh` | GET | — | Refresh Shop token |
| `/v2/oauth/token/` | POST | — | Exchange/refresh Creator token |
| `/order/202309/orders/search` | POST | Shop | Search orders |
| `/order/202309/orders` | GET | Shop | Get order details |
| `/product/202309/products/search` | POST | Shop | Search products |
| `/product/202309/products` | GET | Shop | Get product details |
| `/analytics/202509/shop_lives/overview_performance` | GET | Shop | Daily aggregated live stats (delayed ~2d) |
| `/analytics/202509/shop_lives/performance` | GET | Shop | List stream sessions with per-stream stats |
| `/analytics/202510/shop_lives/{live_id}/performance_per_minutes` | GET | Shop | Per-minute breakdown for ended streams |
| `/analytics/202502/live_rooms/{live_room_id}/core_stats` | GET | Creator | Real-time live stream stats |

---

## Files

| File | Responsibility |
|---|---|
| `app/tiktok_ingest.py` | OAuth token exchange, webhook parsing, order normalization |
| `scripts/tiktok_backfill.py` | API request signing, order/product fetching, all analytics API calls |
| `app/main.py` | FastAPI routes, background pollers, streamer dashboard, analytics page |
| `app/models.py` | `TikTokAuth`, `TikTokOrder`, `TikTokProduct`, `AppSetting` models |
| `app/config.py` | Environment variable definitions |
| `app/tiktok_live_chat.py` | TikSync WebSocket for live chat + room ID capture |
| `app/db.py` | Database init, migrations, SQLite locking helpers |
