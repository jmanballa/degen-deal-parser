# Cardeye (Degen Eye v2) v1-Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Degen Eye v2 behave like v1 (every scan with a `best_match` lands in the batch drawer, low-confidence surfaces as a warning pill) while reducing perceived time-to-identify by dropping the SSE round-trip on the default tap path.

**Architecture:** Frontend-first change to the v2 scan template — the orchestrator's MATCHED/AMBIGUOUS verdict is preserved, but the UI stops treating AMBIGUOUS as rejection. Default flow switches from `POST /scan-init` + SSE `/scan-stream` to a single `POST /degen_eye/v2/scan`. A small backend telemetry addition (per-stage ms on history entries) informs Phase 2 decisions.

**Tech Stack:** FastAPI (Python 3.11), Jinja2 templates, vanilla JS with `localStorage.scan_batch`, OpenCV + imagehash (unchanged), TCGTracking price cache (unchanged), pytest for backend contract tests.

**Spec:** `docs/superpowers/specs/2026-04-24-cardeye-v1-parity-design.md`

**Out of scope (explicitly deferred):** numpy pHash vectorization, auto-capture flow changes, keypoint matching, set-symbol OCR.

---

## File map

| File | Role | Change |
|---|---|---|
| `tests/test_degen_eye_v2_scan.py` | Backend contract — NEW | Pin: AMBIGUOUS response includes `best_match`; identified_ms present on result |
| `app/degen_eye_v2.py` | Orchestrator | Stamp `identified_ms` on v2 history entries; no logic changes |
| `app/templates/inventory_scan_pokemon_v2.html` | Scan page (the main change) | Extend batch-item shape with `_confidence`/`_match_source`/`_status`; render confidence pill; fix `done` handler to add on any `best_match`; swap default tap to POST `/scan`; gate 4-dot progress behind localStorage toggle |

---

## Review checkpoints

Two natural review points:

- **After Task 4** — the reliability bug is fixed. Manually scan a handful of cards on machine B and confirm every scan lands in the drawer with the right pill. If this fails, we stop and debug before proceeding to speed work.
- **After Task 7** — the speed change is in. Compare perceived time-to-identify to v1 on the same device.

---

## Task 1: Backend contract — AMBIGUOUS responses include best_match

Pin the contract the frontend is about to rely on: even when the orchestrator decides a scan is AMBIGUOUS, the response payload contains a populated `best_match` (the pHash top candidate) and a `candidates` list.

**Files:**
- Create: `tests/test_degen_eye_v2_scan.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_degen_eye_v2_scan.py
"""Contract tests for Degen Eye v2 orchestrator.

The v2 frontend (inventory_scan_pokemon_v2.html) relies on AMBIGUOUS
scan responses still carrying a populated best_match so every scan lands
in the batch drawer. This test pins that contract.
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import degen_eye_v2 as v2
from app import phash_scanner as ps


def _sync(coro):
    return asyncio.run(coro)


def _fake_match(name: str, number: str, set_id: str, set_name: str, distance: int, phash: int = 0) -> ps.PhashMatch:
    return ps.PhashMatch(
        entry=ps.PhashEntry(
            card_id=f"tcgdex:{set_id}-{number}",
            name=name, number=number, set_id=set_id, set_name=set_name,
            phash=phash, image_url="https://example/x.png",
            tcgplayer_url=None, source="tcgdex",
        ),
        distance=distance,
        confidence=ps._band(distance),
        margin_to_next=0,
        rank=0,
    )


# A 1x1 PNG so base64 decode succeeds in run_v2_pipeline
_TINY_PNG = base64.b64encode(
    bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
        "890000000d4944415478da63000000000000050001e7f9b6440000000049454e"
        "44ae426082"
    )
).decode("ascii")


def test_ambiguous_same_art_reprint_still_returns_best_match(monkeypatch):
    """When two different printings tie at the same pHash distance,
    the orchestrator marks the scan AMBIGUOUS but must still populate
    best_match so the frontend can show it (with a warning pill) in the
    batch drawer."""
    matches = [
        _fake_match("Charizard", "4", "base1", "Base Set", distance=5),
        _fake_match("Charizard", "2", "xy12", "Evolutions", distance=5),
    ]
    monkeypatch.setattr(ps, "has_index", lambda: True)
    monkeypatch.setattr(ps, "lookup", lambda *a, **kw: (123, matches))
    monkeypatch.setattr(v2, "detect_and_crop", lambda _: (None, {"reason": "skipped"}))

    async def _no_price(*args, **kwargs):
        return {
            "market_price": None, "tcgplayer_url": None,
            "image_url": None, "image_url_small": None,
            "variants": [], "source": "none", "elapsed_ms": 0.0,
        }
    monkeypatch.setattr(v2, "get_price_for_match", _no_price)

    result = _sync(v2.run_v2_pipeline(_TINY_PNG, category_id="3"))

    assert result["status"] == "AMBIGUOUS", f"expected AMBIGUOUS, got {result.get('status')}"
    assert result.get("best_match"), "AMBIGUOUS result must still include best_match"
    assert result["best_match"]["name"] == "Charizard"
    assert len(result.get("candidates") or []) >= 2
```

- [ ] **Step 2: Run test (diagnostic)**

```bash
pytest tests/test_degen_eye_v2_scan.py::test_ambiguous_same_art_reprint_still_returns_best_match -xvs
```

Expected: **PASS.** The orchestrator today already populates best_match on AMBIGUOUS — this test pins that behavior so nothing regresses it. If it FAILS, the orchestrator has an unexpected shape change — stop and debug before proceeding.

- [ ] **Step 3: Commit**

```bash
git add tests/test_degen_eye_v2_scan.py
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "test(degen-eye-v2): pin AMBIGUOUS responses include best_match

The v2 frontend is about to depend on this contract — pin it first
so the upcoming 'add every best_match to the drawer' change can't
silently regress if the orchestrator's AMBIGUOUS branch changes shape.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Frontend — extend batch item shape with confidence/source/status

Prep for the pill. Extend `addToBatch` to persist the three fields the pill renderer needs. No visual change yet — the pill renderer is Task 3. This task alone is harmless if shipped standalone because the new fields are ignored by both v2's current render path and v1's batch-review page.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (function `addToBatch`, ~L588–615)

- [ ] **Step 1: Update `addToBatch` to record confidence/source/status**

Find `addToBatch(result)` in `inventory_scan_pokemon_v2.html`. Replace the `item` object literal with the version below (three new `_`-prefixed fields, everything else identical):

```javascript
function addToBatch(result) {
    var m = result.best_match || {};
    var variants = m.available_variants || [];
    var variant = '';
    var autoPrice = m.market_price;
    if (variants.length) {
        variant = variants[0].name || '';
        if (variants[0].price != null) autoPrice = variants[0].price;
    }
    var item = {
        card_name: m.name || '',
        game: result.game || 'Pokemon',
        condition: 'NM',
        set_id: m.set_id || '',
        set_name: m.set_name || '',
        card_number: m.number || '',
        image_url: m.image_url || '',
        auto_price: autoPrice,
        is_foil: (variant || '').toLowerCase().indexOf('holo') >= 0,
        variant: variant,
        notes: 'Scanner: Degen Eye v2',
        _available_variants: variants,
        _source: 'v2',
        _v2_capture_id: result.capture_id || ((result.debug || {}).v2_capture_id) || '',
        _confidence: m.confidence || result.status || '',
        _match_source: m.source || (result.disambiguation_method || ''),
        _status: result.status || ''
    };
    batch.push(item);
    saveBatch();
    showToast('✓ ' + (m.name || 'Card') + ' added');
}
```

- [ ] **Step 2: Verify the template still parses**

```bash
python -c "from jinja2 import Environment, FileSystemLoader; Environment(loader=FileSystemLoader('app/templates')).get_template('inventory_scan_pokemon_v2.html')"
```

Expected: exits 0 with no output.

- [ ] **Step 3: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "feat(degen-eye-v2): persist confidence/source/status on batch items

Prep for the confidence pill in the drawer. New _confidence /
_match_source / _status fields are additive — ignored by v1 review and
by the current drawer renderer, no behavior change yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Frontend — render confidence pill in the drawer

Add the pill to the drawer via DOM APIs after `renderBatch` runs. No new string-based HTML injection; the existing renderBatch's output is untouched.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (CSS block ~L98–137; tail of `renderBatch` ~L323–327)

- [ ] **Step 1: Add pill CSS**

Near the bottom of the `<style>` block (just before the closing `</style>`, or just after `.drawer-item-remove` rules), add:

```css
.pill {
    display: inline-block; padding: 1px 6px; border-radius: 999px;
    font-size: 10px; font-weight: 800; letter-spacing: .02em;
    text-transform: lowercase; margin-left: 6px; vertical-align: middle;
    border: 1px solid transparent;
}
.pill-warn {
    background: rgba(255, 193, 7, .15); color: #ffd54f;
    border-color: rgba(255, 193, 7, .4);
}
.pill-review {
    background: rgba(244, 67, 54, .12); color: #ff8a80;
    border-color: rgba(244, 67, 54, .4);
}
.pill-cloud {
    background: rgba(255, 255, 255, .06); color: rgba(255, 255, 255, .5);
    border-color: rgba(255, 255, 255, .12);
}
```

- [ ] **Step 2: Add a `_decoratePills` helper that appends pills after render**

Above `function renderBatch()` (~L263), add:

```javascript
function _decoratePills() {
    // Runs after renderBatch repopulates the drawer body. For each item in
    // the current batch we look up its corresponding .drawer-item-name
    // node (rendered in reverse order by renderBatch) and append pill
    // nodes built via createElement — no innerHTML.
    var body = document.getElementById('drawer-body');
    if (!body) return;
    var nameNodes = body.querySelectorAll('.drawer-item-name');
    if (!nameNodes || nameNodes.length === 0) return;
    for (var idx = 0; idx < nameNodes.length; idx++) {
        // nameNodes[idx] corresponds to batch index (batch.length - 1 - idx)
        var batchIdx = batch.length - 1 - idx;
        if (batchIdx < 0 || batchIdx >= batch.length) continue;
        var item = batch[batchIdx] || {};
        var conf = String(item._confidence || '').toUpperCase();
        var status = String(item._status || '').toUpperCase();
        var source = String(item._match_source || '').toLowerCase();

        if (status === 'AMBIGUOUS') {
            _appendPill(nameNodes[idx], 'review', 'pill-review',
                'Scan was ambiguous — confirm on the review page before submitting.');
        } else if (conf === 'LOW') {
            _appendPill(nameNodes[idx], 'low', 'pill-warn',
                'Low-confidence match — double-check this one.');
        } else if (conf === 'MEDIUM') {
            _appendPill(nameNodes[idx], 'medium', 'pill-warn',
                'Medium-confidence match.');
        }
        if (source.indexOf('ximilar') >= 0 && source !== 'phash') {
            _appendPill(nameNodes[idx], 'cloud', 'pill-cloud',
                'Matched via cloud fallback.');
        }
    }
}

function _appendPill(parentEl, label, className, title) {
    var pill = document.createElement('span');
    pill.className = 'pill ' + className;
    pill.textContent = label;
    if (title) pill.title = title;
    parentEl.appendChild(pill);
}
```

- [ ] **Step 3: Call `_decoratePills()` at the end of `renderBatch`**

Find the final line of `renderBatch` (after `body.innerHTML = html;` and any binding of the remove button click handlers) and append:

```javascript
_decoratePills();
```

- [ ] **Step 4: Manually verify pill rendering without running a scan**

On machine B in the browser console on `/degen_eye/v2`:

```javascript
var b = JSON.parse(localStorage.scan_batch || '[]');
b.push({card_name:'Test HIGH', _confidence:'HIGH', _status:'MATCHED', auto_price: 1, image_url:''});
b.push({card_name:'Test LOW', _confidence:'LOW', _status:'MATCHED', auto_price: 1, image_url:''});
b.push({card_name:'Test AMBI', _confidence:'LOW', _status:'AMBIGUOUS', auto_price: 1, image_url:''});
b.push({card_name:'Cloud', _confidence:'MEDIUM', _status:'MATCHED', _match_source:'ximilar', auto_price: 1, image_url:''});
localStorage.scan_batch = JSON.stringify(b);
location.reload();
```

Expected: drawer shows 4 items — "Test HIGH" (no pill), "Test LOW" (yellow "low"), "Test AMBI" (red "review"), "Cloud" (yellow "medium" + grey "cloud"). Clean up: `localStorage.removeItem('scan_batch'); location.reload();`.

- [ ] **Step 5: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "feat(degen-eye-v2): render confidence pill in batch drawer

Decorates drawer items via DOM APIs (createElement + textContent) after
renderBatch paints. HIGH -> no pill; MEDIUM/LOW -> yellow 'medium'/
'low'; AMBIGUOUS -> red 'review'; any Ximilar fallback -> grey 'cloud'.
Items without _confidence/_status (e.g. v1-origin drawer items) get
no pill, so the change is safe for mixed-source batches.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Frontend — add every `best_match` to the drawer (the bug fix)

The core reliability fix. In the SSE `done` handler, stop gating on MATCHED — add whenever a `best_match` exists.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (`done` handler ~L551–565)

- [ ] **Step 1: Replace the MATCHED-gated branch**

Find the SSE `done` event listener (~L551). Replace the `if (finalResult && finalResult.status === 'MATCHED' && finalResult.best_match)` block with:

```javascript
es.addEventListener('done', function(ev) {
    try { finalResult = JSON.parse(ev.data); } catch (_) {}
    streamFinished = true;
    es.close();
    scanInFlight = false;
    if (finalResult && finalResult.best_match) {
        // Add to drawer regardless of MATCHED/AMBIGUOUS — confidence is
        // surfaced via the pill on the drawer item, and /inventory/scan/
        // batch-review is where the user fixes up low-confidence ones.
        addToBatch(finalResult);
        setTimeout(_hideResult, 900);
    } else {
        // NO_MATCH or ERROR — only case where the drawer is skipped
        var msg = (finalResult && finalResult.error) || 'No card detected';
        showToast(msg, 'err', 2500);
        setTimeout(_hideResult, 1500);
    }
});
```

- [ ] **Step 2: Lint-check the template renders**

```bash
python -c "from jinja2 import Environment, FileSystemLoader; Environment(loader=FileSystemLoader('app/templates')).get_template('inventory_scan_pokemon_v2.html')"
```

Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "fix(degen-eye-v2): add every best_match to the drawer (not just MATCHED)

Previously v2 showed a red 'Needs review' toast and dropped the scan
whenever the orchestrator returned AMBIGUOUS (same-art reprint risk,
LOW pHash with disagreeing Ximilar, etc.) — which is a large fraction
of real scans on cards outside the pHash index. Now: any best_match
lands in the drawer; the confidence pill on the drawer item signals
which items need review. Matches v1's behavior.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## **REVIEW CHECKPOINT 1 — Reliability fix shipped**

The bug is fixed. Manually verify on machine B before starting speed work:

- [ ] Scan 5+ cards, including at least one you expect to trigger AMBIGUOUS (a reprint, a card not in the index, or a low-light photo).
- [ ] All 5 land in the drawer.
- [ ] LOW / AMBIGUOUS scans show the pill.
- [ ] Clicking "Review Batch →" opens the existing review page and the scanned cards appear for fix-up.
- [ ] "Ship Task 4 and stop here if speed isn't actually a problem anymore" is a valid outcome — revisit with Jeff before continuing.

If any of these fail, stop and debug before Task 5.

---

## Task 5: Frontend — default tap path uses POST /scan (not SSE)

Cut the SSE round-trip on the tap flow. Auto-capture and opt-in live-progress keep SSE.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (`runScan` / tap handler — contains the `POST /scan-init` + `new EventSource('/scan-stream?...')` block, ~L465–585)

- [ ] **Step 1: Rename the existing SSE scan function to `runScanStream`**

Locate the function that today POSTs to `/degen_eye/v2/scan-init` and opens an `EventSource` — it may be named `runScan`, `startScan`, `doScan`, or similar. Rename it to `runScanStream`. Update any internal references so the rename is self-consistent.

Auto-capture code that previously called this function should now call `runScanStream` explicitly — auto-capture always uses SSE.

- [ ] **Step 2: Add a `runScanPost` helper (non-streaming tap path)**

Add just above `runScanStream`:

```javascript
async function runScanPost(b64) {
    // Default tap-to-scan path — single POST, no SSE. The 4-dot progress
    // card is hidden (Task 6); the drawer pill (Task 3) carries confidence.
    var t0 = performance.now();
    _showResultSimple('Scanning…');
    try {
        var resp = await fetch('/degen_eye/v2/scan', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ image: b64, category_id: '3' })
        });
        if (!resp.ok) {
            var text = await resp.text();
            throw new Error('HTTP ' + resp.status + ': ' + text.slice(0, 160));
        }
        var result = await resp.json();
        var elapsed = Math.round(performance.now() - t0);
        if (result && result.best_match) {
            _paintResultSimple(result.best_match);
            addToBatch(result);
            setTimeout(_hideResultSimple, 700);
        } else {
            showToast((result && result.error) || 'No card detected', 'err', 2500);
            setTimeout(_hideResultSimple, 1500);
        }
        console.debug('[v2] tap scan complete in ' + elapsed + 'ms');
    } catch (e) {
        _hideResultSimple();
        showToast('Scan failed: ' + e.message, 'err', 3500);
    }
}

// DOM-safe paint helper — textContent for text, createElement for badges,
// never innerHTML.
function _paintResultSimple(m) {
    var nameEl = document.getElementById('lr-name');
    if (nameEl) {
        while (nameEl.firstChild) nameEl.removeChild(nameEl.firstChild);
        nameEl.appendChild(document.createTextNode(m.name || 'Unknown'));
        if (m.confidence) {
            var badge = document.createElement('span');
            badge.className = 'lr-confidence ' + String(m.confidence);
            badge.textContent = String(m.confidence);
            nameEl.appendChild(badge);
        }
    }
    var priceEl = document.getElementById('lr-price');
    if (priceEl && m.market_price != null) {
        priceEl.textContent = '$' + Number(m.market_price).toFixed(2);
    }
    var thumbEl = document.getElementById('lr-thumb');
    if (thumbEl && m.image_url) thumbEl.src = m.image_url;
}

function _showResultSimple(label) {
    _showResult(label);
    var card = document.getElementById('live-result');
    if (card) card.classList.add('simple');
}

function _hideResultSimple() {
    var card = document.getElementById('live-result');
    if (card) card.classList.remove('simple');
    _hideResult();
}
```

- [ ] **Step 3: Route the shutter button through `runScanPost` by default**

Find the shutter/capture handler — it calls `runScanStream(b64)` (or whatever it called before the rename). Wrap:

```javascript
async function onShutterPressed(b64) {
    if (localStorage.getItem('v2_live_progress') === '1') {
        return runScanStream(b64);
    }
    return runScanPost(b64);
}
```

And change the shutter click/submit handler to call `onShutterPressed(b64)` instead of `runScanStream(b64)` directly.

- [ ] **Step 4: Manually verify the tap flow**

On machine B:

```
1. Open /degen_eye/v2 — ensure localStorage has no `v2_live_progress` key.
2. Tap the shutter on a real card.
3. Expected: single "Scanning..." spinner, then card in drawer <=1s.
4. DevTools Network tab — only one request fires: POST /degen_eye/v2/scan.
   (no /scan-init, no /scan-stream)
5. Console — "[v2] tap scan complete in Xms" with X typically 200-800.
```

- [ ] **Step 5: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "perf(degen-eye-v2): default tap flow uses POST /scan, skip SSE round-trip

Tap-to-scan now fires a single POST /degen_eye/v2/scan instead of the
POST /scan-init + GET /scan-stream pair. Removes one HTTP round-trip
plus a server-side pending-file write per scan. Auto-capture and the
opt-in 'live progress' flow keep SSE unchanged. Result painting uses
textContent + createElement, not innerHTML.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Frontend — hide the 4-dot progress card on the default tap flow

Drop the `detect → identify → price → variants` dots when using `runScanPost`. SSE auto-capture and opt-in live-progress still show them.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (CSS only)

- [ ] **Step 1: Identify the dot-row class**

Open the template. Locate the 4-dot progress row inside the result card — it's the element marked by each `_markDot('lr-dot-detect', ...)` call-site's target. The row's wrapping element typically has a class like `.lr-dots`. If the class name differs in your template, use that name in Step 2.

- [ ] **Step 2: Hide the dot row when `.live-result` has class `simple`**

Add to the `<style>` block:

```css
/* Task 5 runScanPost applies .simple to the result card for the
   single-POST flow — hide the stage dots so the card is just a name
   + price + thumbnail. */
.live-result.simple .lr-dots { display: none; }
```

- [ ] **Step 3: Manually verify**

```
1. Default tap: no dots visible, just spinner -> card.
2. localStorage.setItem('v2_live_progress', '1'); location.reload();
3. Tap again: dots are back (SSE path, runScanStream).
```

- [ ] **Step 4: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "feat(degen-eye-v2): hide 4-dot progress on default tap flow

The stage dots (detect/identify/price/variants) remain visible on the
SSE-backed live-progress path but are hidden on the plain POST tap flow
where the response is already complete when the card appears. Reduces
visual jitter on the fast path.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Frontend — "live progress" opt-in toggle

Surface `v2_live_progress` as a small toggle so users can re-enable the SSE live-progress flow without editing browser storage.

**Files:**
- Modify: `app/templates/inventory_scan_pokemon_v2.html` (top bar markup + toggle wiring)

- [ ] **Step 1: Add the toggle button to the top bar**

Locate the top-bar HTML (contains the mode selector / category selector). Add, using the same `.top-bar-btn`-style class the nearby buttons use:

```html
<button type="button" id="live-progress-toggle" class="top-bar-btn"
        title="Show step-by-step progress while scanning (slower)">
    Live progress: <span id="live-progress-state">off</span>
</button>
```

- [ ] **Step 2: Wire the toggle**

Near the other event wiring at the bottom of the `<script>` block:

```javascript
(function () {
    var btn = document.getElementById('live-progress-toggle');
    var state = document.getElementById('live-progress-state');
    if (!btn || !state) return;
    function render() {
        state.textContent = localStorage.getItem('v2_live_progress') === '1' ? 'on' : 'off';
    }
    btn.addEventListener('click', function () {
        var next = localStorage.getItem('v2_live_progress') === '1' ? '0' : '1';
        localStorage.setItem('v2_live_progress', next);
        render();
    });
    render();
})();
```

- [ ] **Step 3: Manually verify**

```
1. Load /degen_eye/v2. Button shows "Live progress: off".
2. Tap a scan -> no dots.
3. Click toggle -> "Live progress: on".
4. Tap a scan -> dots visible; Network tab shows /scan-init + /scan-stream.
```

- [ ] **Step 4: Commit**

```bash
git add app/templates/inventory_scan_pokemon_v2.html
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "feat(degen-eye-v2): add live-progress toggle in top bar

Exposes the v2_live_progress localStorage flag as a UI toggle so users
who prefer SSE-backed step-by-step progress can opt in without editing
browser storage. Defaults to off (fast POST path).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## **REVIEW CHECKPOINT 2 — Speed change shipped**

Before Task 8, verify perceived latency improved:

- [ ] Scan the same card 5 times on machine B. Record perceived time from shutter tap to card-in-drawer. Should be noticeably faster than pre-Task 5, especially with the pHash index healthy.
- [ ] Scan a card that triggers AMBIGUOUS — still lands in drawer with review pill (no Task 4 regression).
- [ ] Toggle Live Progress on → flow regresses to the old 4-dot SSE card; confirm both paths still work.

If perceived speed is unchanged, that's a sign the backend (pHash lookup or price) is the real bottleneck — Task 8's telemetry will tell us.

---

## Task 8: Backend — stamp `identified_ms` on v2 history entries

Telemetry to confirm whether Phase 2 (pHash numpy vectorization) is worth doing.

**Files:**
- Modify: `app/degen_eye_v2.py` (`run_v2_pipeline` ~L439; `run_v2_pipeline_stream` ~L609)
- Modify: `tests/test_degen_eye_v2_scan.py` (add test)

- [ ] **Step 1: Write a failing test for `identified_ms`**

Append to `tests/test_degen_eye_v2_scan.py`:

```python
def test_history_entry_includes_identified_ms(monkeypatch):
    """v2 debug should carry identified_ms = detect+phash stage sum.
    This is the latency number the debug page displays."""
    matches = [_fake_match("Pikachu", "25", "sv3pt5", "Scarlet & Violet: 151", distance=2)]
    monkeypatch.setattr(ps, "has_index", lambda: True)
    monkeypatch.setattr(ps, "lookup", lambda *a, **kw: (1, matches))
    monkeypatch.setattr(v2, "detect_and_crop", lambda _: (None, {"reason": "skipped"}))

    async def _no_price(*args, **kwargs):
        return {
            "market_price": None, "tcgplayer_url": None,
            "image_url": None, "image_url_small": None,
            "variants": [], "source": "none", "elapsed_ms": 0.0,
        }
    monkeypatch.setattr(v2, "get_price_for_match", _no_price)

    result = _sync(v2.run_v2_pipeline(_TINY_PNG, category_id="3"))

    dbg = result.get("debug") or {}
    v2d = dbg.get("v2") or {}
    stages = v2d.get("stages_ms") or {}
    assert "identified_ms" in v2d, f"identified_ms missing from v2 debug: {list(v2d.keys())}"
    assert isinstance(v2d["identified_ms"], (int, float))
    if stages.get("phash") is not None:
        assert v2d["identified_ms"] >= stages["phash"]
```

- [ ] **Step 2: Run, expect failure**

```bash
pytest tests/test_degen_eye_v2_scan.py::test_history_entry_includes_identified_ms -xvs
```

Expected: **FAIL** with `identified_ms missing from v2 debug`.

- [ ] **Step 3: Stamp `identified_ms` in `run_v2_pipeline`**

In `app/degen_eye_v2.py`, find the line in `run_v2_pipeline` that sets `v2_debug["stages_ms"]["phash"]` after the pHash lookup (~L439). Directly after that line, add:

```python
v2_debug["identified_ms"] = round(
    (v2_debug["stages_ms"].get("detect") or 0.0)
    + (v2_debug["stages_ms"].get("phash") or 0.0),
    1,
)
```

- [ ] **Step 4: Stamp `identified_ms` in `run_v2_pipeline_stream`**

In the same file, find the equivalent location in `run_v2_pipeline_stream` (~L609, immediately after `v2_debug["stages_ms"]["phash"]` is set). Add the same three-line block.

- [ ] **Step 5: Run tests, expect pass**

```bash
pytest tests/test_degen_eye_v2_scan.py -xvs
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add app/degen_eye_v2.py tests/test_degen_eye_v2_scan.py
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "feat(degen-eye-v2): stamp identified_ms on v2 debug

identified_ms = detect_ms + phash_ms — the latency that actually matters
to the user (time-to-card-name-on-screen). Surfacing this on every v2
history entry lets the debug page compute p50/p95, which is what we
need before deciding whether to spend time on pHash numpy vectorization.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Final verification + update docs

Non-code wrap-up.

- [ ] **Step 1: Run the v2 + adjacent tests**

```bash
pytest tests/test_degen_eye_v2_scan.py tests/test_scanner_multigame.py tests/test_scanner_debt_fixes.py -xvs
```

Expected: all pass.

- [ ] **Step 2: Update AGENTS.md**

In `AGENTS.md`, the `Degen Eye v2` section describes the 4-dot progress card and auto-capture. Update the capture-UX paragraph to note:

> Capture UX:
> - **Tap mode** (default): shutter button fires a single POST `/degen_eye/v2/scan`; the result card shows the matched card and hides the stage-progress dots. Every scan with a `best_match` enters the batch drawer; confidence is surfaced as a pill (HIGH none, MEDIUM/LOW yellow, AMBIGUOUS red) so the reviewer sees which items need a second look at `/inventory/scan/batch-review`.
> - **Live progress** (opt-in via top-bar toggle): tap flow regresses to the SSE `/scan-init` + `/scan-stream` pair with the 4-dot progress card.
> - **Auto mode**: unchanged — polls `/detect-only`, fires full SSE scan on 3 stable frames.

- [ ] **Step 3: Update PROJECT_STATUS**

In `PROJECT_STATUS.md`, under the Degen Eye v2 section, add:

```
- **2026-04-24** — v2 tap flow matches v1 reliability: every scan lands in the drawer with a confidence pill; default tap uses a single POST (SSE behind a Live Progress toggle). See docs/superpowers/specs/2026-04-24-cardeye-v1-parity-design.md and docs/superpowers/plans/2026-04-24-cardeye-v1-parity.md.
```

- [ ] **Step 4: Commit docs**

```bash
git add AGENTS.md PROJECT_STATUS.md
git -c user.email='jeffreylee94@gmail.com' -c user.name='Jeffrey Lee' commit -m "docs: note Degen Eye v2 v1-parity tap flow + live-progress toggle

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 5: Manual acceptance pass on machine B**

Run through the spec's acceptance criteria:

1. Scan a known-reprint-risk card → lands in drawer with red "review" pill.
2. Scan a healthy HIGH-confidence card → lands in drawer, no pill, subjectively faster than v1.
3. Open `/degen_eye/v2/debug` (or `/degen_eye/v2/stats`) → `identified_ms` populated on v2 entries / available via stats.
4. Toggle Live Progress on → scan regresses to SSE + dots, still works.

If all four pass, we're done.

---

## What's explicitly NOT in this plan

- Numpy pHash vectorization — defer until Task 8's telemetry shows pHash is the bottleneck.
- Auto-capture flow changes — still uses SSE + stability_hash; no touch.
- Backend orchestrator logic (`run_v2_pipeline` MATCHED/AMBIGUOUS decision) — unchanged.
- Batch-review page UI changes — relies on existing behavior; AMBIGUOUS items can be sort-prioritized in a follow-up.
- Nightly phash index rebuild automation — separate Phase C work.
- `network_elapsed_ms` middleware — the spec mentioned it, but `identified_ms` alone is the key lever and the middleware adds complexity; defer unless Task 8's numbers are hard to interpret without it.

## Self-review notes

- **Spec coverage:** Every goal in the spec maps to a task — every-scan-to-drawer (Task 4), confidence pill (Tasks 2+3), single-POST default path (Task 5), perceived-latency improvement via dot-hide (Task 6), opt-in SSE (Task 7), backend telemetry (Task 8). Non-goals (orchestrator rewrite, keypoints, multi-TCG) are explicitly excluded above.
- **Placeholder scan:** No TBD/TODO/"handle edge cases"; every step has real code and real commands.
- **Type/naming consistency:** `runScanStream` (renamed existing SSE fn), `runScanPost` (new), `onShutterPressed` (new dispatcher), `_decoratePills` / `_appendPill` / `_showResultSimple` / `_hideResultSimple` / `_paintResultSimple` — all referenced where defined. The localStorage key `v2_live_progress` is used consistently in Tasks 5, 6, 7.
