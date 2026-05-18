---
name: ruflo-degen-orchestration
description: Use Ruflo as a sidecar for live-deal-parser memory, diff review, and agent coordination without replacing the repo's AGENTS.md or changing app runtime behavior.
---

# Ruflo Degen Orchestration

Use Ruflo as a sidecar only. Codex or OpenClaw still performs the actual code
edits, tests, commits, and pushes.

## Rules

- Start with `scripts/ruflo_pilot.py status` when checking whether Ruflo is usable.
- Use `scripts/ruflo_pilot.py init-memory --apply` before storing project memory.
- Use `scripts/ruflo_pilot.py seed-memory --apply` to seed known Degen invariants.
- Use `scripts/ruflo_pilot.py preflight "<task>" --apply` before risky or multi-agent work.
- Use `scripts/ruflo_pilot.py handoff-openclaw "<task>" --apply` before handing work to OpenClaw.
- Use `scripts/ruflo_pilot.py remember "<key>" "<lesson>" --tags tag1,tag2 --apply` after a useful fix.
- Use `scripts/ruflo_pilot.py review-diff --apply` as an extra review signal before risky commits.
- Ledger automation handoffs should start from `/ledger?status=needs_action&action_reason=needs_log_check`, use the Automation Workbench preview before applying bulk log-check changes, and then re-run `scripts/ruflo_pilot.py review-diff --apply` before commit.
- Do not run `ruflo init --codex --force`; this repo already has a specific `AGENTS.md`.
- Do not let Ruflo touch production, pull on production, restart services, or override deploy rules.
- Continue to coordinate local Codex and OpenClaw through git, explicit file scopes, and tests.

## Useful Commands

```powershell
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py status
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py init-memory --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py seed-memory --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py search-memory "TikTok webhook" --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py preflight "fix parser stitching" --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py handoff-openclaw "fix parser stitching" --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py remember "parser/stitch-fix" "what worked" --tags parser,stitching --apply
.\.venv\Scripts\python.exe scripts\ruflo_pilot.py review-diff --apply
```
