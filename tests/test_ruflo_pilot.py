from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "ruflo_pilot.py"


def load_ruflo_pilot():
    spec = importlib.util.spec_from_file_location("ruflo_pilot", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_seed_plan_stores_degen_invariants() -> None:
    module = load_ruflo_pilot()

    plan = module.build_seed_memory_plan()
    commands = [step.command for step in plan]
    joined = [" ".join(command) for command in commands]

    assert any("memory store" in command for command in joined)
    assert any("parser/stitching" in command for command in joined)
    assert any("tiktok/webhook-signature" in command for command in joined)
    assert any("45s" in command or "45 seconds" in command for command in joined)
    assert any("HMAC-SHA256(app_secret, app_key + raw_body)" in command for command in joined)
    assert all("-n" in command and "degen" in command for command in commands)
    assert all("--upsert" in command for command in commands)


def test_apply_flag_is_explicit_opt_in() -> None:
    module = load_ruflo_pilot()
    parser = module.make_parser()

    dry_run = parser.parse_args(["seed-memory"])
    applied = parser.parse_args(["seed-memory", "--apply"])

    assert dry_run.apply is False
    assert applied.apply is True


def test_status_plan_uses_ruflo_doctor_without_auto_install() -> None:
    module = load_ruflo_pilot()

    plan = module.build_status_plan()
    commands = [step.command for step in plan]

    assert ["npx", "--yes", "ruflo@3.7.0-alpha.45", "--version"] in commands
    assert ["npx", "--yes", "ruflo@3.7.0-alpha.45", "doctor", "--fix"] in commands
    assert not any("--install" in command for command in commands)


def test_review_diff_plan_avoids_current_ruflo_risk_flag_bug() -> None:
    module = load_ruflo_pilot()

    plan = module.build_review_diff_plan()

    assert plan == [
        module.PlannedCommand(
            "Classify current git diff",
            ["npx", "--yes", "ruflo@3.7.0-alpha.45", "analyze", "diff", "--classify"],
        ),
        module.PlannedCommand(
            "Recommend reviewers for current git diff",
            ["npx", "--yes", "ruflo@3.7.0-alpha.45", "analyze", "diff", "--reviewers"],
        ),
    ]


def test_preflight_plan_searches_memory_and_routes_task() -> None:
    module = load_ruflo_pilot()

    plan = module.build_preflight_plan("fix parser stitching")

    assert plan == [
        module.PlannedCommand(
            "Current git state",
            ["git", "status", "--short", "--branch"],
        ),
        module.PlannedCommand(
            "Search Degen Ruflo memory",
            [
                "npx",
                "--yes",
                "ruflo@3.7.0-alpha.45",
                "memory",
                "search",
                "-q",
                "fix parser stitching",
                "-n",
                "degen",
                "-t",
                "keyword",
                "-l",
                "8",
            ],
        ),
        module.PlannedCommand(
            "Route task through Ruflo hooks",
            [
                "npx",
                "--yes",
                "ruflo@3.7.0-alpha.45",
                "hooks",
                "route",
                "-t",
                "fix parser stitching",
                "-K",
                "3",
            ],
        ),
    ]


def test_remember_plan_stores_user_lesson_with_tags() -> None:
    module = load_ruflo_pilot()

    plan = module.build_remember_plan("plaid/webhook", "Verify webhook URL before live sync", "finance,oauth")

    assert plan == [
        module.PlannedCommand(
            "Store plaid/webhook",
            [
                "npx",
                "--yes",
                "ruflo@3.7.0-alpha.45",
                "memory",
                "store",
                "-k",
                "plaid/webhook",
                "-v",
                "Verify webhook URL before live sync",
                "-n",
                "degen",
                "--upsert",
                "--tags",
                "finance,oauth",
            ],
        )
    ]


def test_default_handoff_path_is_ignored_runtime_file() -> None:
    module = load_ruflo_pilot()
    now = dt.datetime(2026, 5, 17, 11, 30, 0)

    path = module.default_handoff_path("Fix parser / TikTok?", now=now)

    assert path == PROJECT_ROOT / ".ruflo" / "handoffs" / "20260517-113000-fix-parser-tiktok.md"


def test_handoff_packet_includes_state_and_guardrails(monkeypatch) -> None:
    module = load_ruflo_pilot()

    outputs = {
        ("git", "branch", "--show-current"): "main\n",
        ("git", "status", "--short", "--branch"): "## main...origin/main [behind 1]\n M app/parser.py\n",
        ("git", "diff", "--stat"): " app/parser.py | 2 +-\n",
    }

    monkeypatch.setattr(
        module,
        "capture_command",
        lambda command, cwd=module.PROJECT_ROOT: outputs[tuple(command)],
    )

    packet = module.build_openclaw_handoff_packet("Fix parser stitching")

    assert "# OpenClaw Handoff: Fix parser stitching" in packet
    assert "Branch: `main`" in packet
    assert " M app/parser.py" in packet
    assert "Do not edit production files under `/opt/degen/app`" in packet
    assert "Run `scripts/ruflo_pilot.py preflight \"Fix parser stitching\" --apply`" in packet
    assert "Run focused tests before handing back" in packet


def test_run_plan_resolves_windows_command_shims(monkeypatch) -> None:
    module = load_ruflo_pilot()
    calls: list[list[str]] = []

    monkeypatch.setattr(
        module.shutil,
        "which",
        lambda name: {
            "npm": r"C:\Program Files\nodejs\npm.cmd",
            "npx": r"C:\Program Files\nodejs\npx.cmd",
        }.get(name),
    )

    def fake_run(command, *, cwd, check):
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    module.run_plan(
        [
            module.PlannedCommand("npm", ["npm", "--version"]),
            module.PlannedCommand("npx", ["npx", "--yes", "ruflo@3.7.0-alpha.45", "--version"]),
        ],
        apply=True,
    )

    assert calls == [
        [r"C:\Program Files\nodejs\npm.cmd", "--version"],
        [r"C:\Program Files\nodejs\npx.cmd", "--yes", "ruflo@3.7.0-alpha.45", "--version"],
    ]


def test_ruflo_runtime_artifacts_are_gitignored() -> None:
    gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")

    for pattern in [
        ".swarm/",
        ".claude-flow/",
        ".ruflo/",
        "ruvector.db",
        "agentdb.rvf",
        "agentdb.rvf.lock",
    ]:
        assert pattern in gitignore


def test_project_codex_config_only_adds_ruflo_mcp() -> None:
    config = (PROJECT_ROOT / ".agents" / "config.toml").read_text(encoding="utf-8")

    assert "[mcp_servers.ruflo]" in config
    assert 'command = "npx"' in config
    assert 'args = ["--yes", "ruflo@3.7.0-alpha.45", "mcp", "start"]' in config
    assert "model =" not in config
    assert "approval_policy" not in config
    assert "sandbox_mode" not in config


def test_ruflo_skill_documents_ledger_workbench_handoff() -> None:
    skill = (PROJECT_ROOT / ".agents" / "skills" / "ruflo-degen-orchestration" / "SKILL.md").read_text(encoding="utf-8")

    assert "/ledger?status=needs_action&action_reason=needs_log_check" in skill
    assert "Automation Workbench" in skill
    assert "review-diff --apply" in skill
