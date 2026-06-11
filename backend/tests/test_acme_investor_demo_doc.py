from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC = REPO_ROOT / "docs" / "ACME_INVESTOR_DEMO.md"
CHAT = REPO_ROOT / "backend" / "app" / "services" / "agents" / "chat.py"
POSTGRES_SEED = REPO_ROOT / "tests" / "integration" / "postgres" / "01_seed.sql"
ORCHESTRATION_API = REPO_ROOT / "tests" / "integration" / "services" / "orchestration_api.py"


def test_acme_investor_demo_seed_supports_duplicate_payment_story() -> None:
    doc = DOC.read_text()
    seed = POSTGRES_SEED.read_text()
    fixture_api = ORCHESTRATION_API.read_text()

    assert "Which customers have duplicate successful payments" in doc
    assert "duplicate successful payments" in doc
    assert "stuck_in_3ds" in doc
    assert "refund_alerts" in doc
    assert "weekly_revenue" in doc

    assert "alice@example.com" in seed
    assert "alice_duplicate_payment" in seed
    assert "'stuck_in_3ds'" in seed
    assert "INSERT INTO core.refunds" in seed
    assert '"page-order-status-definitions"' in fixture_api
    assert '"page-ownership-runbook"' in fixture_api
    assert '"page-refund-alerts-sop"' in fixture_api


def test_acme_investor_demo_prompts_match_direct_chat_hooks() -> None:
    doc = DOC.read_text()
    chat = CHAT.read_text()

    expected_prompts = [
        "Which customers have duplicate successful payments, and what orders should finance review?",
        "How many customers have stuck_in_3ds orders, and what does Notion say that status means?",
        "Which Airflow DAG owns refund processing, and who owns the runbook?",
        "Build me an Airflow DAG that materializes weekly_revenue every Monday.",
        "Document this duplicate payment investigation in Notion.",
    ]
    for prompt in expected_prompts:
        assert prompt in doc

    assert 'if "duplicate" in lower and "payment" in lower and ("customer" in lower or "customers" in lower):' in chat
    assert "Customers with duplicate successful payments" in chat
    assert 'if "stuck_in_3ds" in lower:' in chat
    assert 'if "refund processing" in lower and ("dag" in lower or "airflow" in lower):' in chat
    assert 'if "airflow dag" in lower and "weekly_revenue" in lower:' in chat
    assert 'if "document this" in lower and "investigation" in lower and "notion" in lower:' in chat
