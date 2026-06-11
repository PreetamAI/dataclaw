from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHAT = REPO_ROOT / "backend" / "app" / "services" / "agents" / "chat.py"
POSTGRES_SEED = REPO_ROOT / "tests" / "integration" / "postgres" / "01_seed.sql"
ORCHESTRATION_API = REPO_ROOT / "tests" / "integration" / "services" / "orchestration_api.py"


def test_acme_finance_demo_seed_supports_duplicate_payment_story() -> None:
    seed = POSTGRES_SEED.read_text()
    fixture_api = ORCHESTRATION_API.read_text()

    assert "priya.shah@northstar-retail.example" in seed
    assert "Northstar Retail Group" in seed
    assert "northstar_duplicate_payment" in seed
    assert "'stuck_in_3ds'" in seed
    assert "INSERT INTO core.refunds" in seed
    assert '"page-order-status-definitions"' in fixture_api
    assert '"page-ownership-runbook"' in fixture_api
    assert '"page-refund-alerts-sop"' in fixture_api
    assert '"page-duplicate-payment-runbook"' in fixture_api


def test_acme_finance_demo_direct_chat_hooks_exist() -> None:
    chat = CHAT.read_text()

    assert 'if "duplicate" in lower and "payment" in lower and ("customer" in lower or "customers" in lower):' in chat
    assert "Customers with duplicate successful payments" in chat
    assert 'if "stuck_in_3ds" in lower:' in chat
    assert 'if "refund processing" in lower and ("dag" in lower or "airflow" in lower):' in chat
    assert 'if "airflow dag" in lower and "weekly_revenue" in lower:' in chat
    assert 'if "document this" in lower and "investigation" in lower and "notion" in lower:' in chat
