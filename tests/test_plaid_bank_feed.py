from sqlmodel import Session, SQLModel, create_engine, select

from app.models import BankFeedConnection, BankTransaction
from app.discord.plaid_bank_feed import sync_plaid_connection


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def test_plaid_sync_runs_ledger_agent_after_successful_sync(monkeypatch):
    engine = make_engine()

    def fake_plaid_post(path, payload, *, timeout=30.0):
        assert path == "/transactions/sync"
        return {
            "accounts": [
                {
                    "account_id": "acc_1",
                    "name": "Chase Checking",
                    "type": "depository",
                    "subtype": "checking",
                    "mask": "1234",
                    "balances": {"current": 1000.0, "available": 950.0, "iso_currency_code": "USD"},
                }
            ],
            "added": [
                {
                    "account_id": "acc_1",
                    "transaction_id": "plaid_tx_1",
                    "date": "2026-05-15",
                    "authorized_date": "2026-05-15",
                    "merchant_name": "Amazon Prime Video",
                    "name": "Amazon Prime Video",
                    "amount": 11.99,
                    "category": ["Entertainment"],
                    "payment_channel": "online",
                    "personal_finance_category": {
                        "primary": "ENTERTAINMENT",
                        "detailed": "ENTERTAINMENT_VIDEO",
                    },
                }
            ],
            "modified": [],
            "removed": [],
            "next_cursor": "cursor_after_sync",
            "has_more": False,
        }

    monkeypatch.setattr("app.discord.plaid_bank_feed.decrypt_access_token", lambda _blob: "access-token")
    monkeypatch.setattr("app.discord.plaid_bank_feed._plaid_post", fake_plaid_post)

    with Session(engine) as session:
        connection = BankFeedConnection(
            provider="plaid",
            provider_item_id="item_1",
            access_token_enc=b"encrypted",
            institution_name="Chase",
            status="active",
        )
        session.add(connection)
        session.commit()
        session.refresh(connection)

        result = sync_plaid_connection(session, connection.id or 0)
        row = session.exec(select(BankTransaction).where(BankTransaction.provider_transaction_id == "plaid_tx_1")).one()

    assert result["added"] == 1
    assert result["ledger_agent"]["scanned_count"] == 1
    assert result["ledger_agent"]["auto_reviewed"] == 0
    assert row.expense_category == "meals_entertainment"
    assert row.review_status == "open"
