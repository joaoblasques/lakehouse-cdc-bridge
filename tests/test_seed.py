from decimal import Decimal

from banking_cdc.seed import _sql


def test_insert_is_parameterised():
    sql, params = _sql("accounts", "insert", {"account_id": 1, "balance": Decimal("2.00")})
    assert sql == "INSERT INTO dbo.accounts (account_id, balance) VALUES (%s, %s)"
    assert params == (1, Decimal("2.00"))


def test_update_sets_non_key_columns_by_primary_key():
    sql, params = _sql("card_transactions", "update", {"tx_id": 5, "status": "SETTLED"})
    assert sql == "UPDATE dbo.card_transactions SET status = %s WHERE tx_id = %s"
    assert params == ("SETTLED", 5)


def test_delete_by_primary_key():
    sql, params = _sql("card_transactions", "delete", {"tx_id": 5})
    assert sql == "DELETE FROM dbo.card_transactions WHERE tx_id = %s"
    assert params == (5,)
