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


def test_db2_statements_use_qmark_parameters():
    from banking_cdc.seed import _db2_sql

    sql, params = _db2_sql("loans", "update", {"LOAN_ID": 3, "STATUS": "CLOSED"})
    assert sql == "UPDATE CORE.LOANS SET STATUS = ? WHERE LOAN_ID = ?"
    assert params == ("CLOSED", 3)
    sql, _ = _db2_sql("loans", "delete", {"LOAN_ID": 3})
    assert sql == "DELETE FROM CORE.LOANS WHERE LOAN_ID = ?"
