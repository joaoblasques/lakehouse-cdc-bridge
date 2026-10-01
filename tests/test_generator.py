import csv
import io
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from banking_cdc.generator import BankSimulator, iban_is_valid

START = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)


def _round(seed=42, n=1):
    sim = BankSimulator(seed=seed, start=START, n_customers=40)
    sim.initial_load()
    return sim, [sim.next_round() for _ in range(n)]


def test_same_seed_same_data():
    _, a = _round(seed=7)
    _, b = _round(seed=7)
    assert a[0].sql_ops == b[0].sql_ops
    assert a[0].files == b[0].files


def test_initial_load_creates_valid_portuguese_ibans():
    sim = BankSimulator(seed=1, start=START, n_customers=10)
    batch = sim.initial_load()
    accounts = [row for table, op, row in batch.sql_ops if table == "accounts"]
    assert len(accounts) >= 10
    assert all(a["iban"].startswith("PT50") and iban_is_valid(a["iban"]) for a in accounts)


def test_second_round_contains_inserts_updates_and_deletes():
    _, (_, batch) = _round(n=2)
    ops = {(table, op) for table, op, _ in batch.sql_ops}
    assert ("card_transactions", "insert") in ops
    assert ("card_transactions", "update") in ops
    assert ("card_transactions", "delete") in ops
    assert ("accounts", "update") in ops


def test_deletes_only_hit_expired_pending_authorisations():
    sim, batches = _round(n=3)
    for batch in batches:
        for table, op, row in batch.sql_ops:
            if table == "card_transactions" and op == "delete":
                assert sim.deleted_tx_status[row["tx_id"]] == "AUTHORISED"


def test_velocity_fraud_is_injected_and_labelled():
    _, batches = _round(n=4)
    labels = [g for b in batches for g in b.ground_truth if g["rule"] == "card_velocity"]
    assert labels
    label = labels[0]
    tx = [
        row
        for b in batches
        for table, op, row in b.sql_ops
        if table == "card_transactions" and op == "insert" and row["tx_id"] in label["tx_ids"]
    ]
    assert len(tx) >= 5
    times = sorted(t["tx_ts"] for t in tx)
    assert times[-1] - times[0] <= timedelta(minutes=10)


def test_impossible_travel_fraud_uses_two_countries_within_an_hour():
    _, batches = _round(n=4)
    label = next(g for b in batches for g in b.ground_truth if g["rule"] == "impossible_travel")
    tx = [
        row
        for b in batches
        for table, op, row in b.sql_ops
        if table == "card_transactions" and op == "insert" and row["tx_id"] in label["tx_ids"]
    ]
    assert len({t["country"] for t in tx}) == 2
    assert abs(tx[0]["tx_ts"] - tx[1]["tx_ts"]) <= timedelta(minutes=60)


def test_partner_file_is_csv_with_structuring_pattern():
    _, batches = _round(n=4)
    files = [f for b in batches for f in b.files]
    assert files
    rows = [r for _, text in files for r in csv.DictReader(io.StringIO(text))]
    assert {"transfer_id", "debtor_iban", "creditor_iban", "amount", "booking_ts"} <= set(rows[0])
    label = next(g for b in batches for g in b.ground_truth if g["rule"] == "structuring")
    flagged = [r for r in rows if r["transfer_id"] in label["transfer_ids"]]
    assert len(flagged) >= 3
    assert all(Decimal("9000") <= Decimal(r["amount"]) < Decimal("10000") for r in flagged)


def test_some_partner_files_carry_a_malformed_amount():
    _, batches = _round(n=6)
    amounts = [
        r["amount"]
        for b in batches
        for _, text in b.files
        for r in csv.DictReader(io.StringIO(text))
    ]
    assert "1.250,00" in amounts


def test_travellers_use_one_foreign_country_for_the_whole_trip():
    sim, batches = _round(n=5)
    planted = {t for b in batches for g in b.ground_truth for t in g.get("tx_ids", [])}
    countries: dict[int, set[str]] = {}
    for b in batches:
        for table, op, row in b.sql_ops:
            if table == "card_transactions" and op == "insert" and row["tx_id"] not in planted:
                countries.setdefault(row["account_id"], set()).add(row["country"])
    foreign = [c for c in countries.values() if c - {"PT"}]
    assert foreign, "some cardholders should travel"
    assert all(len(c - {"PT"}) == 1 for c in foreign)
