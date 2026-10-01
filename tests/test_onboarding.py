"""AI source onboarding: DDL parsing, the model call, the validator and the proposal file.

The model is replaced by a fake client. These tests prove what the code does with any answer
the model gives; they do not judge the model's answers (that is a manual review step).
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from banking_cdc.onboarding.assistant import (
    MODEL,
    AssistantError,
    AssistantRefused,
    build_prompt,
    draft_entry,
)
from banking_cdc.onboarding.cli import main
from banking_cdc.onboarding.ddl import parse_ddl, spark_type
from banking_cdc.onboarding.review import build_proposal, validate
from banking_cdc.pipeline.config import load_config

DDL = Path(__file__).parent / "fixtures" / "ddl"


@pytest.fixture
def deposits():
    return parse_ddl((DDL / "db2_deposits.sql").read_text())


@pytest.fixture
def merchants():
    return parse_ddl((DDL / "sqlserver_merchants.sql").read_text())


def good_deposits_draft(**overrides):
    draft = {
        "table_summary": "Term deposits held by customers.",
        "primary_key": ["DEPOSIT_ID"],
        "ts_column": "ROW_CHANGED",
        "topic": "banking.cdc.core.deposits",
        "silver_table": "silver_deposits",
        "pii_columns": [
            {"column": "HOLDER_NAME", "category": "name", "handling": "mask"},
            {"column": "HOLDER_NIF", "category": "tax_id", "handling": "hash"},
            {"column": "IBAN", "category": "account_number", "handling": "keep_restricted"},
        ],
        "review_notes": [
            {"severity": "warning", "note": "BONUS_RATE is DOUBLE; rates should be DECIMAL."}
        ],
    }
    return draft | overrides


# --- DDL parsing ------------------------------------------------------------------------------


def test_parses_db2_ddl_with_quoted_names_and_table_constraint(deposits):
    assert (deposits.schema, deposits.table) == ("CORE", "DEPOSITS")
    assert deposits.primary_key == ["DEPOSIT_ID"]
    names = [c.name for c in deposits.columns]
    assert names[:3] == ["DEPOSIT_ID", "CUSTOMER_ID", "HOLDER_NAME"]
    assert len(names) == 12
    assert deposits.column("PRINCIPAL").sql_type == "DECIMAL(18,2)"
    assert deposits.column("DEPOSIT_ID").nullable is False
    assert deposits.column("HOLDER_NAME").nullable is True


def test_detects_db2_row_change_timestamp(deposits):
    assert deposits.row_change_column == "ROW_CHANGED"
    assert deposits.column("OPENED_ON").is_row_change_timestamp is False


def test_parses_sql_server_ddl_with_brackets_and_inline_primary_key(merchants):
    assert (merchants.schema, merchants.table) == ("dbo", "merchants")
    assert merchants.primary_key == ["merchant_id"]
    assert merchants.row_change_column is None
    assert merchants.column("notes").sql_type == "NVARCHAR(MAX)"


def test_table_without_primary_key_parses_with_empty_key():
    t = parse_ddl("CREATE TABLE audit_log (event_ts TIMESTAMP, message VARCHAR(200));")
    assert (t.schema, t.table) == (None, "audit_log")
    assert t.primary_key == []


def test_composite_primary_key():
    t = parse_ddl(
        "CREATE TABLE s.fx (ccy CHAR(3), day DATE, rate DECIMAL(12,6), PRIMARY KEY (ccy, day))"
    )
    assert t.primary_key == ["ccy", "day"]


def test_rejects_text_that_is_not_a_create_table():
    with pytest.raises(ValueError, match="CREATE TABLE"):
        parse_ddl("SELECT 1")


@pytest.mark.parametrize(
    ("sql", "spark"),
    [
        ("INTEGER", "INT"),
        ("SMALLINT", "INT"),
        ("BIGINT", "BIGINT"),
        ("DECIMAL(18,2)", "DECIMAL(18,2)"),
        ("NUMERIC(5,3)", "DECIMAL(5,3)"),
        ("DECIMAL", "DECIMAL(10,0)"),
        ("MONEY", "DECIMAL(19,4)"),
        ("FLOAT", "DOUBLE"),
        ("DOUBLE", "DOUBLE"),
        ("NVARCHAR(MAX)", "STRING"),
        ("CHAR(9)", "STRING"),
        ("UNIQUEIDENTIFIER", "STRING"),
        ("DATE", "DATE"),
        ("DATETIME2(3)", "TIMESTAMP"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("BIT", "BOOLEAN"),
        ("VARBINARY(64)", "BINARY"),
        ("GEOGRAPHY", None),
    ],
)
def test_spark_type_mapping(sql, spark):
    assert spark_type(sql) == spark


# --- the model call ---------------------------------------------------------------------------


class FakeMessages:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def fake_client(payload=None, stop_reason="end_turn", text=None):
    content = [SimpleNamespace(type="text", text=text or json.dumps(payload))]
    response = SimpleNamespace(
        content=content, stop_reason=stop_reason, id="msg_test", model=MODEL, stop_details=None
    )
    messages = FakeMessages(response)
    return SimpleNamespace(beta=SimpleNamespace(messages=messages)), messages


def test_prompt_carries_ddl_source_type_and_existing_conventions(deposits):
    cfg = load_config()
    prompt = build_prompt(deposits, "DDL TEXT", "db2", cfg)
    assert "DDL TEXT" in prompt
    assert "db2" in prompt
    assert "banking.cdc.core.loans" in prompt  # existing topics, so the model follows the pattern
    assert "silver_loans" in prompt


def test_draft_entry_calls_the_model_with_structured_output_and_fallback(deposits):
    client, messages = fake_client(good_deposits_draft())
    draft, meta = draft_entry(client, deposits, "DDL", "db2", load_config())
    assert draft["topic"] == "banking.cdc.core.deposits"
    assert meta == {"model": MODEL, "message_id": "msg_test"}
    call = messages.calls[0]
    assert call["model"] == MODEL
    assert call["fallbacks"] == "default"
    assert call["betas"] == ["server-side-fallback-2026-07-01"]
    assert call["output_config"]["effort"] == "medium"
    fmt = call["output_config"]["format"]
    assert fmt["type"] == "json_schema"
    assert set(fmt["schema"]["required"]) >= {"primary_key", "topic", "pii_columns"}
    assert "tool_choice" not in call and "thinking" not in call


def test_refusal_is_reported_not_parsed(deposits):
    client, _ = fake_client(stop_reason="refusal", text="")
    with pytest.raises(AssistantRefused):
        draft_entry(client, deposits, "DDL", "db2", load_config())


def test_truncated_answer_is_an_error(deposits):
    client, _ = fake_client(stop_reason="max_tokens", text='{"topic": "banki')
    with pytest.raises(AssistantError, match="max_tokens"):
        draft_entry(client, deposits, "DDL", "db2", load_config())


# --- the validator ----------------------------------------------------------------------------


def errors(findings):
    return [f.message for f in findings if f.level == "error"]


def test_good_draft_passes(deposits):
    assert errors(validate(good_deposits_draft(), deposits, "db2", load_config())) == []


def test_primary_key_must_be_real_columns(deposits):
    draft = good_deposits_draft(primary_key=["DEPOSIT_NO"])
    assert any("DEPOSIT_NO" in e for e in errors(validate(draft, deposits, "db2", load_config())))


def test_primary_key_must_match_the_declared_key(deposits):
    draft = good_deposits_draft(primary_key=["CUSTOMER_ID"])
    assert any("declared" in e for e in errors(validate(draft, deposits, "db2", load_config())))


def test_db2_needs_a_timestamp_watermark_column(deposits):
    cfg = load_config()
    assert any(
        "ts_column" in e
        for e in errors(validate(good_deposits_draft(ts_column=""), deposits, "db2", cfg))
    )
    bad = good_deposits_draft(ts_column="OPENED_ON")  # a DATE, not a TIMESTAMP
    assert any("TIMESTAMP" in e for e in errors(validate(bad, deposits, "db2", cfg)))


def test_topic_and_silver_table_follow_conventions_and_are_unique(deposits):
    cfg = load_config()
    for draft, word in [
        (good_deposits_draft(topic="deposits"), "convention"),
        (good_deposits_draft(topic="banking.cdc.core.loans"), "already used"),
        (good_deposits_draft(silver_table="Deposits"), "convention"),
        (good_deposits_draft(silver_table="silver_loans"), "already used"),
    ]:
        assert any(word in e for e in errors(validate(draft, deposits, "db2", cfg))), draft


def test_pii_columns_must_exist(deposits):
    draft = good_deposits_draft(
        pii_columns=[{"column": "EMAIL", "category": "contact", "handling": "mask"}]
    )
    assert any("EMAIL" in e for e in errors(validate(draft, deposits, "db2", load_config())))


def test_unmappable_column_type_is_an_error():
    t = parse_ddl("CREATE TABLE dbo.branches (branch_id INT PRIMARY KEY, location GEOGRAPHY)")
    draft = {
        "table_summary": "",
        "primary_key": ["branch_id"],
        "ts_column": "",
        "topic": "banking.cdc.cards.branches",
        "silver_table": "silver_branches",
        "pii_columns": [],
        "review_notes": [],
    }
    assert any("GEOGRAPHY" in e for e in errors(validate(draft, t, "sqlserver", load_config())))


def test_table_without_any_key_is_an_error_even_if_the_model_invents_one():
    t = parse_ddl("CREATE TABLE dbo.audit_log (event_ts DATETIME2, message NVARCHAR(200))")
    draft = {
        "table_summary": "",
        "primary_key": ["event_ts"],
        "ts_column": "",
        "topic": "banking.cdc.cards.audit_log",
        "silver_table": "silver_audit_log",
        "pii_columns": [],
        "review_notes": [],
    }
    assert any(
        "no primary key" in e for e in errors(validate(draft, t, "sqlserver", load_config()))
    )


# --- the proposal -----------------------------------------------------------------------------


def test_proposal_entry_is_built_from_the_ddl_not_from_the_model(deposits):
    draft = good_deposits_draft()
    findings = validate(draft, deposits, "db2", load_config())
    proposal = build_proposal(
        draft,
        findings,
        deposits,
        "DDL TEXT",
        "db2_core",
        "db2",
        {"model": MODEL, "message_id": "m"},
    )
    entry = proposal["entry"]
    assert entry == {
        "schema": "CORE",
        "table": "DEPOSITS",
        "primary_key": ["DEPOSIT_ID"],
        "ts_column": "ROW_CHANGED",
        "topic": "banking.cdc.core.deposits",
        "silver_table": "silver_deposits",
        "silver_schema": (
            "DEPOSIT_ID INT, CUSTOMER_ID INT, HOLDER_NAME STRING, HOLDER_NIF STRING, "
            "IBAN STRING, PRINCIPAL DECIMAL(18,2), RATE_PCT DECIMAL(5,3), BONUS_RATE DOUBLE, "
            "OPENED_ON DATE, MATURES_ON DATE, STATUS STRING"
        ),
    }
    assert proposal["status"] == "ready_for_review"
    assert proposal["source"] == "db2_core"
    assert proposal["provenance"]["model"] == MODEL
    assert len(proposal["provenance"]["ddl_sha256"]) == 64
    assert proposal["review"]["pii_columns"][1]["column"] == "HOLDER_NIF"


def test_sqlserver_entry_has_no_ts_column(merchants):
    draft = {
        "table_summary": "",
        "primary_key": ["merchant_id"],
        "ts_column": "",
        "topic": "banking.cdc.cards.merchants",
        "silver_table": "silver_merchants",
        "pii_columns": [],
        "review_notes": [],
    }
    findings = validate(draft, merchants, "sqlserver", load_config())
    proposal = build_proposal(draft, findings, merchants, "", "sqlserver_cards", "sqlserver", {})
    assert "ts_column" not in proposal["entry"]
    assert proposal["entry"]["silver_schema"].endswith("onboarded_at TIMESTAMP, notes STRING")


def test_blocker_note_from_the_model_marks_the_proposal_as_needing_changes(deposits):
    draft = good_deposits_draft(review_notes=[{"severity": "blocker", "note": "stop"}])
    findings = validate(draft, deposits, "db2", load_config())
    proposal = build_proposal(draft, findings, deposits, "", "db2_core", "db2", {})
    assert proposal["status"] == "needs_changes"


# --- the command line -------------------------------------------------------------------------


def test_cli_writes_a_proposal_file(tmp_path):
    client, _ = fake_client(good_deposits_draft())
    code = main(
        ["--ddl", str(DDL / "db2_deposits.sql"), "--source", "db2_core", "--out", str(tmp_path)],
        client_factory=lambda: client,
    )
    assert code == 0
    proposal = yaml.safe_load((tmp_path / "db2_core__CORE.DEPOSITS.yml").read_text())
    assert proposal["entry"]["topic"] == "banking.cdc.core.deposits"


def test_cli_exits_non_zero_when_validation_fails(tmp_path):
    client, _ = fake_client(good_deposits_draft(topic="banking.cdc.core.loans"))
    code = main(
        ["--ddl", str(DDL / "db2_deposits.sql"), "--source", "db2_core", "--out", str(tmp_path)],
        client_factory=lambda: client,
    )
    assert code == 1
    assert (tmp_path / "db2_core__CORE.DEPOSITS.yml").exists()  # written, for the reviewer


def test_cli_dry_run_prints_prompt_without_calling_the_model(tmp_path, capsys):
    def no_client():
        raise AssertionError("dry run must not create a client")

    code = main(
        ["--ddl", str(DDL / "db2_deposits.sql"), "--source", "db2_core", "--dry-run"],
        client_factory=no_client,
    )
    assert code == 0
    assert "CORE" in capsys.readouterr().out


def test_cli_rejects_unknown_source(tmp_path):
    with pytest.raises(SystemExit):
        main(["--ddl", str(DDL / "db2_deposits.sql"), "--source", "nope"], client_factory=None)
