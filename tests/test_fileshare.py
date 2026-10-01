from pathlib import Path

from banking_cdc.sources.fileshare import FileShareAdapter, LocalDirectoryShare

HEADER = "transfer_id,value_date,booking_ts,debtor_iban,creditor_iban,amount,currency,reference\n"


def _iban(nib19):
    from banking_cdc.generator import _pt_iban

    return _pt_iban(nib19)


A = _iban("0033000000000000001")
B = _iban("0035000000000000002")


def _row(tid, amount="100.00", debtor=None, creditor=None):
    ts = "2026-10-01T09:00:00+00:00"
    return f"{tid},2026-10-01,{ts},{debtor or A},{creditor or B},{amount},EUR,REF\n"


def _adapter(tmp_path: Path) -> FileShareAdapter:
    return FileShareAdapter(LocalDirectoryShare(tmp_path), share_name="partner-drop")


def test_new_file_becomes_one_insert_event_per_row(tmp_path):
    (tmp_path / "f1.csv").write_text(HEADER + _row("T1") + _row("T2"))
    result = _adapter(tmp_path).capture(None, "run-1")
    assert [e.op for e in result.events] == ["c", "c"]
    ev = result.events[0]
    assert ev.key == {"transfer_id": "T1"}
    assert ev.after["amount"] == "100.00"
    assert ev.source.system == "azure_fileshare"
    assert ev.source.position.startswith("f1.csv:2@")
    assert result.manifest[0]["rows"] == 2


def test_processed_file_is_not_captured_again(tmp_path):
    (tmp_path / "f1.csv").write_text(HEADER + _row("T1"))
    adapter = _adapter(tmp_path)
    first = adapter.capture(None, "r1")
    second = adapter.capture(first.new_watermark, "r2")
    assert second.events == []
    assert second.new_watermark == first.new_watermark


def test_resent_file_with_new_content_is_an_update(tmp_path):
    f = tmp_path / "f1.csv"
    f.write_text(HEADER + _row("T1"))
    adapter = _adapter(tmp_path)
    first = adapter.capture(None, "r1")
    f.write_text(HEADER + _row("T1", amount="150.00"))
    second = adapter.capture(first.new_watermark, "r2")
    assert [e.op for e in second.events] == ["u"]
    assert second.events[0].event_id != first.events[0].event_id


def test_bad_rows_are_rejected_not_dropped(tmp_path):
    (tmp_path / "f1.csv").write_text(
        HEADER
        + _row("T1")
        + _row("T2", amount="abc")
        + _row("T3", debtor="PT50000000000000000000000")
    )
    result = _adapter(tmp_path).capture(None, "r")
    assert [e.key["transfer_id"] for e in result.events] == ["T1"]
    assert sorted(r.reason for r in result.rejects) == ["invalid amount", "invalid debtor_iban"]
    assert result.manifest[0]["rejected"] == 2


def test_non_csv_files_are_ignored(tmp_path):
    (tmp_path / "readme.txt").write_text("hello")
    assert _adapter(tmp_path).capture(None, "r").events == []


def test_source_stats_counts_valid_rows_in_all_files(tmp_path):
    (tmp_path / "f1.csv").write_text(HEADER + _row("T1") + _row("T2", amount="x"))
    (tmp_path / "f2.csv").write_text(HEADER + _row("T3"))
    stats = _adapter(tmp_path).source_stats()
    assert stats["partner_transfers"]["rows"] == 2
