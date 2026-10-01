from banking_cdc.evaluate import percentile, score

TRUTH = [
    {"rule": "card_velocity", "account_id": 1, "tx_ids": [1, 2, 3, 4, 5]},
    {"rule": "card_velocity", "account_id": 2, "tx_ids": [10, 11, 12, 13, 14]},
    {"rule": "structuring", "account_id": 3, "transfer_ids": ["TRF1", "TRF2", "TRF3"]},
]


def test_precision_and_recall_per_rule():
    alerts = [
        {"rule": "card_velocity", "account_id": 1, "evidence_keys": ["1", "2", "3", "4", "5"]},
        {"rule": "card_velocity", "account_id": 9, "evidence_keys": ["90", "91"]},
        {"rule": "structuring", "account_id": 3, "evidence_keys": ["TRF2", "TRF3", "TRF9"]},
    ]
    s = score(alerts, TRUTH)
    assert s["card_velocity"] == {
        "alerts": 2,
        "true_positives": 1,
        "false_positives": 1,
        "planted": 2,
        "caught": 1,
        "precision": 0.5,
        "recall": 0.5,
    }
    assert s["structuring"]["precision"] == 1.0
    assert s["structuring"]["recall"] == 1.0


def test_rule_with_no_alerts_has_zero_recall_and_no_precision():
    s = score([], TRUTH)
    assert s["card_velocity"]["recall"] == 0.0
    assert s["card_velocity"]["precision"] is None


def test_percentile_interpolates():
    assert percentile([1, 2, 3, 4], 0.5) == 2.5
    assert percentile([], 0.5) is None
