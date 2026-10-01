"""Score Gold alerts against the simulator's planted fraud (ground truth)."""

from __future__ import annotations

from typing import Any


def _evidence(label: dict[str, Any]) -> set[str]:
    return {str(x) for x in label.get("tx_ids", []) + label.get("transfer_ids", [])}


def score(alerts: list[dict[str, Any]], truth: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """An alert is a true positive when it shares evidence with a planted case of the same rule.

    precision = true alerts / alerts raised; recall = planted cases caught / planted cases.
    """
    out = {}
    for rule in sorted({a["rule"] for a in alerts} | {t["rule"] for t in truth}):
        rule_alerts = [a for a in alerts if a["rule"] == rule]
        rule_truth = [t for t in truth if t["rule"] == rule]
        hits = [
            a
            for a in rule_alerts
            if any(set(map(str, a["evidence_keys"])) & _evidence(t) for t in rule_truth)
        ]
        caught = [
            t
            for t in rule_truth
            if any(set(map(str, a["evidence_keys"])) & _evidence(t) for a in rule_alerts)
        ]
        out[rule] = {
            "alerts": len(rule_alerts),
            "true_positives": len(hits),
            "false_positives": len(rule_alerts) - len(hits),
            "planted": len(rule_truth),
            "caught": len(caught),
            "precision": round(len(hits) / len(rule_alerts), 3) if rule_alerts else None,
            "recall": round(len(caught) / len(rule_truth), 3) if rule_truth else None,
        }
    return out


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 2)
