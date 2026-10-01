"""uv run python -m banking_cdc.onboarding --ddl table.sql --source db2_core

Writes conf/proposals/<source>__<table>.yml. A person reviews it, copies the entry into
conf/sources.yml and opens a pull request: that pull request is the approval.
Exit code 1 when the validator or the model found a blocker (the file is still written).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from banking_cdc.onboarding.assistant import build_prompt, draft_entry, make_client
from banking_cdc.onboarding.ddl import parse_ddl
from banking_cdc.onboarding.review import build_proposal, validate
from banking_cdc.pipeline.config import CONF, load_config

PROPOSALS = CONF.parent / "proposals"


def main(argv: list[str] | None = None, client_factory=make_client) -> int:
    parser = argparse.ArgumentParser(prog="banking_cdc.onboarding", description=__doc__)
    parser.add_argument("--ddl", required=True, type=Path, help="file with one CREATE TABLE")
    parser.add_argument("--source", required=True, help="source name in conf/sources.yml")
    parser.add_argument("--config", type=Path, default=CONF)
    parser.add_argument("--out", type=Path, default=PROPOSALS)
    parser.add_argument("--dry-run", action="store_true", help="print the prompt; no model call")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    if args.source not in cfg["sources"]:
        parser.error(f"unknown source {args.source!r}; known: {', '.join(cfg['sources'])}")
    source_type = cfg["sources"][args.source]["type"]
    ddl = args.ddl.read_text()
    table = parse_ddl(ddl)

    if args.dry_run:
        print(build_prompt(table, ddl, source_type, cfg))
        return 0

    draft, provenance = draft_entry(client_factory(), table, ddl, source_type, cfg)
    findings = validate(draft, table, source_type, cfg)
    proposal = build_proposal(draft, findings, table, ddl, args.source, source_type, provenance)

    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{args.source}__{table.qualified}.yml"
    path.write_text(yaml.safe_dump(proposal, sort_keys=False, allow_unicode=True, width=100))
    print(f"{proposal['status']}: {path}")
    for f in findings:
        print(f"  {f.level}: {f.message}")
    for n in draft["review_notes"]:
        print(f"  model {n['severity']}: {n['note']}")
    return 0 if proposal["status"] == "ready_for_review" else 1
