"""Command line.

    python -m src.validation setup                 create/update the GE objects
    python -m src.validation run spotify_raw       run one gate (or: grammys_raw, prepared, all)
    python -m src.validation mapping               write the Expectation -> Rule ID mapping
    python -m src.validation check-contract        check literals against sql/dw_schema.sql
Exit code 1 when a Critical rule fails (the Airflow task must fail).
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

from .. import paths
from .rules import GATES, POPULARITY_TIERS, RECOGNITION_PAIRS, mapping_rows


def _mapping() -> int:
    paths.EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out = paths.EVIDENCE_DIR / "expectation_rule_mapping.csv"
    rows = mapping_rows(grouped=False)
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} expectations mapped to {len({r['rule_id'] for r in rows})} rules -> {out}")
    return 0


def _check_contract() -> int:
    if not paths.DW_SCHEMA_SQL.exists():
        print(f"Not found: {paths.DW_SCHEMA_SQL}")
        return 1
    sql = paths.DW_SCHEMA_SQL.read_text(encoding="utf-8")
    literals = sorted({v for pair in RECOGNITION_PAIRS for v in pair} | set(POPULARITY_TIERS))
    missing = [v for v in literals if f"'{v}'" not in sql]
    for v in literals:
        print(f"  [{'MISSING' if v in missing else 'ok'}] '{v}'")
    if missing:
        print("\nEdit RECOGNITION_PAIRS / POPULARITY_TIERS in src/validation/rules.py so they "
              "match the seed values of sql/dw_schema.sql.")
        return 1
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(prog="python -m src.validation")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup")
    run = sub.add_parser("run")
    run.add_argument("gate", choices=[*GATES, "all"])
    run.add_argument("--staging-dir", help="read the Parquet files from this folder instead of data/staging")
    sub.add_parser("mapping")
    sub.add_parser("check-contract")
    
    args = parser.parse_args(argv)
    if args.command == "run" and args.staging_dir:
        paths.STAGING_DIR = Path(args.staging_dir).resolve()

    if args.command == "mapping":
        return _mapping()
    if args.command == "check-contract":
        return _check_contract()

    from .evaluate import DataQualityError   # GE is imported lazily from here on
    from . import gates
    if args.command == "setup":
        gates.setup()
        print("Great Expectations objects created/updated.")
        return 0
    code = 0
    for name in (GATES if args.gate == "all" else [args.gate]):
        try:
            gates.run_gate(name)
        except DataQualityError as exc:
            print(exc, file=sys.stderr)
            code = 1
    return code


if __name__ == "__main__":
    sys.exit(main())
