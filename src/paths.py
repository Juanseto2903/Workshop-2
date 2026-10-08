"""Project paths. Override the root with WORKSHOP_ROOT (e.g. inside the Airflow container)."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.getenv("WORKSHOP_ROOT") or Path(__file__).resolve().parents[1])
RAW_DIR = ROOT / "data" / "raw"
# Parquet staging area (raw extractions and prepared datasets). Override with STAGING_DIR.
STAGING_DIR = Path(os.getenv("STAGING_DIR") or ROOT / "data" / "staging")
EVIDENCE_DIR = ROOT / "docs" / "evidence" / "validation"
DW_SCHEMA_SQL = ROOT / "sql" / "dw_schema.sql"
