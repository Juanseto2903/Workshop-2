"""Raw extraction to Parquet staging (no cleaning, no type coercion beyond what the reader infers).

    python -m src.extract spotify     data/raw/spotify_dataset.csv  -> data/staging/spotify_raw.parquet
    python -m src.extract grammys     table grammy_awards (PostgreSQL) -> data/staging/grammys_raw.parquet

The Airflow tasks extract_spotify / extract_grammys (Step 10) call these functions.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd

from . import paths

try:
    from dotenv import load_dotenv
    load_dotenv(paths.ROOT / ".env")
except ImportError:      # dentro del contenedor las variables ya vienen del compose
    pass

log = logging.getLogger(__name__)
SPOTIFY_CSV = paths.RAW_DIR / "spotify_dataset.csv"


def _grammy_source_url() -> str:
    user = os.getenv("WH_USER", "")
    password = os.getenv("WH_PASSWORD", "")
    host = os.getenv("WH_HOST", "localhost")   # PC: localhost | contenedor Airflow: warehouse-db
    port = os.getenv("WH_PORT", "5433")        # PC: 5433 | contenedor Airflow: 5432
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/grammy_source"

def _write(frame: pd.DataFrame, name: str) -> Path:
    paths.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = paths.STAGING_DIR / name
    frame.to_parquet(out, engine="pyarrow", index=False)   # overwrite: safe to re-run
    log.info("%s: %d rows x %d columns -> %s", name, len(frame), frame.shape[1], out)
    return out


def extract_spotify(csv_path: Path | None = None) -> Path:
    path = Path(csv_path or SPOTIFY_CSV)
    if not path.exists():
        raise FileNotFoundError(path)          # operational (transient) failure
    return _write(pd.read_csv(path), "spotify_raw.parquet")


def extract_grammys() -> Path:
    from sqlalchemy import create_engine

    engine = create_engine(_grammy_source_url())
    try:
        frame = pd.read_sql("SELECT * FROM grammy_awards ORDER BY grammy_id", engine)
    finally:
        engine.dispose()
    return _write(frame, "grammys_raw.parquet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    commands = {"spotify": extract_spotify, "grammys": extract_grammys}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        sys.exit("usage: python -m src.extract [spotify|grammys]")
    commands[sys.argv[1]]()
