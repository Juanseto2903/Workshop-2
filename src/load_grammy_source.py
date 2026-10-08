"""Carga el CSV original de Grammy a la tabla fuente en PostgreSQL
y genera evidencia de conciliación de filas.

Preparación de la fuente (NO es la carga ETL al Data Warehouse).
Se ejecuta desde la raíz del proyecto con el venv activo:
    python src/load_grammy_source.py
"""
import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

CSV_PATH = ROOT / "data" / "raw" / "the_grammy_awards.csv"
SQL_PATH = ROOT / "sql" / "source_setup.sql"
EVIDENCE_PATH = ROOT / "docs" / "evidence" / "grammy_source_reconciliation.json"

COLUMNS = ("year,title,published_at,updated_at,category,"
           "nominee,artist,workers,img,winner")


def count_csv_rows(path: Path) -> int:
    # csv.reader maneja campos entre comillas con comas o saltos de línea
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # menos el encabezado


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    csv_rows = count_csv_rows(CSV_PATH)

    conn = psycopg2.connect(
        #host=os.getenv("GRAMMY_DB_HOST", "localhost"),
        #port=int(os.getenv("GRAMMY_DB_PORT", "5433")),
        host=os.getenv("WH_HOST", "localhost"),
        port=int(os.getenv("WH_PORT", "5433")),
        user=os.environ["WH_USER"],
        password=os.environ["WH_PASSWORD"],
        dbname="grammy_source",
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(SQL_PATH.read_text(encoding="utf-8"))
            with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
                cur.copy_expert(
                    f"COPY grammy_awards ({COLUMNS}) FROM STDIN "
                    "WITH (FORMAT csv, HEADER true)",
                    f,
                )
            cur.execute("SELECT COUNT(*) FROM grammy_awards")
            db_rows = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM grammy_awards WHERE winner")
            winners = cur.fetchone()[0]
    finally:
        conn.close()

    evidence = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": CSV_PATH.name,
        "source_sha256": sha256(CSV_PATH),
        "csv_data_rows": csv_rows,
        "table_rows": db_rows,
        "winner_true_rows": winners,
        "reconciled": csv_rows == db_rows,
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))

    if csv_rows != db_rows:
        raise SystemExit("ERROR: las filas del CSV no concilian con la tabla")


if __name__ == "__main__":
    main()