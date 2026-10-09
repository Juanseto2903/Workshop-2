"""
Reliable batch pipeline: Spotify CSV + Grammy PostgreSQL -> Dimensional DW.

Orchestrates the full workflow with two independent raw branches that converge
at transformation, followed by a prepared gate that gates the DW load.

Step 10 of Workshop 2. TaskFlow API (Airflow 3.1.8).
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

# Project modules (mounted at /opt/airflow/src)
from src import extract, transform, load
from src.validation.gates import setup as gx_setup, run_gate
from src.validation.evaluate import DataQualityError

log = logging.getLogger("airflow.task")

DAG_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="reliable_music_pipeline",
    description="Spotify + Grammy batch pipeline with raw and prepared validation gates.",
    schedule=None,  # manual / external trigger only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
    tags=["workshop", "etl", "great-expectations"],
    doc_md=__doc__,
)
def reliable_music_pipeline():

    @task(retries=0)
    def setup_gx():
        """Create/update GX objects once per DAG run, before any parallel validation."""
        gx_setup()
        log.info("Great Expectations objects are ready.")

    # ----- Spotify branch --------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_spotify():
        """Extract the Spotify CSV to Parquet staging. Retries on transient I/O."""
        path = extract.extract_spotify()
        return {"source": "spotify", "path": str(path)}

    @task(retries=0)
    def validate_spotify_raw():
        """Raw gate for Spotify (DQ-01 to DQ-05). Deterministic: no retry."""
        run_gate("spotify_raw")

    # ----- Grammy branch ---------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def extract_grammys():
        """Extract the Grammy table from PostgreSQL to Parquet staging."""
        path = extract.extract_grammys()
        return {"source": "grammys", "path": str(path)}

    @task(retries=0)
    def validate_grammys_raw():
        """Raw gate for Grammy (DQ-06 to DQ-08). Deterministic: no retry."""
        run_gate("grammys_raw")

    # ----- Convergence -----------------------------------------------------
    @task(retries=0)
    def transform_and_integrate():
        """Clean, harmonize and integrate both sources (TR-01 to TR-13)."""
        summary = transform.run()
        return summary

    @task(retries=0)
    def validate_prepared():
        """Prepared gate (DQ-09 to DQ-12). Gates the load."""
        run_gate("prepared")

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def load_dw():
        """Idempotent load into the dimensional Data Warehouse."""
        summary = load.run()
        return summary

    # ----- Dependencies ----------------------------------------------------
    setup_task = setup_gx()

    spotify_extracted = extract_spotify()
    grammys_extracted = extract_grammys()

    spotify_validated = validate_spotify_raw()
    grammys_validated = validate_grammys_raw()

    prepared = transform_and_integrate()
    prepared_validated = validate_prepared()
    loaded = load_dw()

    # Setup runs first, sequentially, before ANY parallel work
    setup_task >> spotify_extracted >> spotify_validated
    setup_task >> grammys_extracted >> grammys_validated

    [spotify_validated, grammys_validated] >> prepared
    prepared >> prepared_validated >> loaded    

reliable_music_pipeline()