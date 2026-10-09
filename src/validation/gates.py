"""Great Expectations context, validation definitions, checkpoints and gate runner.

Objects (all created idempotently, so re-running never duplicates anything):
  data source        staging_pandas (in-memory DataFrames read from Parquet)
  expectation suites spotify_raw_suite, grammys_raw_suite, prepared_tracks_suite,
                     grammy_artist_match_suite
  validation defs    <dataset>_validation   (one suite + one batch definition)
  checkpoints        <dataset>_checkpoint   (runs the validation, updates Data Docs)
A gate (= one Airflow validation task) runs the checkpoints of its datasets.
"""
from __future__ import annotations

import logging
import os

from .. import paths
from .evaluate import DataQualityError, GateOutcome, evaluate_gate, format_report, write_evidence
from .rules import GATES
from .suites import build_suite

log = logging.getLogger(__name__)
DATASOURCE = "staging_pandas"


def get_context():
    import great_expectations as gx
    return gx.get_context(mode="file", project_root_dir=str(paths.ROOT))


def _get_or_add_asset(datasource, name):
    try:
        return datasource.get_asset(name)
    except (LookupError, KeyError):
        return datasource.add_dataframe_asset(name=name)


def _get_or_add_batch_definition(asset, name):
    try:
        return asset.get_batch_definition(name)
    except (LookupError, KeyError):
        return asset.add_batch_definition_whole_dataframe(name)

def _ensure_suite(context, suite_name):
    """Build the full suite first (with all expectations), then delete any
    stale version from the store and add the fresh one."""
    from .suites import build_suite

    # 1. Construir la suite COMPLETA (con expectativas) primero
    suite = build_suite(suite_name)

    # 2. Borrar cualquier versión vieja
    try:
        context.suites.delete(name=suite_name)
    except Exception:
        pass

    # 3. Guardar la suite completa
    return context.suites.add(suite)


def setup(context=None) -> dict:
    """Create or update data source, suites, validation definitions and checkpoints.

    Suites, validation definitions and checkpoints are recreated from scratch on
    every call (delete-then-add) so that a stale store created in another
    environment cannot break the GX 1.x update path.
    """
    import great_expectations as gx
    from great_expectations.checkpoint import UpdateDataDocsAction

    context = context or get_context()
    datasource = context.data_sources.add_or_update_pandas(name=DATASOURCE)

    checkpoints = {}
    for gate in GATES.values():
        for dataset in gate.datasets:
            # Data source and batch definition
            asset = _get_or_add_asset(datasource, dataset.name)
            batch_definition = _get_or_add_batch_definition(asset, "whole_dataframe")

            # Suite: delete + add
            suite = _ensure_suite(context, dataset.suite)

            # Validation definition: delete + add
            try:
                context.validation_definitions.delete(
                    name=f"{dataset.name}_validation")
            except Exception:
                pass
            validation = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"{dataset.name}_validation",
                    data=batch_definition,
                    suite=suite,
                )
            )

            # Checkpoint: delete + add
            try:
                context.checkpoints.delete(name=f"{dataset.name}_checkpoint")
            except Exception:
                pass
            checkpoints[dataset.name] = context.checkpoints.add(
                gx.Checkpoint(
                    name=f"{dataset.name}_checkpoint",
                    validation_definitions=[validation],
                    actions=[UpdateDataDocsAction(name="update_data_docs")],
                    result_format={"result_format": "SUMMARY"},
                )
            )

    return checkpoints


def _flatten(checkpoint_result):
    for validation_result in checkpoint_result.run_results.values():
        yield from validation_result.results


def run_gate(gate_name: str) -> GateOutcome:
    """Run one validation gate. Raises DataQualityError if a Critical rule fails.

    Assumes setup() has already been called (once per DAG run). A missing Parquet
    file raises FileNotFoundError: operational failure, not data quality.
    """
    import pandas as pd

    gate = GATES[gate_name]
    context = get_context()

    results, datasets = [], {}
    for dataset in gate.datasets:
        path = paths.STAGING_DIR / dataset.parquet
        frame = pd.read_parquet(path)
        datasets[dataset.name] = {
            "file": os.path.relpath(path, paths.ROOT),
            "rows": int(len(frame)),
            "columns": int(frame.shape[1]),
        }

        checkpoint = context.checkpoints.get(name=f"{dataset.name}_checkpoint")
        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": frame})
        results.extend(_flatten(checkpoint_result))

    outcome = evaluate_gate(gate_name, results, datasets)
    write_evidence(outcome, paths.EVIDENCE_DIR)
    report = format_report(outcome)
    if outcome.blocking:
        log.error("\n%s", report)
        raise DataQualityError(
            f"{gate.dag_task}: Critical rule(s) failed: "
            + ", ".join(r.rule_id for r in outcome.blocking))
    log.info("\n%s", report)
    return outcome
