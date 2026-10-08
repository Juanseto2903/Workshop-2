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


def setup(context=None) -> dict:
    """Create or update data source, suites, validation definitions and checkpoints."""
    import great_expectations as gx
    from great_expectations.checkpoint import UpdateDataDocsAction

    context = context or get_context()
    datasource = context.data_sources.add_or_update_pandas(name=DATASOURCE)
    checkpoints = {}
    for gate in GATES.values():
        for dataset in gate.datasets:
            asset = _get_or_add_asset(datasource, dataset.name)
            batch_definition = _get_or_add_batch_definition(asset, "whole_dataframe")
            suite = context.suites.add_or_update(build_suite(dataset.suite))
            validation = context.validation_definitions.add_or_update(
                gx.ValidationDefinition(name=f"{dataset.name}_validation",
                                        data=batch_definition, suite=suite))
            checkpoints[dataset.name] = context.checkpoints.add_or_update(
                gx.Checkpoint(name=f"{dataset.name}_checkpoint",
                              validation_definitions=[validation],
                              actions=[UpdateDataDocsAction(name="update_data_docs")],
                              result_format={"result_format": "SUMMARY"}))
    return checkpoints


def _flatten(checkpoint_result):
    for validation_result in checkpoint_result.run_results.values():
        yield from validation_result.results


def run_gate(gate_name: str) -> GateOutcome:
    """Run one validation gate. Raises DataQualityError if a Critical rule fails.

    A missing Parquet file raises FileNotFoundError: that is an operational failure
    (transient), not a data-quality failure.
    """
    import pandas as pd

    gate = GATES[gate_name]
    context = get_context()
    checkpoints = setup(context)

    results, datasets = [], {}
    for dataset in gate.datasets:
        path = paths.STAGING_DIR / dataset.parquet
        frame = pd.read_parquet(path)
        datasets[dataset.name] = {"file": os.path.relpath(path, paths.ROOT), "rows": int(len(frame)),
                                  "columns": int(frame.shape[1])}
        checkpoint_result = checkpoints[dataset.name].run(batch_parameters={"dataframe": frame})
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
