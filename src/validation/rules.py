"""Data-quality rules DQ-01..DQ-12 (Section 5 of the design document).

This module is the single source of truth. The Great Expectations suites, the
Expectation -> Rule ID mapping and the evaluation of severities are all derived
from RULES, so the code, the mapping table and the document cannot drift apart.
It deliberately does not import Great Expectations.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

CRITICAL = "Critical"
WARNING = "Warning"
INFORMATIONAL = "Informational"

# --------------------------------------------------------------------------- #
# Contract values
# --------------------------------------------------------------------------- #
SPOTIFY_REQUIRED_COLUMNS = [
    "track_id", "track_name", "album_name", "artists", "track_genre",
    "popularity", "duration_ms", "explicit", "danceability", "energy",
    "valence", "acousticness", "loudness", "tempo",
]  # the 14 required columns of DQ-01
GRAMMY_REQUIRED_COLUMNS = ["year", "category", "artist", "winner"]  # DQ-06

SPOTIFY_MIN_ROWS = 102_600   # 90% of 114,000 (114 genres x 1,000 rows)
GRAMMY_MIN_ROWS = 4_329      # 90% of 4,810 reconciled rows

PREPARED_TRACK_COLUMNS = [
    "track_id", "track_name", "album_name", "popularity", "duration_ms",
    "danceability", "energy", "valence", "acousticness", "loudness", "tempo",
    "explicit_flag", "track_count", "recognition_group",
    "artist_grammy_wins_range", "popularity_tier", "is_grammy_track",
]
PREPARED_MATCH_COLUMNS = ["artist_key", "matched_flag"]

# !! These literals must match the seed rows of Dim_Grammy_Recognition and
# !! Dim_Popularity_Tier in sql/dw_schema.sql. Verify with:
# !!     python -m src.validation check-contract
RECOGNITION_PAIRS = [
    ("No-Grammy", "0"),
    ("Grammy", "1"),
    ("Grammy", "2-4"),
    ("Grammy", "5+"),
]
POPULARITY_TIERS = ["Most popular", "Rest"]

_FEATURES = ["danceability", "energy", "valence", "acousticness"]


# --------------------------------------------------------------------------- #
# Structures
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Check:
    """One Great Expectations expectation (class name + kwargs)."""
    expectation: str
    kwargs: dict


@dataclass(frozen=True)
class Rule:
    rule_id: str
    suite: str
    severity: str
    dimension: str
    description: str
    threshold: str
    requirement: str
    checks: tuple


@dataclass(frozen=True)
class Dataset:
    name: str       # GE asset name; checkpoint = f"{name}_checkpoint"
    parquet: str    # file inside the staging directory
    suite: str


@dataclass(frozen=True)
class Gate:
    name: str
    dag_task: str
    datasets: tuple


GATES: dict[str, Gate] = {
    "spotify_raw": Gate("spotify_raw", "validate_spotify_raw", (
        Dataset("spotify_raw", "spotify_raw.parquet", "spotify_raw_suite"),)),
    "grammys_raw": Gate("grammys_raw", "validate_grammys_raw", (
        Dataset("grammys_raw", "grammys_raw.parquet", "grammys_raw_suite"),)),
    "prepared": Gate("prepared", "validate_prepared", (
        Dataset("prepared_tracks", "prepared_tracks.parquet", "prepared_tracks_suite"),
        Dataset("grammy_artist_match", "grammy_artist_match.parquet",
                "grammy_artist_match_suite"),)),
}


def _kw(**kwargs: Any) -> dict:
    return {k: v for k, v in kwargs.items() if v is not None}


def _between(column, min_value=None, max_value=None, **extra) -> Check:
    return Check("ExpectColumnValuesToBeBetween",
                 _kw(column=column, min_value=min_value, max_value=max_value, **extra))


def _not_null(column, mostly=None) -> Check:
    return Check("ExpectColumnValuesToNotBeNull", _kw(column=column, mostly=mostly))


def _in_set(column, values) -> Check:
    return Check("ExpectColumnValuesToBeInSet", {"column": column, "value_set": list(values)})


_PREPARED_NOT_NULL = [
    "popularity", "duration_ms", *_FEATURES, "explicit_flag", "track_count",
    "recognition_group", "artist_grammy_wins_range", "popularity_tier",
]

# --------------------------------------------------------------------------- #
# The 12 rules (thresholds exactly as in Tables 13 and 14 of the document)
# --------------------------------------------------------------------------- #
RULES: tuple[Rule, ...] = (
    # ---- Gate: Spotify raw -------------------------------------------------
    Rule("DQ-01", "spotify_raw_suite", CRITICAL, "Validity, completeness",
         "The required columns exist and the extraction is not truncated.",
         "All 14 columns present; rows of at least 102,600.", "AR-01 to 03", (
             Check("ExpectTableColumnsToMatchSet",
                   {"column_set": SPOTIFY_REQUIRED_COLUMNS, "exact_match": False}),
             Check("ExpectTableRowCountToBeBetween", {"min_value": SPOTIFY_MIN_ROWS}),
         )),
    Rule("DQ-02", "spotify_raw_suite", CRITICAL, "Validity",
         "Numeric attributes respect the documented Spotify scales.",
         "popularity within 0-100 and the four features within 0-1, in 100% of rows.",
         "AR-01, AR-02", (
             _between("popularity", 0, 100),
             *[_between(c, 0, 1) for c in _FEATURES],
         )),
    Rule("DQ-03", "spotify_raw_suite", WARNING, "Completeness",
         "The artist is present.", "At least 99.9% non-null.", "AR-01 to 03",
         (_not_null("artists", mostly=0.999),)),
    Rule("DQ-04", "spotify_raw_suite", WARNING, "Uniqueness",
         "The same track and genre pair is not repeated.",
         "At most 1% of rows duplicated.", "AR-01 to 03", (
             Check("ExpectCompoundColumnsToBeUnique",
                   {"column_list": ["track_id", "track_genre"], "mostly": 0.99}),
         )),
    Rule("DQ-05", "spotify_raw_suite", WARNING, "Validity",
         "Popularity zero is not dominant.", "At most 20% of rows equal 0.", "AR-01", (
             Check("ExpectColumnValuesToNotBeInSet",
                   {"column": "popularity", "value_set": [0], "mostly": 0.80}),
         )),
    # ---- Gate: Grammy raw --------------------------------------------------
    Rule("DQ-06", "grammys_raw_suite", CRITICAL, "Validity, completeness",
         "The required columns exist and the extraction is complete.",
         "All 4 columns present; rows of at least 4,329.", "AR-01 to 03", (
             Check("ExpectTableColumnsToMatchSet",
                   {"column_set": GRAMMY_REQUIRED_COLUMNS, "exact_match": False}),
             Check("ExpectTableRowCountToBeBetween", {"min_value": GRAMMY_MIN_ROWS}),
         )),
    Rule("DQ-07", "grammys_raw_suite", CRITICAL, "Validity",
         "Every record represents a Grammy win.", "100% TRUE.", "AR-01 to 03", (
             _not_null("winner"),
             _in_set("winner", [True]),
         )),
    Rule("DQ-08", "grammys_raw_suite", WARNING, "Completeness",
         "The artist is present for enough records.", "At least 50% non-null.",
         "AR-01 to 03", (_not_null("artist", mostly=0.5),)),
    # ---- Gate: prepared (tracks) -------------------------------------------
    Rule("DQ-09", "prepared_tracks_suite", CRITICAL, "Uniqueness",
         "There is one row per track.", "100% unique.", "AR-01 to 03", (
             _not_null("track_id"),
             Check("ExpectColumnValuesToBeUnique", {"column": "track_id"}),
         )),
    Rule("DQ-10", "prepared_tracks_suite", CRITICAL, "Validity, consistency",
         "Required attributes exist, values respect the Data Warehouse domains, "
         "and the classification pair is one of the four valid combinations.",
         "100% of attributes present; 100% of rows within domains.", "AR-01 to 03", (
             Check("ExpectTableColumnsToMatchSet",
                   {"column_set": PREPARED_TRACK_COLUMNS, "exact_match": False}),
             *[_not_null(c) for c in _PREPARED_NOT_NULL],
             _between("popularity", 0, 100),
             *[_between(c, 0, 1) for c in _FEATURES],
             _between("duration_ms", min_value=1),
             _between("tempo", min_value=0, strict_min=True),
             _in_set("explicit_flag", [0, 1]),
             _in_set("track_count", [1]),
             _in_set("popularity_tier", POPULARITY_TIERS),
             Check("ExpectColumnPairValuesToBeInSet", {
                 "column_A": "recognition_group",
                 "column_B": "artist_grammy_wins_range",
                 "value_pairs_set": [list(p) for p in RECOGNITION_PAIRS]}),
         )),
    Rule("DQ-11", "prepared_tracks_suite", WARNING, "Completeness (analytical readiness)",
         "The Grammy-recognized group is large enough to compare.",
         "At least 1,000 tracks and 2% of tracks.", "AR-01 to 03", (
             Check("ExpectColumnSumToBeBetween",
                   {"column": "is_grammy_track", "min_value": 1000}),
             Check("ExpectColumnMeanToBeBetween",
                   {"column": "is_grammy_track", "min_value": 0.02}),
         )),
    # ---- Gate: prepared (Grammy-artist match report) -----------------------
    Rule("DQ-12", "grammy_artist_match_suite", INFORMATIONAL, "Consistency",
         "Match coverage between the sources is reported.",
         "Share of Grammy artists matched; no blocking threshold.", "AR-01 to 03", (
             # Always passes by construction: the purpose is to record the observed mean.
             Check("ExpectColumnMeanToBeBetween",
                   {"column": "matched_flag", "min_value": 0, "max_value": 1}),
         )),
)

RULES_BY_ID = {r.rule_id: r for r in RULES}
SUITES = tuple(dict.fromkeys(r.suite for r in RULES))


def rules_for_suite(suite: str) -> list[Rule]:
    return [r for r in RULES if r.suite == suite]


def rules_for_gate(gate: str) -> list[Rule]:
    suites = {d.suite for d in GATES[gate].datasets}
    return [r for r in RULES if r.suite in suites]


def gate_of_rule(rule_id: str) -> str:
    suite = RULES_BY_ID[rule_id].suite
    return next(g.name for g in GATES.values() if suite in {d.suite for d in g.datasets})


# --------------------------------------------------------------------------- #
# Expectation -> Rule ID mapping (generated, never typed by hand)
# --------------------------------------------------------------------------- #
def snake_case(class_name: str) -> str:
    """ExpectColumnValuesToBeBetween -> expect_column_values_to_be_between."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()


def _target(kwargs: dict) -> str:
    if "column" in kwargs:
        return kwargs["column"]
    if "column_list" in kwargs:
        return ", ".join(kwargs["column_list"])
    if "column_A" in kwargs:
        return f'{kwargs["column_A"]} + {kwargs["column_B"]}'
    return "(table)"


def _params(kwargs: dict) -> str:
    parts = []
    for key, value in kwargs.items():
        if key in ("column", "column_list", "column_A", "column_B"):
            continue
        if isinstance(value, list) and len(value) > 6:
            value = f"[{len(value)} values]"
        parts.append(f"{key}={value}")
    return "; ".join(parts)


def mapping_rows(grouped: bool = False) -> list[dict]:
    """One row per expectation, or (grouped=True) per rule + expectation + parameters."""
    rows = []
    for rule in RULES:
        for check in rule.checks:
            rows.append({
                "rule_id": rule.rule_id,
                "severity": rule.severity,
                "gate": gate_of_rule(rule.rule_id),
                "suite": rule.suite,
                "expectation_type": snake_case(check.expectation),
                "target": _target(check.kwargs),
                "parameters": _params(check.kwargs),
                "dimension": rule.dimension,
                "requirement": rule.requirement,
            })
    if not grouped:
        return rows
    merged: dict[tuple, dict] = {}
    for row in rows:
        key = (row["rule_id"], row["expectation_type"], row["parameters"])
        if key in merged:
            merged[key]["target"] += ", " + row["target"]
        else:
            merged[key] = dict(row)
    return list(merged.values())
