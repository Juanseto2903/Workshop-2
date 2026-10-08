"""Tests for the pure-Python part of the validation layer (no Great Expectations needed)."""
from types import SimpleNamespace as NS

from src.validation import evaluate as ev
from src.validation import rules as r


def test_twelve_rules_with_documented_severities():
    assert [x.rule_id for x in r.RULES] == [f"DQ-{i:02d}" for i in range(1, 13)]
    counts = {s: sum(x.severity == s for x in r.RULES)
              for s in (r.CRITICAL, r.WARNING, r.INFORMATIONAL)}
    assert counts == {r.CRITICAL: 6, r.WARNING: 5, r.INFORMATIONAL: 1}


def test_gate_assignment_matches_section_5():
    ids = lambda g: [x.rule_id for x in r.rules_for_gate(g)]  # noqa: E731
    assert ids("spotify_raw") == [f"DQ-0{i}" for i in range(1, 6)]
    assert ids("grammys_raw") == ["DQ-06", "DQ-07", "DQ-08"]
    assert ids("prepared") == ["DQ-09", "DQ-10", "DQ-11", "DQ-12"]


def test_every_rule_has_checks_and_valid_gate():
    for rule in r.RULES:
        assert rule.checks, rule.rule_id
        assert r.gate_of_rule(rule.rule_id) in r.GATES


def test_spotify_contract_has_14_columns():
    assert len(r.SPOTIFY_REQUIRED_COLUMNS) == 14
    assert r.SPOTIFY_MIN_ROWS == round(0.9 * 114_000)
    assert r.GRAMMY_MIN_ROWS == round(0.9 * 4_810)


def test_mapping_is_complete_and_snake_case():
    rows = r.mapping_rows()
    assert {x["rule_id"] for x in rows} == {x.rule_id for x in r.RULES}
    assert all(x["expectation_type"].startswith("expect_") for x in rows)
    assert len(r.mapping_rows(grouped=True)) < len(rows)
    assert r.snake_case("ExpectColumnValuesToBeBetween") == "expect_column_values_to_be_between"


# ---- evaluation with fake GE results ------------------------------------- #
def _result(rule_id, success=True, etype="expect_x", result=None, error=None):
    meta = {"rule_id": rule_id} if rule_id else {}
    info = {"k": {"raised_exception": True, "exception_message": error}} if error else {}
    return NS(success=success, result=result or {}, exception_info=info,
              expectation_config=NS(type=etype, kwargs={"column": "c"}, meta=meta))


def _all_pass(gate):
    return [_result(rule.rule_id) for rule in r.rules_for_gate(gate) for _ in rule.checks]


def test_all_pass():
    out = ev.evaluate_gate("spotify_raw", _all_pass("spotify_raw"))
    assert out.passed and not out.warnings


def test_critical_failure_blocks():
    results = _all_pass("spotify_raw")
    results[0] = _result("DQ-01", success=False)
    out = ev.evaluate_gate("spotify_raw", results)
    assert not out.passed and [x.rule_id for x in out.blocking] == ["DQ-01"]


def test_warning_failure_does_not_block():
    results = [x for x in _all_pass("spotify_raw") if x.expectation_config.meta["rule_id"] != "DQ-03"]
    results.append(_result("DQ-03", success=False, result={"unexpected_percent": 0.5}))
    out = ev.evaluate_gate("spotify_raw", results)
    assert out.passed and [x.rule_id for x in out.warnings] == ["DQ-03"]


def test_missing_critical_rule_is_not_evaluated_and_blocks():
    results = [x for x in _all_pass("grammys_raw") if x.expectation_config.meta["rule_id"] != "DQ-07"]
    out = ev.evaluate_gate("grammys_raw", results)
    assert [x.rule_id for x in out.blocking] == ["DQ-07"]
    assert out.blocking[0].status == ev.NOT_EVALUATED


def test_incomplete_rule_fails():
    results = _all_pass("spotify_raw")
    results.pop(0)  # DQ-01 has 2 expectations; only 1 reported
    assert [x.rule_id for x in ev.evaluate_gate("spotify_raw", results).blocking] == ["DQ-01"]


def test_expectation_exception_counts_as_failure():
    results = _all_pass("grammys_raw")
    results[0] = _result("DQ-06", success=True, error="column missing")
    assert [x.rule_id for x in ev.evaluate_gate("grammys_raw", results).blocking] == ["DQ-06"]


def test_unmapped_failure_is_critical():
    results = _all_pass("grammys_raw") + [_result(None, success=False)]
    out = ev.evaluate_gate("grammys_raw", results)
    assert [x.rule_id for x in out.blocking] == ["UNMAPPED"]


def test_informational_never_blocks():
    results = [x for x in _all_pass("prepared") if x.expectation_config.meta["rule_id"] != "DQ-12"]
    results.append(_result("DQ-12", success=False))
    assert ev.evaluate_gate("prepared", results).passed


def test_evidence_written(tmp_path):
    out = ev.evaluate_gate("spotify_raw", _all_pass("spotify_raw"), {"spotify_raw": {"rows": 1}})
    latest = ev.write_evidence(out, tmp_path)
    assert latest.exists() and list((tmp_path / "history").glob("spotify_raw_*.json"))
    assert '"gate_passed": true' in latest.read_text()
