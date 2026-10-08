"""Severity policy and evidence for Great Expectations results.

Great Expectations only reports pass/fail per expectation. The severity policy of
Section 5.2 (Critical blocks, Warning and Informational are recorded) is applied
here, using the rule_id stored in each expectation's ``meta``. The module works on
duck-typed result objects, so it needs no Great Expectations import.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .rules import (CRITICAL, GATES, INFORMATIONAL, RULES_BY_ID, WARNING,
                    rules_for_gate, snake_case)

log = logging.getLogger(__name__)

PASS, FAIL, NOT_EVALUATED = "PASS", "FAIL", "NOT_EVALUATED"
_OBSERVED_KEYS = ("observed_value", "element_count", "unexpected_count",
                  "unexpected_percent", "unexpected_percent_total",
                  "unexpected_percent_nonmissing", "missing_count", "missing_percent")


class DataQualityError(RuntimeError):
    """A Critical rule failed: downstream tasks must not run. Not retryable."""


@dataclass
class CheckOutcome:
    rule_id: str
    expectation: str
    target: str
    success: bool
    observed: dict
    error: str | None = None


@dataclass
class RuleOutcome:
    rule_id: str
    severity: str
    dimension: str
    description: str
    threshold: str
    requirement: str
    status: str
    checks: list = field(default_factory=list)


@dataclass
class GateOutcome:
    gate: str
    dag_task: str
    executed_at: str
    datasets: dict
    rules: list

    @property
    def blocking(self) -> list:
        return [r for r in self.rules if r.severity == CRITICAL and r.status != PASS]

    @property
    def warnings(self) -> list:
        return [r for r in self.rules if r.severity == WARNING and r.status != PASS]

    @property
    def passed(self) -> bool:
        return not self.blocking


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):          # numpy scalars
        try:
            return value.item()
        except Exception:               # noqa: BLE001
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _target(kwargs: dict) -> str:
    if "column" in kwargs:
        return str(kwargs["column"])
    if "column_list" in kwargs:
        return ", ".join(kwargs["column_list"])
    if "column_A" in kwargs:
        return f'{kwargs["column_A"]} + {kwargs["column_B"]}'
    return "(table)"


def _error_message(result: Any) -> str | None:
    info = getattr(result, "exception_info", None) or {}
    candidates = [info] if "raised_exception" in info else list(info.values())
    for item in candidates:
        if isinstance(item, dict) and item.get("raised_exception"):
            return str(item.get("exception_message") or "expectation raised an exception")
    return None


def _to_check(result: Any) -> tuple[str, CheckOutcome]:
    config = result.expectation_config
    meta = getattr(config, "meta", None) or {}
    kwargs = getattr(config, "kwargs", None) or {}
    etype = getattr(config, "type", None) or getattr(config, "expectation_type", "unknown")
    rule_id = meta.get("rule_id") or "UNMAPPED"
    payload = getattr(result, "result", None) or {}
    observed = {k: _jsonable(payload[k]) for k in _OBSERVED_KEYS if k in payload}
    error = _error_message(result)
    return rule_id, CheckOutcome(rule_id, str(etype), _target(kwargs),
                                 bool(result.success) and error is None, observed, error)


def evaluate_gate(gate: str, results: Iterable[Any], datasets: dict | None = None) -> GateOutcome:
    """Group expectation results by rule and apply the severity policy.

    Fail-safe behaviour: a rule of the gate with no results, or with fewer results
    than expectations, counts as not passed; a failing expectation without a
    rule_id is reported as a Critical UNMAPPED rule.
    """
    by_rule: dict[str, list[CheckOutcome]] = {}
    for result in results:
        rule_id, check = _to_check(result)
        by_rule.setdefault(rule_id, []).append(check)

    outcomes = []
    for rule in rules_for_gate(gate):
        checks = by_rule.pop(rule.rule_id, [])
        if not checks:
            status = NOT_EVALUATED
        elif len(checks) < len(rule.checks) or not all(c.success for c in checks):
            status = FAIL
        else:
            status = PASS
        outcomes.append(RuleOutcome(rule.rule_id, rule.severity, rule.dimension,
                                    rule.description, rule.threshold, rule.requirement,
                                    status, checks))
    for rule_id, checks in by_rule.items():   # results not belonging to this gate's rules
        if any(not c.success for c in checks):
            outcomes.append(RuleOutcome(rule_id, CRITICAL, "unknown", "Unmapped expectation",
                                        "", "", FAIL, checks))
    return GateOutcome(gate, GATES[gate].dag_task,
                       dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                       datasets or {}, outcomes)


def format_report(outcome: GateOutcome) -> str:
    lines = [f"Gate {outcome.gate} ({outcome.dag_task}) - {outcome.executed_at}"]
    for r in outcome.rules:
        mark = "PASS" if r.status == PASS else ("WARN" if r.severity != CRITICAL else "FAIL")
        detail = ""
        for c in r.checks:
            if not c.success:
                detail = f"  <- {c.expectation}[{c.target}] {c.error or c.observed}"
                break
        lines.append(f"  [{mark}] {r.rule_id} {r.severity:<13} {r.status}{detail}")
    if outcome.blocking:
        lines.append("  => BLOCKED: " + ", ".join(r.rule_id for r in outcome.blocking))
    else:
        lines.append("  => PASSED" + (f" with {len(outcome.warnings)} warning(s)" if outcome.warnings else ""))
    return "\n".join(lines)


def write_evidence(outcome: GateOutcome, directory: Path) -> Path:
    """Write <gate>_latest.json and a timestamped copy under history/."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "history").mkdir(exist_ok=True)
    doc = _jsonable(asdict(outcome))
    doc["gate_passed"] = outcome.passed
    doc["blocking_rules"] = [r.rule_id for r in outcome.blocking]
    doc["warning_rules"] = [r.rule_id for r in outcome.warnings]
    text = json.dumps(doc, indent=2, ensure_ascii=False)
    stamp = outcome.executed_at.replace(":", "").replace("-", "").replace("+0000", "Z")
    (directory / "history" / f"{outcome.gate}_{stamp}.json").write_text(text, encoding="utf-8")
    latest = directory / f"{outcome.gate}_latest.json"
    latest.write_text(text, encoding="utf-8")
    return latest
