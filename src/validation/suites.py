"""Builds the Great Expectations expectation suites from rules.py."""
from __future__ import annotations

from .rules import RULES, rules_for_suite


def build_expectations(suite_name: str) -> list:
    import great_expectations.expectations as gxe

    expectations = []
    for rule in rules_for_suite(suite_name):
        for check in rule.checks:
            expectation_class = getattr(gxe, check.expectation)
            expectations.append(expectation_class(
                **check.kwargs,
                meta={"rule_id": rule.rule_id, "severity": rule.severity,
                      "dimension": rule.dimension, "requirement": rule.requirement},
            ))
    return expectations


def build_suite(suite_name: str):
    import great_expectations as gx

    suite = gx.ExpectationSuite(name=suite_name)
    for expectation in build_expectations(suite_name):
        suite.add_expectation(expectation)
    return suite
