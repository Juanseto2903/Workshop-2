"""Great Expectations validation layer (Step 7).

Layout
------
rules.py     single source of truth for DQ-01..DQ-12 and their expectations (no GE import)
evaluate.py  applies the severity policy to GE results and writes JSON evidence (no GE import)
suites.py    builds the GE expectation suites from rules.py
gates.py     GE context, validation definitions, checkpoints and ``run_gate``
__main__.py  command line: ``python -m src.validation --help``
"""
