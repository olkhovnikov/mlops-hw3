"""Evaluation pipeline helpers for the mini-swe-agent -> SWE-bench workflow.

The functions here are deliberately framework-agnostic: they are called both by
the Airflow DAG (via the ``python -m pipeline`` CLI) and can be run directly for
local debugging / reproduction. All heavy dependencies (mini-swe-agent, swebench,
mlflow) live in the project venv, so this package is imported/run there.
"""

from pipeline.helpers import (
    build_run_config,
    collect_metrics,
    log_mlflow_run,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    write_manifest,
)

__all__ = [
    "build_run_config",
    "prepare_run_dir",
    "run_agent_batch",
    "run_swebench_eval",
    "collect_metrics",
    "write_manifest",
    "log_mlflow_run",
]
