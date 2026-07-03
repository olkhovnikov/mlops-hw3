"""Evaluation pipeline for the mini-swe-agent -> SWE-bench workflow.

Framework-agnostic helpers, called both by the Airflow DAG (via the
``python -m pipeline`` CLI) and directly for local debugging / reproduction.
All heavy dependencies (mini-swe-agent, swebench, mlflow, boto3) live in the
project venv, so this package is imported/run there.

Internal layout (import graph flows downward):

    settings    constants, paths, env/util
    config      build_run_config, run-dir scaffolding
    agent       run_agent stage
    evaluation  run_eval stage + metrics
    storage     manifest + S3/MinIO upload
    tracking    MLflow

Public API is re-exported here so callers use ``from pipeline import ...``
regardless of the internal file layout.
"""

from pipeline.agent import run_agent_batch
from pipeline.config import (
    build_run_config,
    load_config,
    prepare_run_dir,
    resolve_dataset_name,
)
from pipeline.evaluation import collect_metrics, find_report, run_swebench_eval
from pipeline.settings import load_dotenv
from pipeline.storage import run_s3_uri, upload_run_to_s3, write_manifest
from pipeline.tracking import log_mlflow_run

__all__ = [
    "load_dotenv",
    "build_run_config",
    "resolve_dataset_name",
    "prepare_run_dir",
    "load_config",
    "run_agent_batch",
    "run_swebench_eval",
    "find_report",
    "collect_metrics",
    "write_manifest",
    "upload_run_to_s3",
    "run_s3_uri",
    "log_mlflow_run",
]
