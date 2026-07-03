"""Experiment tracking: log a run's params, metrics, and artifact refs to
MLflow. Isolated so swapping the tracking backend touches only this file.
"""

from __future__ import annotations

import os
from pathlib import Path

from pipeline.settings import DEFAULT_MLFLOW_EXPERIMENT, PROJECT_ROOT


def log_mlflow_run(
    run_config: dict,
    metrics: dict,
    run_dir: Path,
    artifact_uri: str | None = None,
) -> None:
    """Log params, metrics, and artifact references to MLflow.

    Tracking URI defaults to a local sqlite store so no server is required for
    Phase 1; override with MLFLOW_TRACKING_URI to point at the compose server.
    """
    import mlflow

    run_dir = Path(run_dir)
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI") or (
        f"sqlite:///{PROJECT_ROOT / 'mlflow.db'}"
    )
    experiment = os.environ.get("MLFLOW_EXPERIMENT", DEFAULT_MLFLOW_EXPERIMENT)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_config["run_id"]):
        mlflow.log_params(
            {
                "run_id": run_config["run_id"],
                "split": run_config["split"],
                "subset": run_config["subset"],
                "dataset_name": run_config["dataset_name"],
                "model": run_config["model"],
                "workers": run_config["workers"],
                "task_slice": run_config["task_slice"] or "all",
                "cost_limit": run_config["cost_limit"],
                "git_sha": run_config.get("git_sha"),
            }
        )
        mlflow.log_metrics({k: float(v) for k, v in metrics.items()})
        mlflow.set_tags(
            {
                "run_dir": str(run_dir.resolve()),
                "artifact_root": artifact_uri or str(run_dir.resolve()),
                "artifact_s3_uri": artifact_uri or "",
            }
        )
        # Log the small, human-readable summaries as artifacts (references to the
        # full run folder are captured via the run_dir tag + manifest).
        for name in ("config.json", "metrics.json", "manifest.json"):
            f = run_dir / name
            if f.exists():
                mlflow.log_artifact(str(f))
    print(f"[log_mlflow_run] logged run '{run_config['run_id']}' to {tracking_uri}")
