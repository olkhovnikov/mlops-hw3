"""Configurable Airflow DAG: run mini-swe-agent on a SWE-bench subset, evaluate
the produced patches, and log a reproducible run to MLflow.

    prepare_run -> run_agent -> run_eval -> summarize_and_log

The DAG itself is a thin orchestrator. All ML work runs in the *project* venv
via `uv run python -m pipeline <step>` (Airflow runs from a separate uv-tool
env), and every step reads/writes a single reproducible `runs/<run-id>/` tree.
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _pipeline(step_args, capture=False):
    """Invoke `uv run python -m pipeline <step_args>` in the project venv."""
    cmd = ["uv", "run", "python", "-m", "pipeline", *step_args]
    print(f"$ (cwd={PROJECT_ROOT}) {' '.join(cmd)}", flush=True)
    if capture:
        result = subprocess.run(
            cmd, cwd=PROJECT_ROOT, check=True, text=True, capture_output=True
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        return result.stdout
    # Stream long-running agent/eval output straight into the Airflow task log.
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)
    return None


@dag(
    dag_id="evaluate_agent",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["swe-bench", "mini-swe-agent", "evaluation"],
    params={
        # Required.
        "split": Param("test", type="string", description="Dataset split"),
        "subset": Param(
            "verified",
            type="string",
            description="SWE-bench subset (lite | verified | full | multimodal) "
            "or a dataset path",
        ),
        "workers": Param(5, type="integer", minimum=1, description="Parallel workers"),
        # Optional / experiment knobs.
        "model": Param(
            "nebius/moonshotai/Kimi-K2.6", type="string", description="Agent model"
        ),
        "task_slice": Param(
            "0:3",
            type=["string", "null"],
            description="Instance slice, e.g. '0:3'. Clear/null = whole split.",
        ),
        "run_id": Param(
            None,
            type=["string", "null"],
            description="Run id; clear/null = auto-generated",
        ),
        "cost_limit": Param(
            3.0, type="number", description="Per-instance USD cost limit (0 = none)"
        ),
    },
)
def evaluate_agent():
    @task
    def prepare_run() -> str:
        """Read params, build config, create runs/<run-id>/config.json."""
        params = dict(get_current_context()["params"])
        stdout = _pipeline(
            ["prepare", "--params-json", json.dumps(params)], capture=True
        )
        for line in stdout.splitlines():
            if line.startswith("RUN_DIR="):
                return line.split("=", 1)[1].strip()
        raise RuntimeError("prepare step did not emit a RUN_DIR sentinel")

    @task
    def run_agent(run_dir: str) -> str:
        """Run mini-swe-agent batch -> run-agent/preds.json + trajectories."""
        _pipeline(["agent", "--run-dir", run_dir])
        return run_dir

    @task
    def run_eval(run_dir: str) -> str:
        """Evaluate preds.json with SWE-bench -> run-eval/ logs + report."""
        _pipeline(["eval", "--run-dir", run_dir])
        return run_dir

    @task
    def summarize(run_dir: str) -> str:
        """Parse eval reports -> metrics.json + manifest.json."""
        _pipeline(["summarize", "--run-dir", run_dir])
        return run_dir

    @task
    def publish_artifacts(run_dir: str) -> str:
        """Upload runs/<run-id>/ to S3/MinIO (if configured) and log to MLflow."""
        _pipeline(["publish", "--run-dir", run_dir])
        return run_dir

    run_dir = prepare_run()
    run_dir = run_agent(run_dir)
    run_dir = run_eval(run_dir)
    run_dir = summarize(run_dir)
    publish_artifacts(run_dir)


evaluate_agent()
