"""Configurable Airflow DAG: run mini-swe-agent on a SWE-bench subset, evaluate
the produced patches, and log a reproducible run to MLflow.

    prepare_run -> run_agent -> run_eval -> summarize -> publish_artifacts

Each task runs a step of the `pipeline` package inside the project Docker image
(`DockerOperator`), so agent and evaluation work happen in an isolated,
repeatable environment rather than in Airflow's own process. All steps share one
image and one `runs/<run-id>/` bind mount, so the run dir path threaded through
XCom resolves identically in every container.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag
from docker.types import Mount

# Repo root as seen by the *host* Docker daemon. When Airflow runs on the host
# (standalone) this is just the repo path. When Airflow itself runs in a
# container (docker-compose), bind-mount sources are still resolved by the host
# daemon, so compose must pass the host repo path in HOST_PROJECT_DIR.
HOST_PROJECT_DIR = os.environ.get(
    "HOST_PROJECT_DIR", str(Path(__file__).resolve().parents[1])
)

# The project image built from ./Dockerfile (see docker-compose `project-image`).
PROJECT_IMAGE = os.environ.get("AIRFLOW_PROJECT_IMAGE", "swe-pipeline:latest")
# Docker network the task containers join so `publish` can reach MLflow/MinIO by
# service name (http://mlflow:5000, http://minio:9000).
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "swe-net")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")

# Where the pipeline lives inside the image (matches the Dockerfile WORKDIR).
IMAGE_WORKDIR = "/mlops-assignment"

# Host env forwarded into every task container. Absent keys are simply skipped,
# so the pipeline degrades gracefully (e.g. no S3 vars -> artifacts stay local).
ENV_PASSTHROUGH = [
    "NEBIUS_API_KEY",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_S3_ENDPOINT_URL",
    "MLFLOW_EXPERIMENT",
    "S3_ENDPOINT_URL",
    "S3_BUCKET",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
]

# Baseline for every task: retry transient failures with exponential backoff.
# Long, network-bound stages (agent/eval) and the deterministic ones override
# `retries` and `execution_timeout` individually below.
DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _task_env() -> dict:
    return {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}


def _pipeline_task(task_id, command, *, needs_docker=False, **op_kwargs):
    """A DockerOperator that runs one `python -m pipeline <step>` in the image.

    Every task bind-mounts the shared runs/ tree. Steps that spawn their own
    sibling containers (mini-swe-agent per-instance envs, the SWE-bench eval
    harness) also get the host Docker socket -> docker-out-of-docker.
    """
    mounts = [
        Mount(
            source=f"{HOST_PROJECT_DIR}/runs",
            target=f"{IMAGE_WORKDIR}/runs",
            type="bind",
        )
    ]
    if needs_docker:
        mounts.append(
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        )
    return DockerOperator(
        task_id=task_id,
        image=PROJECT_IMAGE,
        command=command,
        environment=_task_env(),
        mounts=mounts,
        working_dir=IMAGE_WORKDIR,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,  # no host /tmp mount (would not exist under DooD)
        auto_remove="force",
        # Push the container's last stdout line to XCom (prepare emits run_dir).
        do_xcom_push=True,
        **op_kwargs,
    )


@dag(
    dag_id="evaluate_agent",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    tags=["swe-bench", "mini-swe-agent", "evaluation", "docker"],
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
    # prepare: params -> runs/<run-id>/config.json; last stdout line = run_dir.
    prepare_run = _pipeline_task(
        "prepare_run",
        ["python", "-m", "pipeline", "prepare", "--params-json", "{{ params | tojson }}"],
        execution_timeout=timedelta(minutes=5),
    )

    # Downstream steps pull the run_dir path prepare pushed to XCom.
    run_dir = "{{ ti.xcom_pull(task_ids='prepare_run') }}"

    # agent: mini-swe-agent batch -> run-agent/preds.json + trajectories.
    # Long and LLM-API-bound; spawns per-instance containers (needs Docker).
    run_agent = _pipeline_task(
        "run_agent",
        ["python", "-m", "pipeline", "agent", "--run-dir", run_dir],
        needs_docker=True,
        retries=2,
        execution_timeout=timedelta(hours=3),
    )

    # eval: SWE-bench harness over preds.json -> run-eval/ logs + report.
    # Builds/runs per-instance images; needs Docker.
    run_eval = _pipeline_task(
        "run_eval",
        ["python", "-m", "pipeline", "eval", "--run-dir", run_dir],
        needs_docker=True,
        retries=2,
        execution_timeout=timedelta(hours=3),
    )

    # summarize: parse reports -> metrics.json + manifest.json. Pure local
    # parsing, so a failure is a real bug, not transient -> don't retry.
    summarize = _pipeline_task(
        "summarize",
        ["python", "-m", "pipeline", "summarize", "--run-dir", run_dir],
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    # publish: upload runs/<run-id>/ to S3/MinIO (if configured) + log to MLflow.
    # Network I/O -> retry generously.
    publish_artifacts = _pipeline_task(
        "publish_artifacts",
        ["python", "-m", "pipeline", "publish", "--run-dir", run_dir],
        retries=3,
        execution_timeout=timedelta(minutes=30),
    )

    prepare_run >> run_agent >> run_eval >> summarize >> publish_artifacts


evaluate_agent()
