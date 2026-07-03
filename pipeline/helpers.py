"""Core pipeline helpers.

Data flow (one call per DAG task):

    build_run_config(params)          -> run_config dict
    prepare_run_dir(run_config)       -> runs/<run-id>/  (+ config.json)
    run_agent_batch(cfg, run_dir)     -> runs/<run-id>/run-agent/preds.json
    run_swebench_eval(cfg, preds, rd) -> runs/<run-id>/run-eval/  (report + logs)
    collect_metrics(eval_dir, cfg)    -> metrics dict (+ metrics.json)
    write_manifest(run_dir, cfg, m)   -> manifest.json
    log_mlflow_run(cfg, metrics, rd)  -> logs params/metrics/artifacts to MLflow
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Repo root = parent of the `pipeline/` package directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

DEFAULT_MODEL = "nebius/moonshotai/Kimi-K2.6"
DEFAULT_MLFLOW_EXPERIMENT = "mini-swe-bench"

# Map a mini-swe-agent `--subset` name to the HuggingFace dataset SWE-bench
# evaluation expects via `--dataset_name`. If the subset already looks like a
# dataset path (contains "/") it is used verbatim.
SUBSET_TO_DATASET = {
    "lite": "princeton-nlp/SWE-bench_Lite",
    "verified": "princeton-nlp/SWE-bench_Verified",
    "full": "princeton-nlp/SWE-bench",
    "test": "princeton-nlp/SWE-bench",
    "multimodal": "princeton-nlp/SWE-bench_Multimodal",
    "multilingual": "princeton-nlp/SWE-bench_Multilingual",
}


# --------------------------------------------------------------------------- #
# environment / small utilities
# --------------------------------------------------------------------------- #
def load_dotenv(path: Path | None = None) -> None:
    """Best-effort load of the repo `.env` into os.environ (no extra deps).

    Airflow may be launched without the project's env (it runs from a separate
    `uv tool` environment), so we make sure NEBIUS_API_KEY etc. are present when
    the pipeline actually shells out to the agent.
    """
    path = path or (PROJECT_ROOT / ".env")
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        # Don't clobber values already exported into the environment.
        os.environ.setdefault(key, value)


def _git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def resolve_dataset_name(subset: str) -> str:
    if "/" in subset:  # already a HF dataset path or local file
        return subset
    return SUBSET_TO_DATASET.get(subset.lower(), subset)


def _builtin_swebench_config() -> str:
    """Absolute path to mini-swe-agent's builtin benchmarks/swebench.yaml.

    The ad-hoc batch script pointed at a sibling checkout that isn't guaranteed
    to exist; we resolve the installed package's copy instead.
    """
    from minisweagent.config import builtin_config_dir

    return str(builtin_config_dir / "benchmarks" / "swebench.yaml")


# --------------------------------------------------------------------------- #
# 1. config
# --------------------------------------------------------------------------- #
def build_run_config(params: dict) -> dict:
    """Turn raw Airflow params into a fully-resolved, self-describing config."""
    split = params.get("split")
    subset = params.get("subset")
    workers = params.get("workers")
    if not split or not subset or workers in (None, ""):
        raise ValueError("`split`, `subset`, and `workers` are required params")

    model = params.get("model") or DEFAULT_MODEL
    task_slice = (params.get("task_slice") or "").strip()
    cost_limit = params.get("cost_limit")
    cost_limit = float(cost_limit) if cost_limit not in (None, "") else 3.0

    created_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = (params.get("run_id") or "").strip()
    if not run_id:
        slug = subset.replace("/", "-")
        slice_tag = task_slice.replace(":", "-") if task_slice else "all"
        run_id = f"run-{created_at}-{slug}-{slice_tag}"

    return {
        "run_id": run_id,
        "split": split,
        "subset": subset,
        "dataset_name": resolve_dataset_name(subset),
        "model": model,
        "workers": int(workers),
        "task_slice": task_slice,
        "cost_limit": cost_limit,
        "created_at": created_at,
        "git_sha": _git_sha(),
    }


# --------------------------------------------------------------------------- #
# 2. run directory
# --------------------------------------------------------------------------- #
def prepare_run_dir(run_config: dict, runs_root: Path | None = None) -> Path:
    runs_root = Path(runs_root) if runs_root else RUNS_ROOT
    run_dir = runs_root / run_config["run_id"]
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(run_config, indent=2))
    return run_dir


def load_config(run_dir: Path) -> dict:
    return json.loads((Path(run_dir) / "config.json").read_text())


# --------------------------------------------------------------------------- #
# 3. run agent (mini-swe-agent batch)
# --------------------------------------------------------------------------- #
def run_agent_batch(run_config: dict, run_dir: Path) -> Path:
    """Run mini-swe-agent over the selected instances, writing to run-agent/.

    Produces:  run-agent/preds.json  and  run-agent/<instance>/<instance>.traj.json
    Returns the path to preds.json.
    """
    agent_dir = Path(run_dir) / "run-agent"
    cmd = [
        "mini-extra",
        "swebench",
        "--subset",
        run_config["subset"],
        "--split",
        run_config["split"],
        "--model",
        run_config["model"],
        "--workers",
        str(run_config["workers"]),
        "-o",
        str(agent_dir),
        # Start from the builtin swebench config, then override the per-instance
        # cost limit from the run config (batch CLI has no --cost-limit flag).
        "-c",
        _builtin_swebench_config(),
        "-c",
        f"agent.cost_limit={run_config['cost_limit']}",
    ]
    if run_config.get("task_slice"):
        cmd += ["--slice", run_config["task_slice"]]

    env = {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}
    print(f"[run_agent_batch] $ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=PROJECT_ROOT, env=env, check=True)

    preds = agent_dir / "preds.json"
    if not preds.exists():
        raise FileNotFoundError(f"agent batch did not produce {preds}")
    return preds


# --------------------------------------------------------------------------- #
# 4. run evaluation (SWE-bench harness; uses Docker under the hood)
# --------------------------------------------------------------------------- #
def run_swebench_eval(run_config: dict, preds_path: Path, run_dir: Path) -> Path:
    """Evaluate preds.json with the SWE-bench harness, writing to run-eval/.

    The harness writes per-instance logs to a *relative* `logs/run_evaluation/`
    and the summary report to `--report_dir`, so we run it with cwd=run-eval to
    land everything under runs/<run-id>/run-eval/.
    Returns the run-eval directory.
    """
    eval_dir = Path(run_dir) / "run-eval"
    eval_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        run_config["dataset_name"],
        "--split",
        run_config["split"],
        "--predictions_path",
        str(Path(preds_path).resolve()),
        "--max_workers",
        str(run_config["workers"]),
        "--run_id",
        run_config["run_id"],
        "--report_dir",
        ".",
    ]
    print(f"[run_swebench_eval] (cwd={eval_dir}) $ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=eval_dir, env=os.environ, check=True)
    return eval_dir


# --------------------------------------------------------------------------- #
# 5. metrics
# --------------------------------------------------------------------------- #
def find_report(eval_dir: Path, run_config: dict) -> Path | None:
    """Locate the SWE-bench summary report json in run-eval/.

    Named `<model_name_with__>.<run_id>.json`; per-instance report.json files
    live deeper under logs/, so a top-level glob is enough.
    """
    eval_dir = Path(eval_dir)
    candidates = sorted(eval_dir.glob(f"*.{run_config['run_id']}.json"))
    if not candidates:
        candidates = sorted(eval_dir.glob("*.json"))
    return candidates[0] if candidates else None


def collect_metrics(eval_dir: Path, run_config: dict) -> dict:
    report_path = find_report(eval_dir, run_config)
    if report_path is None:
        raise FileNotFoundError(f"no SWE-bench report json found in {eval_dir}")
    report = json.loads(report_path.read_text())

    submitted = report.get("submitted_instances", 0) or 0
    resolved = report.get("resolved_instances", 0) or 0
    completed = report.get("completed_instances", 0) or 0
    metrics = {
        "total_instances": report.get("total_instances", 0),
        "submitted_instances": submitted,
        "completed_instances": completed,
        "resolved_instances": resolved,
        "unresolved_instances": report.get("unresolved_instances", 0),
        "empty_patch_instances": report.get("empty_patch_instances", 0),
        "error_instances": report.get("error_instances", 0),
        "resolved_rate": round(resolved / submitted, 4) if submitted else 0.0,
        "completed_rate": round(completed / submitted, 4) if submitted else 0.0,
    }
    return metrics


# --------------------------------------------------------------------------- #
# 6. manifest + mlflow
# --------------------------------------------------------------------------- #
def _rel(path: Path, run_dir: Path) -> str | None:
    try:
        return str(Path(path).resolve().relative_to(Path(run_dir).resolve()))
    except Exception:
        return None


def write_manifest(
    run_dir: Path, run_config: dict, metrics: dict, remote_uri: str | None = None
) -> Path:
    """Write manifest.json pointing at the key files so one folder is enough
    to reconstruct the whole run."""
    run_dir = Path(run_dir)
    agent_dir = run_dir / "run-agent"
    eval_dir = run_dir / "run-eval"
    preds = agent_dir / "preds.json"
    report = find_report(eval_dir, run_config)
    trajectories = sorted(
        _rel(p, run_dir) for p in agent_dir.glob("*/*.traj.json")
    )

    manifest = {
        "run_id": run_config["run_id"],
        "created_at": run_config.get("created_at"),
        "git_sha": run_config.get("git_sha"),
        "config": "config.json",
        "metrics": "metrics.json",
        "artifacts": {
            "preds": _rel(preds, run_dir) if preds.exists() else None,
            "trajectories": trajectories,
            "eval_report": _rel(report, run_dir) if report else None,
            "eval_logs": "run-eval/logs/run_evaluation",
        },
        # Phase 1 keeps artifacts local. Phase 2/3 will upload this folder to
        # object storage and record the URI here (e.g. s3://bucket/runs/<id>/).
        "artifact_root": {
            "type": "s3" if remote_uri else "local",
            "path": str(run_dir.resolve()),
            "remote_uri": remote_uri,
        },
    }
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path


# --------------------------------------------------------------------------- #
# 7. object storage (S3 / MinIO)
# --------------------------------------------------------------------------- #
def _s3_config() -> tuple[str, str] | None:
    """(endpoint_url, bucket) if S3 upload is configured, else None.

    Upload is opt-in: with S3_ENDPOINT_URL unset the pipeline runs unchanged and
    keeps artifacts local.
    """
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    bucket = os.environ.get("S3_BUCKET")
    if not endpoint or not bucket:
        return None
    return endpoint, bucket


def run_s3_uri(run_config: dict) -> str | None:
    """Deterministic destination URI for a run (known before upload)."""
    cfg = _s3_config()
    if cfg is None:
        return None
    _, bucket = cfg
    return f"s3://{bucket}/runs/{run_config['run_id']}"


def upload_run_to_s3(run_dir: Path, run_config: dict) -> str | None:
    """Upload the whole runs/<run-id>/ tree to S3/MinIO under runs/<run-id>/.

    Returns the s3:// URI, or None if S3 is not configured.
    """
    cfg = _s3_config()
    if cfg is None:
        print("[upload_run_to_s3] S3 not configured (S3_ENDPOINT_URL unset); skipping")
        return None
    endpoint, bucket = cfg

    import boto3

    s3 = boto3.client("s3", endpoint_url=endpoint)
    # Ensure the bucket exists (idempotent; docker-compose also creates it).
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    run_dir = Path(run_dir)
    prefix = f"runs/{run_config['run_id']}"
    n = 0
    for f in run_dir.rglob("*"):
        if f.is_file():
            key = f"{prefix}/{f.relative_to(run_dir).as_posix()}"
            s3.upload_file(str(f), bucket, key)
            n += 1
    uri = f"s3://{bucket}/{prefix}"
    print(f"[upload_run_to_s3] uploaded {n} files -> {uri} (endpoint {endpoint})")
    return uri


# --------------------------------------------------------------------------- #
# 8. mlflow
# --------------------------------------------------------------------------- #
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
