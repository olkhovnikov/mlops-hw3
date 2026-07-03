"""Where a run's artifacts live: the local manifest (index) and the remote
S3/MinIO copy. Both answer "how do I find this run's files".
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pipeline.evaluation import find_report


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
    trajectories = sorted(_rel(p, run_dir) for p in agent_dir.glob("*/*.traj.json"))

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
        "artifact_root": {
            "type": "s3" if remote_uri else "local",
            "path": str(run_dir.resolve()),
            "remote_uri": remote_uri,
        },
    }
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    return path


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
