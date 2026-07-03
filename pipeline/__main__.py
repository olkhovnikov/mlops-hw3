"""CLI entrypoint so the Airflow DAG can drive each pipeline step as a
subprocess in the project venv:

    python -m pipeline prepare  --params-json '{...}'   # -> prints RUN_DIR=<path>
    python -m pipeline agent    --run-dir runs/<id>
    python -m pipeline eval     --run-dir runs/<id>
    python -m pipeline summarize --run-dir runs/<id>
    python -m pipeline run-all  --params-json '{...}'   # all steps (local repro)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline import (
    build_run_config,
    collect_metrics,
    load_config,
    load_dotenv,
    log_mlflow_run,
    prepare_run_dir,
    run_agent_batch,
    run_s3_uri,
    run_swebench_eval,
    upload_run_to_s3,
    write_manifest,
)


def _emit_run_dir(run_dir: Path) -> None:
    # Sentinel line the DAG greps out of stdout to thread run_dir via XCom.
    print(f"RUN_DIR={Path(run_dir).resolve()}", flush=True)


def cmd_prepare(args) -> None:
    params = _load_params(args)
    cfg = build_run_config(params)
    runs_root = Path(args.runs_root) if args.runs_root else None
    run_dir = prepare_run_dir(cfg, runs_root)
    print(f"[prepare] run_id={cfg['run_id']} dataset={cfg['dataset_name']}")
    _emit_run_dir(run_dir)


def cmd_agent(args) -> None:
    run_dir = Path(args.run_dir)
    cfg = load_config(run_dir)
    preds = run_agent_batch(cfg, run_dir)
    print(f"[agent] preds -> {preds}")


def cmd_eval(args) -> None:
    run_dir = Path(args.run_dir)
    cfg = load_config(run_dir)
    preds = run_dir / "run-agent" / "preds.json"
    eval_dir = run_swebench_eval(cfg, preds, run_dir)
    print(f"[eval] run-eval -> {eval_dir}")


def cmd_summarize(args) -> None:
    """Parse eval reports -> metrics.json + manifest.json (local, no upload)."""
    run_dir = Path(args.run_dir)
    cfg = load_config(run_dir)
    metrics = collect_metrics(run_dir / "run-eval", cfg)
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    write_manifest(run_dir, cfg, metrics)
    print(f"[summarize] metrics: {json.dumps(metrics)}")


def _publish(run_dir: Path, cfg: dict, metrics: dict) -> None:
    """Upload artifacts to S3 (if configured) and log the run to MLflow.

    The destination URI is deterministic, so we stamp it into manifest.json
    *before* uploading — the uploaded copy then carries its own remote_uri.
    """
    uri = run_s3_uri(cfg)  # None if S3 not configured
    write_manifest(run_dir, cfg, metrics, remote_uri=uri)
    if uri:
        upload_run_to_s3(run_dir, cfg)
    log_mlflow_run(cfg, metrics, run_dir, artifact_uri=uri)


def cmd_publish(args) -> None:
    run_dir = Path(args.run_dir)
    cfg = load_config(run_dir)
    metrics = json.loads((run_dir / "metrics.json").read_text())
    _publish(run_dir, cfg, metrics)


def cmd_run_all(args) -> None:
    params = _load_params(args)
    cfg = build_run_config(params)
    run_dir = prepare_run_dir(cfg, Path(args.runs_root) if args.runs_root else None)
    _emit_run_dir(run_dir)
    preds = run_agent_batch(cfg, run_dir)
    run_swebench_eval(cfg, preds, run_dir)
    metrics = collect_metrics(run_dir / "run-eval", cfg)
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    _publish(run_dir, cfg, metrics)


def _load_params(args) -> dict:
    if args.params_json:
        return json.loads(args.params_json)
    if args.params_file:
        return json.loads(Path(args.params_file).read_text())
    raise SystemExit("provide --params-json or --params-file")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("prepare", "run-all"):
        p = sub.add_parser(name)
        p.add_argument("--params-json")
        p.add_argument("--params-file")
        p.add_argument("--runs-root")

    for name in ("agent", "eval", "summarize", "publish"):
        p = sub.add_parser(name)
        p.add_argument("--run-dir", required=True)

    args = parser.parse_args()
    {
        "prepare": cmd_prepare,
        "agent": cmd_agent,
        "eval": cmd_eval,
        "summarize": cmd_summarize,
        "publish": cmd_publish,
        "run-all": cmd_run_all,
    }[args.command](args)


if __name__ == "__main__":
    main()
