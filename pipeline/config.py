"""Run configuration and the runs/<run-id>/ directory scaffolding.

Turns raw Airflow params into a fully-resolved, self-describing config and
creates the run directory that every later stage reads/writes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.settings import (
    DEFAULT_MODEL,
    RUNS_ROOT,
    SUBSET_TO_DATASET,
    git_sha,
)


def resolve_dataset_name(subset: str) -> str:
    if "/" in subset:  # already a HF dataset path or local file
        return subset
    return SUBSET_TO_DATASET.get(subset.lower(), subset)


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
        "git_sha": git_sha(),
    }


def prepare_run_dir(run_config: dict, runs_root: Path | None = None) -> Path:
    runs_root = Path(runs_root) if runs_root else RUNS_ROOT
    run_dir = runs_root / run_config["run_id"]
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(run_config, indent=2))
    return run_dir


def load_config(run_dir: Path) -> dict:
    return json.loads((Path(run_dir) / "config.json").read_text())
