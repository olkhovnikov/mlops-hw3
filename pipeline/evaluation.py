"""The run_eval stage: run the SWE-bench harness and read its report.

`find_report` lives here because locating the summary json is an eval-output
detail; `storage.write_manifest` reuses it to point the manifest at the report.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


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
    return {
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
