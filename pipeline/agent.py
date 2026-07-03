"""The run_agent stage: run mini-swe-agent over the selected instances."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from pipeline.settings import PROJECT_ROOT


def _builtin_swebench_config() -> str:
    """Absolute path to mini-swe-agent's builtin benchmarks/swebench.yaml.

    The ad-hoc batch script pointed at a sibling checkout that isn't guaranteed
    to exist; we resolve the installed package's copy instead.
    """
    from minisweagent.config import builtin_config_dir

    return str(builtin_config_dir / "benchmarks" / "swebench.yaml")


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
