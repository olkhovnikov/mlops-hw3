"""Shared constants, paths, and low-level env/utility helpers.

Bottom of the package import graph: every other module may import from here,
this module imports from none of them.
"""

from __future__ import annotations

import os
import subprocess
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


def git_sha() -> str | None:
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
