# REPORT

> Phase 1 done (configurable DAG + reproducible run folder + MLflow).
> Phase 3 (DockerOperator, docker-compose, S3) pending — will expand then.

## Pipeline

`dags/evaluate_agent.py`: `prepare_run → run_agent → run_eval → summarize_and_log`.
The DAG is a thin orchestrator; all ML work runs in the project venv via
`uv run python -m pipeline <step>` (helpers in `pipeline/`). Params (UI-editable):
`split`, `subset`, `workers` (required) + `model`, `task_slice`, `run_id`, `cost_limit`.

## Trigger

`bash run-airflow-standalone.sh` → http://localhost:8080 (`admin`/`admin`) → **evaluate_agent** → Trigger:
```json
{"split":"test","subset":"verified","workers":2,"task_slice":"0:1","cost_limit":1.0}
```
Local: `uv run python -m pipeline run-all --params-json '<same json>'`.

## Artifacts

```
runs/<run-id>/
  config.json  run-agent/{preds.json, <instance>/*.traj.json}
  run-eval/{<model>.<run-id>.json, logs/...}  metrics.json  manifest.json
```
One folder reconstructs the whole run. MLflow logs params + metrics + artifact
refs to a local SQLite store (`MLFLOW_TRACKING_URI` repoints to Phase 3 server).

## View in MLflow

```bash
uv run mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
```
Forward port 5000 (`ssh -L 5000:localhost:5000 <user>@<vm-host>`), open
http://localhost:5000, select the **mini-swe-bench** experiment. Each run is
named by `run-id`; tick multiple runs → **Compare** to diff params/metrics.

## Completed run

`runs/run-20260703T125112Z-verified-0-1/` — `astropy__astropy-12907` **resolved**,
`resolved_rate=1.0` (1/1). Rerun a step: `uv run python -m pipeline eval|summarize --run-dir runs/<run-id>`.
