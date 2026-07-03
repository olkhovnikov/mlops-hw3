# REPORT

> Phase 1 done (configurable DAG + reproducible run folder). Phase 3: MLflow
> server + MinIO (S3) run via docker-compose. Remaining (DockerOperator, Airflow
> itself on compose) pending — Airflow still runs standalone for now.

## Pipeline

`dags/evaluate_agent.py`: `prepare_run → run_agent → run_eval → summarize → publish_artifacts`.
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
One folder reconstructs the whole run.

## Infra (docker-compose): MinIO + MLflow

```bash
docker compose up -d          # start MinIO (+bucket) and MLflow; leave running
docker compose ps             # check healthy
docker compose down [-v]      # stop (‑v also wipes stored artifacts)
```
Start this **before** triggering a DAG. Two services:
- **MinIO** — S3 store. `publish_artifacts` uploads `runs/<run-id>/` there and
  records the `s3://` URI in `manifest.json` + the MLflow `artifact_s3_uri` tag.
  Console: http://localhost:9001 (`minioadmin`/`minioadmin`). S3 upload is opt-in
  via `S3_ENDPOINT_URL`.
- **MLflow** — tracking server (`MLFLOW_TRACKING_URI=http://localhost:5000`).
  Metadata in a sqlite volume; artifacts proxied into MinIO (`--serve-artifacts`),
  so clients need only the tracking URI. Unset `MLFLOW_TRACKING_URI` to fall back
  to a local `sqlite:///mlflow.db` store.

## View in MLflow

Forward port 5000 (`ssh -L 5000:localhost:5000 <user>@<vm-host>`), open
http://localhost:5000, select the **mini-swe-bench** experiment. Each run is
named by `run-id`; tick multiple runs → **Compare** to diff params/metrics.

## Completed run

`runs/run-20260703T125112Z-verified-0-1/` — `astropy__astropy-12907` **resolved**,
`resolved_rate=1.0` (1/1). Rerun a step: `uv run python -m pipeline eval|summarize --run-dir runs/<run-id>`.
