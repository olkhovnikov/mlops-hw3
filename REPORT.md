# REPORT

> Phases 1–3 done. Configurable DAG + reproducible run folder (Phase 1/2),
> MLflow + MinIO (S3) + Airflow all on docker-compose, and each pipeline step
> runs in the project Docker image via `DockerOperator` (Phase 3).

## Pipeline

`dags/evaluate_agent.py`: `prepare_run → run_agent → run_eval → summarize → publish_artifacts`.
Each task is a `DockerOperator` that runs one step of the `pipeline` package
inside the project image (`swe-pipeline:latest`), so agent and evaluation work
happen in an isolated, repeatable environment — not in Airflow's own process.
Steps share one image and one `runs/<run-id>/` bind mount, so the run-dir path
threaded through XCom resolves identically in every container. Params
(UI-editable): `split`, `subset`, `workers` (required) + `model`, `task_slice`,
`run_id`, `cost_limit`.

`run_agent` and `run_eval` also mount the host Docker socket: mini-swe-agent and
the SWE-bench harness spawn their own per-instance containers
(docker-out-of-docker). Because those sibling mounts are resolved by the *host*
daemon, the DAG builds mount sources from `HOST_PROJECT_DIR` (the repo path on
the host), which compose passes in.

## Deploy (docker-compose): MinIO + MLflow + Airflow

```bash
cp .env.example .env          # set NEBIUS_API_KEY, HOST_PROJECT_DIR, AIRFLOW_UID, DOCKER_GID
docker compose --profile build build project-image   # build swe-pipeline:latest (one-off)
docker compose up -d          # start MinIO(+bucket), MLflow, Postgres, Airflow
docker compose ps             # all services healthy
docker compose down [-v]      # stop (-v also wipes volumes)
```

Required `.env` values for the compose Airflow (see `.env.example`):
- `HOST_PROJECT_DIR` — absolute repo path on the host (bind-mount source translation).
- `AIRFLOW_UID` (`id -u`) — Airflow containers run as your user so mounted `logs/` stays writable.
- `DOCKER_GID` (`getent group docker | cut -d: -f3`) — so the mounted docker socket is usable.

Services (all on the `swe-net` network):
- **Airflow** (LocalExecutor): `postgres`, `airflow-init` (db migrate), `airflow-apiserver`,
  `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`. Docker provider
  installed at start via `_PIP_ADDITIONAL_REQUIREMENTS`. Auth: SimpleAuthManager
  in all-admins mode → **no login** (fine for a local, SSH-forwarded VM).
- **MinIO** — S3 store. `publish_artifacts` uploads `runs/<run-id>/` there and records
  the `s3://` URI in `manifest.json` + the MLflow `artifact_s3_uri` tag.
- **MLflow** — tracking server; artifacts proxied into MinIO (`--serve-artifacts`).
  `MLFLOW_SERVER_ALLOWED_HOSTS=*` so in-container clients can reach it as `mlflow:5000`
  (MLflow 3.x host-header / DNS-rebinding check otherwise 403s).

In compose, task containers reach services by name (`http://mlflow:5000`,
`http://minio:9000`); the `localhost` URLs in `.env` are for host/standalone runs.

## Trigger

Forward ports (`ssh -L 8080:localhost:8080 -L 5000:localhost:5000 -L 9001:localhost:9001 <user>@<vm>`):
- Airflow UI → http://localhost:8080 (no login) → **evaluate_agent** → Trigger:
```json
{"split":"test","subset":"verified","workers":2,"task_slice":"0:1","cost_limit":1.0}
```

Local (no Airflow, project venv): `uv run python -m pipeline run-all --params-json '<same json>'`.

## Artifacts

```
runs/<run-id>/
  config.json  run-agent/{preds.json, <instance>/*.traj.json}
  run-eval/{<model>.<run-id>.json, logs/...}  metrics.json  manifest.json
```
One folder reconstructs the whole run. Rerun a step:
`uv run python -m pipeline eval|summarize|publish --run-dir runs/<run-id>`.

## View in MLflow

Open http://localhost:5000, select the **mini-swe-bench** experiment. Each run is
named by `run-id`; tick multiple runs → **Compare** to diff params/metrics.

## Completed run

`runs/run-20260703T125112Z-verified-0-1/` — `astropy__astropy-12907` **resolved**,
`resolved_rate=1.0` (1/1); its artifacts are uploaded to `s3://swe-runs/runs/...`
and logged to the MLflow `mini-swe-bench` experiment.

## Notes / known caveats

- Screenshots to capture after a UI-triggered run: `screenshots/airflow_dag.png`,
  `screenshots/mlflow_runs.png`, `screenshots/object_storage_artifacts.png`.
- The pipeline image runs as root, so files it writes under `runs/` are
  root-owned on the host (readable/committable; use a root helper container to
  delete). Fixable later by running the task containers as the host uid.
