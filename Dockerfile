FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    docker.io \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mlops-assignment

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

ENV PATH="/mlops-assignment/.venv/bin:$PATH"

COPY scripts scripts/

# Optional but useful if your script lacks executable bit or shebang issues:
RUN chmod +x scripts/*.sh

# The pipeline package is the unit of work the DockerOperator tasks run via
# `python -m pipeline <step>`. No build-system in pyproject, so it isn't
# installed as a package; `python -m` resolves it from the WORKDIR instead.
COPY pipeline pipeline/
