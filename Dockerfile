# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  UV_LINK_MODE=copy \
  PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev --no-install-project

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev --no-install-project

FROM python:3.12-slim-trixie AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  HOME="/tmp" \
  USER="app" \
  LOGNAME="app" \
  XDG_CACHE_HOME="/tmp/hivewiki-builder/.cache" \
  DSP_CACHEDIR="/tmp/hivewiki-builder/dsp-cache" \
  TORCHINDUCTOR_CACHE_DIR="/tmp/hivewiki-builder/torchinductor-cache" \
  STORM_WORK_DIR="/tmp/hivewiki-builder/storm" \
  PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN useradd --create-home --shell /usr/sbin/nologin app

COPY --from=builder /app /app

USER app

ENTRYPOINT ["python", "consumer.py"]
