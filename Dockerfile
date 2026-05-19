FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# OS security patches 적용
RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /usr/sbin/nologin app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

RUN mkdir -p output outputs/wiki && \
  chown -R app:app output outputs

USER app

CMD ["python", "main.py"]
