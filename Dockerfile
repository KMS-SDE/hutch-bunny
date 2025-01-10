FROM ghcr.io/astral-sh/uv:bookworm-slim

COPY . /app
WORKDIR /app

RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "bunny-daemon"]
