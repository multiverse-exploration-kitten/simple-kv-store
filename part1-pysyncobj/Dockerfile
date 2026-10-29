FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.6.13 /uv /uvx /bin/

# UV_COMPILE_BYTECODE: compile .py files at build time, so the non-root user never writes them.
# UV_LINK_MODE=copy: the uv cache is a separate mount, so hard links cannot work.
# UV_PYTHON_DOWNLOADS=never: use the Python of the base image.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

RUN groupadd --system --gid 10001 kvstore \
    && useradd --system --uid 10001 --gid kvstore --no-create-home --shell /usr/sbin/nologin kvstore \
    && mkdir /data \
    && chown kvstore:kvstore /data

WORKDIR /app

# Dependencies first: this layer changes only when uv.lock or pyproject.toml changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# A new named volume copies /data from the image, so the volume belongs to kvstore too.
ENV PATH="/app/.venv/bin:$PATH" \
    KV_DATA_DIR=/data

USER kvstore

# 4321: Raft traffic between the nodes. 8000: HTTP API.
EXPOSE 4321 8000

CMD ["kvstore"]
