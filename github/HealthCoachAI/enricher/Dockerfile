FROM ghcr.io/astral-sh/uv:0.6.17-python3.13-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"


# You probably don't need build-essential here unless pandas has C extensions
# that require it, but it's safe to keep.
RUN apt update && apt install build-essential -y

# Copy source code and dependency files
COPY . /app/
COPY pyproject.toml /app/
COPY uv.lock /app/

WORKDIR /app
# Install the enricher's specific dependencies (e.g., pandas, schedule)
RUN uv lock
RUN uv sync --locked

# Create the same non-root user
RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser && chown -R appuser:appuser /app
USER appuser

# The command to run the enrichment script
CMD ["uv", "run", "python", "enricher.py"]