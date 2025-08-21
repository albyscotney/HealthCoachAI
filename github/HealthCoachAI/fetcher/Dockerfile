FROM ghcr.io/astral-sh/uv:0.6.17-python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt update && apt install build-essential -y

WORKDIR /app

# Copy all project files into the work directory
COPY . /app/

# Install dependencies
RUN uv sync --locked

# Create and switch to a non-root user
RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser && chown -R appuser:appuser /app
USER appuser

# Set the default command
CMD ["uv", "run", "src/garmin_fetch.py"]