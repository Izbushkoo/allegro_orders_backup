FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry==1.6.1

# Configure Poetry
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VENV_IN_PROJECT=0 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Copy only dependency files first (for better caching)
COPY pyproject.toml poetry.lock* ./

# Install dependencies (this layer will be cached unless dependencies change)  
RUN poetry config virtualenvs.create false \
    && poetry install --no-root \
    && rm -rf $POETRY_CACHE_DIR

# Copy application code (this layer changes frequently)
COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini ./
COPY README.md ./
COPY scripts/ ./scripts/

# Create logs directory and set execute permissions on scripts
RUN mkdir -p /app/logs && \
    chmod +x scripts/*.py && \
    chmod +x scripts/entrypoint.sh

# Install project in editable mode (quick since deps are already installed)
RUN poetry config virtualenvs.create false \
    && poetry install --only-root

# Expose port
EXPOSE 8000

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden in docker-compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]