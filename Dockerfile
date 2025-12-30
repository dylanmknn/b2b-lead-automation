# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install dependencies to /install (shared location)
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Production image
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder to Python site-packages
COPY --from=builder /install /usr/local

# Create non-root user for security
RUN useradd -m -r appuser

# Copy application code and set ownership
COPY --chown=appuser:appuser src/ ./src/

USER appuser

# Default command
ENTRYPOINT ["python", "src/millemail_pipeline.py"]
CMD ["--count", "50"]
