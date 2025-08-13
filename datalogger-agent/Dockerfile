# DataLogger Agent Dockerfile
# Multi-stage build for optimized production image

# Build stage
FROM python:3.11-slim as builder

# Set build arguments
ARG BUILD_DATE
ARG VERSION=1.0.0
ARG VCS_REF

# Add metadata labels
LABEL maintainer="Smart Hotel Team" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="datalogger-agent" \
      org.label-schema.description="DataLogger Agent for IoT sensor data processing" \
      org.label-schema.version=$VERSION \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version="1.0"

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies including PostgreSQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd -r datalogger && useradd -r -g datalogger -u 1000 datalogger

# Set working directory
WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code and schema
COPY --chown=datalogger:datalogger . .

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data && \
    chown -R datalogger:datalogger /app

# Create database initialization script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Wait for TimescaleDB to be ready\n\
until pg_isready -h timescaledb -U postgres; do\n\
  echo "Waiting for TimescaleDB..."\n\
  sleep 2\n\
done\n\
\n\
# Create database\n\
psql -h timescaledb -U postgres -c "CREATE DATABASE IF NOT EXISTS hotel_iot;"\n\
\n\
# Create TimescaleDB extension\n\
psql -h timescaledb -U postgres -d hotel_iot -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"\n\
\n\
# Import schema\n\
if [ -f "/app/schema.sql" ]; then\n\
  psql -h timescaledb -U postgres -d hotel_iot -f /app/schema.sql\n\
fi\n\
\n\
# Start the main application\n\
exec python main.py' > /app/init-and-run.sh && \
    chmod +x /app/init-and-run.sh && \
    chown datalogger:datalogger /app/init-and-run.sh

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PGPASSWORD=password

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Switch to non-root user
USER datalogger

# Expose port for health checks (if needed)
EXPOSE 8080

# Set default command to run initialization script
CMD ["/app/init-and-run.sh"]
