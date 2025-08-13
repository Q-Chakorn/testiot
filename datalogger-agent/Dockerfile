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

# Copy application code
COPY --chown=datalogger:datalogger . .

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data && \
    chown -R datalogger:datalogger /app

# Create database initialization script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting DataLogger Agent with database initialization..."\n\
\n\
# Wait for TimescaleDB to be ready\n\
echo "Waiting for TimescaleDB to be ready..."\n\
until pg_isready -h timescaledb -p 5432 -U postgres; do\n\
  echo "TimescaleDB is unavailable - sleeping"\n\
  sleep 2\n\
done\n\
\n\
echo "TimescaleDB is ready - executing setup"\n\
\n\
# Set password environment\n\
export PGPASSWORD=password\n\
\n\
# Create database if not exists\n\
echo "Creating database hotel_iot..."\n\
psql -h timescaledb -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '\''hotel_iot'\''" | grep -q 1 || psql -h timescaledb -U postgres -c "CREATE DATABASE hotel_iot;"\n\
\n\
# Create TimescaleDB extension\n\
echo "Creating TimescaleDB extension..."\n\
psql -h timescaledb -U postgres -d hotel_iot -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"\n\
\n\
# Import schema if exists\n\
if [ -f "/app/schema.sql" ]; then\n\
  echo "Importing schema..."\n\
  psql -h timescaledb -U postgres -d hotel_iot -f /app/schema.sql\n\
else\n\
  echo "No schema.sql found, skipping schema import"\n\
fi\n\
\n\
echo "Database setup completed successfully!"\n\
\n\
# Start the main application\n\
echo "Starting DataLogger Agent..."\n\
exec python main.py' > /app/init-db.sh && \
    chmod +x /app/init-db.sh && \
    chown datalogger:datalogger /app/init-db.sh

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

# Set default command to initialize database then start app
CMD ["/app/init-db.sh"]
