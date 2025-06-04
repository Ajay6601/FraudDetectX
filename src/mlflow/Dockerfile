FROM ghcr.io/mlflow/mlflow:latest

# Install PostgreSQL driver and other dependencies
RUN pip install --no-cache-dir psycopg2-binary sqlalchemy

# Set up environment variables (these will be overridden by docker-compose)
ENV MLFLOW_TRACKING_URI=http://localhost:5000

# Command will be provided by docker-compose
EXPOSE 5000

# Healthcheck to ensure MLflow is running properly
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:5000/api/2.0/mlflow/experiments/list || exit 1