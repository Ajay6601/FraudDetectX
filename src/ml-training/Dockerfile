FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    pkg-config \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install packages individually to avoid timeouts
COPY requirements.txt .

# Install packages one by one with timeouts
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --timeout 600 celery==5.3.1
RUN pip install --no-cache-dir --timeout 600 flower==2.0.1
RUN pip install --no-cache-dir --timeout 600 redis==4.6.0
RUN pip install --no-cache-dir --timeout 600 pandas==2.0.3
RUN pip install --no-cache-dir --timeout 600 numpy==1.24.3
RUN pip install --no-cache-dir --timeout 600 scikit-learn==1.3.0
RUN pip install --no-cache-dir --timeout 600 psycopg2-binary==2.9.7
RUN pip install --no-cache-dir --timeout 600 python-dotenv==1.0.0
RUN pip install --no-cache-dir --timeout 600 loguru==0.7.0
RUN pip install --no-cache-dir --timeout 600 joblib==1.3.2
RUN pip install --no-cache-dir --timeout 600 mlflow==2.5.0
RUN pip install --no-cache-dir --timeout 600 imbalanced-learn==0.11.0
RUN pip install --no-cache-dir --timeout 1200 xgboost==1.7.6

# Copy application code
COPY . .

# Create directories
RUN mkdir -p /app/models /app/data /app/logs /app/mlflow-artifacts

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV C_FORCE_ROOT=1

CMD ["celery", "-A", "celery_tasks", "worker", "--loglevel=info", "--concurrency=2"]