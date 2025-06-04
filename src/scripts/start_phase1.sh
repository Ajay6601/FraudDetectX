#!/bin/bash

echo "Starting Complete Phase 1 - ML Fraud Detection Pipeline"
echo "============================================================"
echo "Project: C:\\Users\\ajayr\\PycharmProjects\\FraudDetectX\\src\\"
echo "Using: Confluent Cloud Kafka (NO local Kafka)"
echo ""

if [ ! -f .env ]; then
    echo ".env file not found. Please create it with your Confluent Cloud credentials."
    exit 1
fi

if ! grep -q "CONFLUENT_BOOTSTRAP_SERVERS" .env; then
    echo "Confluent Cloud credentials not found in .env file"
    exit 1
fi

echo "Found Confluent Cloud credentials in .env"
echo ""

echo "Phase 1 Components:"
echo "  - Data Generator → Confluent Cloud Kafka (100k transactions)"
echo "  - Spark Processor → Real-time feature engineering"
echo "  - XGBoost + SMOTE → ML Training with hyperparameter optimization"
echo "  - Celery + Redis → Async ML processing"
echo "  - MLflow → Model tracking and registry"
echo "  - PostgreSQL → Data storage with comprehensive schema"
echo ""

echo "Installing database initialization dependencies..."
pip install --only-binary :all: psycopg2-binary pandas numpy 2>/dev/null || echo "Dependencies already installed"

echo "Stopping existing containers..."
docker-compose -f docker-compose.yml down -v 2>/dev/null || echo "No existing containers to stop"

echo "Cleaning up old volumes..."
docker volume rm src_postgres-data 2>/dev/null || echo "No postgres volume to remove"
docker volume rm src_redis-data 2>/dev/null || echo "No redis volume to remove"
docker volume rm src_spark-logs 2>/dev/null || echo "No spark logs volume to remove"
docker volume rm src_mlflow-artifacts 2>/dev/null || echo "No mlflow volume to remove"

echo "Creating necessary directories..."
mkdir -p data logs models config

echo "Building all services..."
docker-compose -f docker-compose.yml build --no-cache
if [ $? -ne 0 ]; then
    echo "Build failed. Please check the error messages above."
    exit 1
fi

echo "Starting infrastructure services..."
docker-compose -f docker-compose.yml up -d postgres redis spark-master spark-worker

echo "Waiting for infrastructure services to be ready..."
sleep 45

echo "Checking PostgreSQL connection..."
for i in {1..10}; do
    if docker-compose -f docker-compose.yml exec -T postgres pg_isready -U mluser -h localhost -p 5432 >/dev/null 2>&1; then
        echo "PostgreSQL is ready"
        break
    else
        echo "Waiting for PostgreSQL... ($i/10)"
        sleep 5
    fi

    if [ $i -eq 10 ]; then
        echo "PostgreSQL failed to start. Check logs:"
        docker-compose -f docker-compose.yml logs postgres
        exit 1
    fi
done

echo "Initializing database schema..."
python init_database.py
if [ $? -ne 0 ]; then
    echo "Database initialization failed"
    exit 1
fi

echo "Starting MLflow tracking server..."
docker-compose -f docker-compose.yml up -d mlflow

echo "Waiting for MLflow to be ready..."
sleep 30

echo "Checking service status..."
docker-compose -f docker-compose.yml ps

echo ""
echo "Starting data processing services..."
docker-compose -f docker-compose.yml up -d data-generator spark-processor

sleep 15

echo "Starting ML training services..."
docker-compose -f docker-compose.yml up -d celery-worker celery-beat celery-flower

echo "Final service status check..."
docker-compose -f docker-compose.yml ps

echo ""
echo "Checking data generation logs..."
sleep 10
docker-compose -f docker-compose.yml logs --tail=20 data-generator

echo ""
echo "Phase 1 Complete ML Pipeline Started Successfully"
echo "============================================================"
echo ""
echo "Web Interfaces:"
echo "  - Spark Master UI:    http://localhost:8080"
echo "  - Celery Flower:      http://localhost:5555"
echo "  - MLflow Tracking:    http://localhost:5000"
echo ""
echo "Real-time Monitoring Commands:"
echo "  - Data Generation:    docker-compose -f docker-compose.cloud.yml logs -f data-generator"
echo "  - Spark Processing:   docker-compose -f docker-compose.cloud.yml logs -f spark-processor"
echo "  - ML Training:        docker-compose -f docker-compose.cloud.yml logs -f celery-worker"
echo "  - Database Check:     docker-compose -f docker-compose.cloud.yml exec postgres psql -U mluser -d fraud_detection -c \"SELECT COUNT(*) FROM transactions;\""
echo ""
echo "Confluent Cloud:"
echo "  - Kafka Cluster: pkc-619z3.us-east1.gcp.confluent.cloud:9092"
echo "  - Monitor: https://confluent.cloud"
echo ""
echo "Expected Results (within 15 minutes):"
echo "  - 100,000 fraud transactions generated"
echo "  - Streaming to Confluent Cloud Kafka"
echo "  - 20+ features engineered by Spark"
echo "  - XGBoost model trained with SMOTE"
echo "  - MLflow experiments logged"
echo "  - Model retraining every 6 hours"
echo ""
echo "Pipeline Status:"
echo "  - Data Generator → Confluent Cloud Kafka"
echo "  - Spark Processor → Kafka → PostgreSQL"
echo "  - Celery Workers → ML Training → MLflow"
echo "  - All services running using your .env config"
echo ""
echo "Checking if transactions are being processed..."
sleep 20

echo "Current transaction count:"
docker-compose -f docker-compose.yml exec -T postgres psql -U mluser -d fraud_detection -c "SELECT COUNT(*) as total_transactions FROM transactions;" 2>/dev/null || echo "Database still initializing..."

echo ""
echo "Phase 1 startup complete. Monitor logs to see the pipeline in action."
