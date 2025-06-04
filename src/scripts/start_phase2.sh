#!/bin/bash
# scripts/start_phase2.sh

# Start services
echo "🚀 Starting core infrastructure..."
docker-compose up -d postgres redis

echo "⏳ Waiting for core services to be ready..."
sleep 30

echo "🚀 Starting Spark cluster..."
docker-compose up -d spark-master spark-worker

docker-compose exec postgres python /scripts/init_mlflow_db.py

echo "🚀 Starting MLflow..."
docker-compose up -d mlflow

echo "⏳ Waiting for MLflow to be ready..."
sleep 20

echo "🚀 Starting monitoring stack..."
docker-compose up -d prometheus grafana postgres-exporter redis-exporter node-exporter

echo "🚀 Starting Airflow components..."
docker-compose up -d airflow-postgres
sleep 20
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

echo "🚀 Starting ML services..."
docker-compose up -d celery-worker celery-beat celery-flower

echo "🚀 Starting data processing pipeline..."
docker-compose up -d spark-processor

echo "✅ FraudDetectX Phase 2 stack is now running!"
echo ""
echo "📊 Access points:"
echo "Spark Master UI:    http://localhost:8080"
echo "MLflow UI:          http://localhost:5000"
echo "Airflow Webserver:  http://localhost:8088"
echo "Celery Flower:      http://localhost:5555"
echo "Prometheus:         http://localhost:9090"
echo "Grafana:            http://localhost:3000"
echo ""
echo "💡 Default credentials:"
echo "Grafana:  admin / admin"
echo "Airflow:  admin / admin"