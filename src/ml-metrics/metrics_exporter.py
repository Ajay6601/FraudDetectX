"""
ML Metrics Exporter for FraudDetectX
Exports model metrics and fraud stats to Prometheus
"""
from prometheus_client import start_http_server, Gauge, Summary
import time
import psycopg2
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
MODEL_ACCURACY = Gauge('fraud_model_accuracy', 'ML Model Accuracy')
MODEL_PRECISION = Gauge('fraud_model_precision', 'ML Model Precision')
MODEL_RECALL = Gauge('fraud_model_recall', 'ML Model Recall')
MODEL_F1 = Gauge('fraud_model_f1_score', 'ML Model F1 Score')
MODEL_AUC = Gauge('fraud_model_auc_roc', 'ML Model AUC-ROC')
TRANSACTIONS_COUNT = Gauge('fraud_detection_transactions_count', 'Total Transactions')
FRAUD_COUNT = Gauge('fraud_detection_frauds_count', 'Total Fraud Transactions')
FRAUD_RATE = Gauge('fraud_rate', 'Fraud Rate Percentage')
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')

def update_metrics():
    """Update metrics from database"""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            dbname=os.getenv('POSTGRES_DB', 'fraud_detection'),
            user=os.getenv('POSTGRES_USER', 'mluser'),
            password=os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        )

        cursor = conn.cursor()

        # Get model metrics
        cursor.execute("""
            SELECT 
                accuracy,
                precision_score,
                recall_score,
                f1_score,
                auc_roc
            FROM model_training_runs
            ORDER BY training_start DESC
            LIMIT 1
        """)

        row = cursor.fetchone()
        if row:
            logger.info(f"Got model metrics: accuracy={row[0]}, precision={row[1]}, recall={row[2]}, f1={row[3]}, auc={row[4]}")
            MODEL_ACCURACY.set(row[0])
            MODEL_PRECISION.set(row[1])
            MODEL_RECALL.set(row[2])
            MODEL_F1.set(row[3])
            MODEL_AUC.set(row[4])
        else:
            logger.warning("No model metrics found in database")

        # Get transaction statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
                ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) as fraud_rate
            FROM transactions
        """)

        stats = cursor.fetchone()
        if stats:
            logger.info(f"Got transaction stats: total={stats[0]}, fraud={stats[1]}, rate={stats[2]}%")
            TRANSACTIONS_COUNT.set(stats[0])
            FRAUD_COUNT.set(stats[1])
            FRAUD_RATE.set(stats[2])
        else:
            logger.warning("No transaction stats found in database")

        conn.close()

    except Exception as e:
        logger.error(f"Error updating metrics: {e}")

def main():
    """Main function"""
    # Start metrics server
    metrics_port = int(os.getenv('METRICS_PORT', 9188))
    start_http_server(metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    # Update metrics at regular intervals
    while True:
        try:
            with REQUEST_TIME.time():
                update_metrics()
        except Exception as e:
            logger.error(f"Error in metrics update: {e}")
        time.sleep(15)  # Update every 15 seconds

if __name__ == "__main__":
    main()