import os
import sys
from celery import Celery
from celery_config import CeleryConfig
from loguru import logger

def create_celery_app():
    """Create and configure Celery application"""
    app = Celery('fraud_detection_ml')
    app.config_from_object(CeleryConfig)

    # Import tasks
    from celery_tasks import (
        train_fraud_model,
        evaluate_model,
        retrain_model_scheduled,
        evaluate_model_performance,
        health_check
    )

    return app

def start_worker():
    """Start Celery worker"""
    app = create_celery_app()

    # Configure logging
    logger.info("Starting Celery worker for ML tasks")

    # Worker options
    worker_options = {
        'loglevel': os.getenv('CELERY_LOG_LEVEL', 'INFO'),
        'concurrency': int(os.getenv('CELERY_WORKER_CONCURRENCY', '2')),
        'queues': ['ml_training', 'ml_evaluation', 'ml_scheduled', 'default'],
        'hostname': f"ml-worker@{os.getenv('HOSTNAME', 'localhost')}"
    }

    # Start worker
    try:
        app.worker_main(['worker'] + [f'--{k}={v}' for k, v in worker_options.items()])
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_worker()

