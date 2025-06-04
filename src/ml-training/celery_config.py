import os

worker_max_tasks_per_child = None  # Don't shut down after task completion
worker_max_memory_per_child = 0    # No memory limit
worker_cancel_long_running_tasks_on_connection_loss = False  # Don't cancel on connection problems

# Broker settings
broker_url = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')

# Task settings
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'
timezone = 'UTC'
enable_utc = True

# Worker settings
worker_prefetch_multiplier = 1
task_acks_late = True
worker_max_tasks_per_child = 50

# Route settings
task_routes = {
    'train_fraud_detection_model': {'queue': 'ml_training'},
    'evaluate_model_performance': {'queue': 'ml_evaluation'},
    'cleanup_old_models': {'queue': 'maintenance'}
}

# Result settings
result_expires = 3600  # 1 hour
result_cache_max = 10000

# Error handling
task_reject_on_worker_lost = True
task_ignore_result = False