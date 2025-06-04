"""
Task Processor for ML Training and Evaluation
"""
import json
import os
import time
import logging
import psycopg2
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MLTaskProcessor:
    """Process ML-related tasks from the task queue"""

    def __init__(self):
        """Initialize the task processor"""
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'dbname': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

    def check_ml_tasks(self):
        """Check for and process ML tasks (training and evaluation)"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False
            cursor = conn.cursor()

            # Find pending tasks for both training and evaluation
            cursor.execute("""
            SELECT id, task_type, parameters 
            FROM processing_tasks 
            WHERE task_type IN ('model_training', 'model_evaluation')
            AND status = 'pending'
            ORDER BY created_at 
            LIMIT 1
            FOR UPDATE SKIP LOCKED;
            """)

            task = cursor.fetchone()
            if task:
                task_id, task_type, parameters_str = task
                parameters = json.loads(parameters_str) if parameters_str else {}

                # Update status to running
                cursor.execute("""
                UPDATE processing_tasks 
                SET status = 'running', started_at = CURRENT_TIMESTAMP 
                WHERE id = %s;
                """, (task_id,))
                conn.commit()

                logger.info(f"Starting {task_type} task {task_id}")

                try:
                    # Process based on task type
                    if task_type == 'model_training':
                        # Import your existing training code
                        from celery_tasks import train_fraud_detection_model
                        result = train_fraud_detection_model.run()

                    elif task_type == 'model_evaluation':
                        # Import your existing evaluation code
                        from celery_tasks import evaluate_model_performance
                        result = evaluate_model_performance.run()

                    # Update task to completed
                    cursor.execute("""
                    UPDATE processing_tasks 
                    SET status = 'completed', 
                        completed_at = CURRENT_TIMESTAMP,
                        result = %s
                    WHERE id = %s;
                    """, (json.dumps(result), task_id))
                    conn.commit()

                    logger.info(f"Completed {task_type} task {task_id}")

                except Exception as e:
                    logger.error(f"Error processing {task_type} task {task_id}: {e}")

                    # Update task to failed
                    cursor.execute("""
                    UPDATE processing_tasks 
                    SET status = 'failed', 
                        completed_at = CURRENT_TIMESTAMP,
                        result = %s
                    WHERE id = %s;
                    """, (json.dumps({"error": str(e)}), task_id))
                    conn.commit()

                return True  # Task was processed

            return False  # No task found

        except Exception as e:
            logger.error(f"Error checking for ML tasks: {e}")
            if conn and not conn.closed:
                conn.rollback()
            return False

        finally:
            if conn and not conn.closed:
                conn.close()

    def run_forever(self, check_interval=30):
        """Run task processor forever, checking at regular intervals"""
        logger.info(f"Starting ML task processor with {check_interval}s check interval")

        while True:
            try:
                task_processed = self.check_ml_tasks()
                if not task_processed:
                    # If no task was processed, wait before checking again
                    time.sleep(check_interval)
                # If a task was processed, check immediately for another one
            except Exception as e:
                logger.error(f"Error in ML task processor main loop: {e}")
                time.sleep(check_interval)

if __name__ == "__main__":
    processor = MLTaskProcessor()
    processor.run_forever()