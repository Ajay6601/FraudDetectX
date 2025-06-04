"""
Task Processor for Spark Feature Engineering
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

class TaskProcessor:
    """Process tasks from the task queue"""

    def __init__(self):
        """Initialize the task processor"""
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'dbname': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

        # Create task table if it doesn't exist
        self._ensure_task_table()

    def _ensure_task_table(self):
        """Ensure the processing_tasks table exists"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            cursor = conn.cursor()

            # Create table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_tasks (
                id SERIAL PRIMARY KEY,
                task_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                parameters JSONB,
                result JSONB
            );
            
            CREATE INDEX IF NOT EXISTS idx_processing_tasks_type_status 
            ON processing_tasks (task_type, status);
            
            CREATE INDEX IF NOT EXISTS idx_processing_tasks_created_at 
            ON processing_tasks (created_at);
            """)

        except Exception as e:
            logger.error(f"Error creating task table: {e}")

        finally:
            if conn:
                conn.close()

    def check_feature_processing_tasks(self):
        """Check for and process feature processing tasks"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = False
            cursor = conn.cursor()

            # Find pending tasks
            cursor.execute("""
            SELECT id, parameters 
            FROM processing_tasks 
            WHERE task_type = 'feature_processing' 
            AND status = 'pending'
            ORDER BY created_at 
            LIMIT 1
            FOR UPDATE SKIP LOCKED;
            """)

            task = cursor.fetchone()
            if task:
                task_id, parameters_str = task
                parameters = json.loads(parameters_str) if parameters_str else {}

                # Update status to running
                cursor.execute("""
                UPDATE processing_tasks 
                SET status = 'running', started_at = CURRENT_TIMESTAMP 
                WHERE id = %s;
                """, (task_id,))
                conn.commit()

                logger.info(f"Starting feature processing task {task_id}")

                try:
                    # Here you would call your existing feature processing code
                    # This is just a placeholder - replace with your actual code
                    from batch_feature_processor import process_features
                    result = process_features()  # Your existing processing function

                    # For this example, we'll just simulate processing
                    logger.info("Processing features...")
                    time.sleep(10)  # Simulate work

                    processed_count = 1000  # Replace with actual result

                    # Update task to completed
                    cursor.execute("""
                    UPDATE processing_tasks 
                    SET status = 'completed', 
                        completed_at = CURRENT_TIMESTAMP,
                        result = %s
                    WHERE id = %s;
                    """, (json.dumps({"processed_count": processed_count}), task_id))
                    conn.commit()

                    logger.info(f"Completed feature processing task {task_id}")

                except Exception as e:
                    logger.error(f"Error processing task {task_id}: {e}")

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
            logger.error(f"Error checking for tasks: {e}")
            if conn and not conn.closed:
                conn.rollback()
            return False

        finally:
            if conn and not conn.closed:
                conn.close()

    def run_forever(self, check_interval=30):
        """Run task processor forever, checking at regular intervals"""
        logger.info(f"Starting task processor with {check_interval}s check interval")

        while True:
            try:
                task_processed = self.check_feature_processing_tasks()
                if not task_processed:
                    # If no task was processed, wait before checking again
                    time.sleep(check_interval)
                # If a task was processed, check immediately for another one
            except Exception as e:
                logger.error(f"Error in task processor main loop: {e}")
                time.sleep(check_interval)

if __name__ == "__main__":
    processor = TaskProcessor()
    processor.run_forever()