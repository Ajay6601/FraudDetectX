"""
Fraud Detection Pipeline DAG - Database Task Queue Approach
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Custom function to check processed data
def check_processed_data(**kwargs):
    """Check if there are unprocessed transactions that need feature extraction"""
    hook = PostgresHook(postgres_conn_id='fraud_db')
    sql = """
    SELECT 
        COUNT(*) as unprocessed_count 
    FROM transactions t
    LEFT JOIN processed_transactions pt ON t.transaction_id = pt.transaction_id
    WHERE pt.transaction_id IS NULL;
    """
    result = hook.get_first(sql)
    unprocessed_count = result[0]

    print(f"Found {unprocessed_count} unprocessed transactions")
    return {'unprocessed_count': unprocessed_count}

# Custom function to get latest model metrics
def get_model_metrics(**kwargs):
    """Fetch and return the latest model metrics from the database"""
    hook = PostgresHook(postgres_conn_id='fraud_db')
    sql = """
    SELECT 
        model_name,
        model_version,
        accuracy::FLOAT,
        precision_score::FLOAT,
        recall_score::FLOAT,
        f1_score::FLOAT,
        auc_roc::FLOAT,
        training_start,
        training_end
    FROM model_training_runs
    ORDER BY training_start DESC
    LIMIT 1;
    """
    result = hook.get_first(sql)

    if not result:
        print("No model metrics found")
        return {'model_found': False}

    model_metrics = {
        'model_found': True,
        'model_name': result[0],
        'model_version': result[1],
        'accuracy': result[2],
        'precision': result[3],
        'recall': result[4],
        'f1_score': result[5],
        'auc_roc': result[6]
    }

    print(f"Latest model metrics: {model_metrics}")
    return model_metrics

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
        'fraud_detection_pipeline',
        default_args=default_args,
        description='End-to-end fraud detection pipeline',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2025, 6, 1),
        catchup=False,
        tags=['fraud', 'ml', 'production'],
) as dag:

    # Create task queue table if it doesn't exist
    create_task_queue = PostgresOperator(
        task_id='create_task_queue',
        postgres_conn_id='fraud_db',
        sql="""
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
        
        CREATE INDEX IF NOT EXISTS idx_processing_tasks_type_status ON processing_tasks (task_type, status);
        CREATE INDEX IF NOT EXISTS idx_processing_tasks_created_at ON processing_tasks (created_at);
        """
    )

    # Validate data quality
    validate_data = PostgresOperator(
        task_id='validate_data',
        postgres_conn_id='fraud_db',
        sql="""
        SELECT 
            COUNT(*)::INTEGER as total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::INTEGER as fraud_count,
            ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::NUMERIC / 
                  NULLIF(COUNT(*), 0) * 100, 2)::FLOAT as fraud_percentage
        FROM transactions 
        WHERE timestamp >= NOW() - INTERVAL '1 day';
        """
    )

    # Check if feature processing is needed
    check_features = PythonOperator(
        task_id='check_features',
        python_callable=check_processed_data,
        provide_context=True,
    )

    # Process features using Task Queue method
    create_feature_task = PostgresOperator(
        task_id='create_feature_task',
        postgres_conn_id='fraud_db',
        sql="""
        -- Create a feature processing task
        INSERT INTO processing_tasks (task_type, parameters)
        VALUES (
            'feature_processing', 
            jsonb_build_object(
                'airflow_run_id', '{{ run_id }}'
            )
        );
        """
    )

    # Wait for feature processing to complete
    wait_for_features = PostgresOperator(
        task_id='wait_for_features',
        postgres_conn_id='fraud_db',
        sql="""
        -- Wait for completion (Airflow will automatically retry this task)
        WITH task AS (
            SELECT id, status
            FROM processing_tasks
            WHERE task_type = 'feature_processing'
            AND parameters->>'airflow_run_id' = '{{ run_id }}'
            ORDER BY created_at DESC
            LIMIT 1
        )
        SELECT 
            CASE 
                WHEN status = 'completed' THEN 'Task completed successfully'
                WHEN status = 'failed' THEN 'Task failed'
                ELSE 'Task still running or pending: ' || status
            END as task_status
        FROM task
        WHERE status IN ('completed', 'failed');
        """
    )

    # Train model task
    create_training_task = PostgresOperator(
        task_id='create_training_task',
        postgres_conn_id='fraud_db',
        sql="""
        INSERT INTO processing_tasks (task_type, parameters)
        VALUES (
            'model_training', 
            jsonb_build_object(
                'algorithm', 'xgboost',
                'use_smote', true,
                'airflow_run_id', '{{ run_id }}'
            )
        );
        """
    )

    # Wait for model training to complete
    wait_for_training = PostgresOperator(
        task_id='wait_for_training',
        postgres_conn_id='fraud_db',
        sql="""
        -- Wait for completion (Airflow will automatically retry this task)
        WITH task AS (
            SELECT id, status
            FROM processing_tasks
            WHERE task_type = 'model_training'
            AND parameters->>'airflow_run_id' = '{{ run_id }}'
            ORDER BY created_at DESC
            LIMIT 1
        )
        SELECT 
            CASE 
                WHEN status = 'completed' THEN 'Task completed successfully'
                WHEN status = 'failed' THEN 'Task failed'
                ELSE 'Task still running or pending: ' || status
            END as task_status
        FROM task
        WHERE status IN ('completed', 'failed');
        """
    )

    # Evaluate model task
    create_evaluation_task = PostgresOperator(
        task_id='create_evaluation_task',
        postgres_conn_id='fraud_db',
        sql="""
        INSERT INTO processing_tasks (task_type, parameters)
        VALUES (
            'model_evaluation', 
            jsonb_build_object(
                'model_version', 'latest',
                'airflow_run_id', '{{ run_id }}'
            )
        );
        """
    )

    # Wait for evaluation to complete
    wait_for_evaluation = PostgresOperator(
        task_id='wait_for_evaluation',
        postgres_conn_id='fraud_db',
        sql="""
        -- Wait for completion (Airflow will automatically retry this task)
        WITH task AS (
            SELECT id, status
            FROM processing_tasks
            WHERE task_type = 'model_evaluation'
            AND parameters->>'airflow_run_id' = '{{ run_id }}'
            ORDER BY created_at DESC
            LIMIT 1
        )
        SELECT 
            CASE 
                WHEN status = 'completed' THEN 'Task completed successfully'
                WHEN status = 'failed' THEN 'Task failed'
                ELSE 'Task still running or pending: ' || status
            END as task_status
        FROM task
        WHERE status IN ('completed', 'failed');
        """
    )

    # Get and log model metrics
    log_metrics = PythonOperator(
        task_id='log_metrics',
        python_callable=get_model_metrics,
        provide_context=True,
    )

    # Define task dependencies
    create_task_queue >> validate_data >> check_features >> create_feature_task >> wait_for_features
    wait_for_features >> create_training_task >> wait_for_training
    wait_for_training >> create_evaluation_task >> wait_for_evaluation
    wait_for_evaluation >> log_metrics