from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'ajaysai',
    'depends_on_past': False,
    'start_date': datetime(year=2025, month=3, day=18),
    # 'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1,
}

def _train_model(**context):
    pass

with DAG(
        dag_id='fraud_detection_training',
        default_args=default_args,
        description='Fraud detection model training pipeline',
        schedule_interval='0 3 * * *',
        catchup=False,
        tags=['fraud', 'ML']
) as dag:
    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Environment is valid!"
        '''
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
        provide_context=True,
    )

    cleanup_task = BashOperator(
        task_id='cleanup_resources',
        bash_command='rm -f /app/tmp/*.pkl',
        trigger_rule='all_done'
    )

    validate_environment>>training_task>>cleanup_task
