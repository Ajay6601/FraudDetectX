# spark-processor/spark_config.py
import os
from typing import Dict, Any

class SparkConfig:
    """Configuration management for Spark processor"""

    def __init__(self):
        self.spark_config = self._get_spark_config()
        self.kafka_config = self._get_kafka_config()
        self.processing_config = self._get_processing_config()
        self.database_config = self._get_database_config()

    def _get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration"""
        return {
            'master': os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077'),
            'app_name': os.getenv('SPARK_APP_NAME', 'FraudDetectionProcessor'),
            'executor_memory': os.getenv('SPARK_EXECUTOR_MEMORY', '2g'),
            'executor_cores': int(os.getenv('SPARK_EXECUTOR_CORES', '2')),
            'driver_memory': os.getenv('SPARK_DRIVER_MEMORY', '1g'),
            'max_result_size': os.getenv('SPARK_DRIVER_MAX_RESULT_SIZE', '1g'),
            'sql_adaptive_enabled': os.getenv('SPARK_SQL_ADAPTIVE_ENABLED', 'true'),
            'sql_adaptive_coalesce_partitions': os.getenv('SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS', 'true'),
            'checkpoint_location': os.getenv('SPARK_CHECKPOINT_LOCATION', '/app/data/checkpoints'),
            'log_level': os.getenv('SPARK_LOG_LEVEL', 'WARN')
        }

    def _get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration"""
        return {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'topic': os.getenv('KAFKA_TOPIC', 'transactions'),
            'group_id': os.getenv('KAFKA_GROUP_ID', 'spark-processor-group'),
            'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest'),
            'max_offsets_per_trigger': int(os.getenv('SPARK_MAX_OFFSETS_PER_TRIGGER', '1000')),
            'fail_on_data_loss': os.getenv('KAFKA_FAIL_ON_DATA_LOSS', 'false')
        }

    def _get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return {
            'batch_duration': int(os.getenv('SPARK_BATCH_DURATION', '30')),
            'trigger_once': os.getenv('SPARK_TRIGGER_ONCE', 'false').lower() == 'true',
            'output_mode': os.getenv('SPARK_OUTPUT_MODE', 'append'),
            'watermark_delay': os.getenv('SPARK_WATERMARK_DELAY', '1 minute'),
            'state_timeout': os.getenv('SPARK_STATE_TIMEOUT', '10 minutes')
        }

    def _get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword'),
            'jdbc_url': f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'fraud_detection')}"
        }

    def get_spark_session_config(self) -> Dict[str, str]:
        """Get Spark session configuration as key-value pairs"""
        config = self.spark_config
        return {
            'spark.master': config['master'],
            'spark.app.name': config['app_name'],
            'spark.executor.memory': config['executor_memory'],
            'spark.executor.cores': str(config['executor_cores']),
            'spark.driver.memory': config['driver_memory'],
            'spark.driver.maxResultSize': config['max_result_size'],
            'spark.sql.adaptive.enabled': config['sql_adaptive_enabled'],
            'spark.sql.adaptive.coalescePartitions.enabled': config['sql_adaptive_coalesce_partitions'],
            'spark.sql.streaming.checkpointLocation': config['checkpoint_location'],
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
            'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0'
        }

# Singleton instance
spark_config = SparkConfig()