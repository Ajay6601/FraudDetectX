# spark-processor/kafka_consumer.py
import os
import json
import time
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from feature_engineering import FeatureEngineer
from loguru import logger

class SparkKafkaProcessor:
    def __init__(self):
        # Configuration
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'spark-processor-group')
        self.spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')

        # Processing configuration
        self.batch_duration = int(os.getenv('SPARK_BATCH_DURATION', '30'))  # seconds
        self.checkpoint_location = os.getenv('SPARK_CHECKPOINT_LOCATION', '/app/data/checkpoints')
        self.max_offsets_per_trigger = os.getenv('SPARK_MAX_OFFSETS_PER_TRIGGER', '1000')

        # Detect if using cloud Kafka
        self.is_cloud_kafka = os.getenv('KAFKA_SECURITY_PROTOCOL') == 'SASL_SSL'

        # Initialize Spark session
        self.spark = self._create_spark_session()
        self.feature_engineer = FeatureEngineer(self.spark)

        # Statistics
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()

        kafka_type = "Confluent Cloud" if self.is_cloud_kafka else "Local"
        logger.info(f"SparkKafkaProcessor initialized for {kafka_type} Kafka")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with proper configuration for cloud or local Kafka"""
        try:
            spark = SparkSession.builder \
                .appName("FraudDetectionProcessor") \
                .master(self.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                        "org.postgresql:postgresql:42.6.0") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")

            kafka_type = "cloud" if self.is_cloud_kafka else "local"
            logger.info(f"Spark session created for {kafka_type} Kafka - Master: {self.spark_master}")
            return spark

        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise

    def _get_kafka_stream(self) -> DataFrame:
        """Create Kafka streaming DataFrame for cloud or local"""
        try:
            # Base options
            options = {
                "kafka.bootstrap.servers": self.kafka_servers,
                "subscribe": self.kafka_topic,
                "startingOffsets": "latest",
                "maxOffsetsPerTrigger": self.max_offsets_per_trigger,
                "failOnDataLoss": "false"
            }

            # Add cloud-specific options if using Confluent Cloud
            if self.is_cloud_kafka:
                options.update({
                    "kafka.security.protocol": os.getenv('KAFKA_SECURITY_PROTOCOL'),
                    "kafka.sasl.mechanism": os.getenv('KAFKA_SASL_MECHANISM'),
                    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_SASL_USERNAME")}" password="{os.getenv("KAFKA_SASL_PASSWORD")}";'
                })
                logger.info("Configured Spark for Confluent Cloud (SASL_SSL)")
            else:
                logger.info("Configured Spark for local Kafka")

            # Create stream with all options
            stream_reader = self.spark.readStream.format("kafka")
            for key, value in options.items():
                stream_reader = stream_reader.option(key, value)

            return stream_reader.load()

        except Exception as e:
            logger.error(f"Failed to create Kafka stream: {e}")
            raise

    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """Parse JSON messages from Kafka"""
        # Define the transaction schema
        transaction_schema = self.feature_engineer.get_transaction_schema()

        # Parse the JSON value from Kafka
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        ).withColumn(
            "transaction",
            from_json(col("kafka_value"), transaction_schema)
        ).select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("transaction.*")
        )

        return parsed_df

    def _process_batch_function(self, batch_df: DataFrame, batch_id: int):
        """Process each batch of streaming data"""
        try:
            start_time = time.time()
            batch_count = batch_df.count()

            if batch_count == 0:
                logger.info(f"Batch {batch_id}: No data to process")
                return

            kafka_type = "Cloud" if self.is_cloud_kafka else "Local"
            logger.info(f"Batch {batch_id} ({kafka_type}): Processing {batch_count} transactions")

            # Apply feature engineering
            processed_df = self.feature_engineer.process_batch(batch_df)

            # Save to PostgreSQL
            self.feature_engineer.save_to_postgres(processed_df)

            # Update statistics
            self.processed_count += batch_count
            processing_time = time.time() - start_time
            total_elapsed = time.time() - self.start_time
            avg_rate = self.processed_count / total_elapsed if total_elapsed > 0 else 0

            logger.info(f"Batch {batch_id} ({kafka_type}) completed:")
            logger.info(f"  - Processed: {batch_count} transactions")
            logger.info(f"  - Processing time: {processing_time:.2f}s")
            logger.info(f"  - Total processed: {self.processed_count}")
            logger.info(f"  - Average rate: {avg_rate:.2f} TPS")

            # Log sample of processed data
            if processed_df.count() > 0:
                sample_data = processed_df.limit(3).collect()
                logger.info(f"Sample processed data: {len(sample_data)} records")
                for row in sample_data:
                    logger.info(f"  - Transaction {row['transaction_id']}: "
                                f"Amount=${row['amount']:.2f}, "
                                f"Fraud={row['is_fraud']}, "
                                f"Anomaly_Score={row['anomaly_score']:.2f}")

        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing batch {batch_id}: {e}")
            raise

    def start_streaming(self, mode: str = "continuous"):
        """Start the streaming processing"""
        try:
            kafka_type = "Confluent Cloud" if self.is_cloud_kafka else "Local"
            logger.info(f"Starting Kafka streaming from {kafka_type} in {mode} mode")

            # Get Kafka stream
            kafka_df = self._get_kafka_stream()

            # Parse messages
            parsed_df = self._parse_kafka_messages(kafka_df)

            if mode == "continuous":
                # Continuous streaming with micro-batches
                query = parsed_df.writeStream \
                    .foreachBatch(self._process_batch_function) \
                    .outputMode("append") \
                    .trigger(processingTime=f'{self.batch_duration} seconds') \
                    .option("checkpointLocation", self.checkpoint_location) \
                    .start()

                logger.info(f"Continuous streaming from {kafka_type} started")

                # Wait for termination
                query.awaitTermination()

            elif mode == "batch":
                # Process available data as batch
                self._process_available_data(parsed_df)

            else:
                logger.error(f"Invalid streaming mode: {mode}")

        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            raise
        finally:
            self.cleanup()

    def _process_available_data(self, df: DataFrame):
        """Process available data in batch mode"""
        try:
            kafka_type = "Confluent Cloud" if self.is_cloud_kafka else "Local"
            logger.info(f"Processing available data from {kafka_type} in batch mode")

            # Create base options for batch read
            options = {
                "kafka.bootstrap.servers": self.kafka_servers,
                "subscribe": self.kafka_topic,
                "startingOffsets": "earliest",
                "endingOffsets": "latest"
            }

            # Add cloud-specific options
            if self.is_cloud_kafka:
                options.update({
                    "kafka.security.protocol": os.getenv('KAFKA_SECURITY_PROTOCOL'),
                    "kafka.sasl.mechanism": os.getenv('KAFKA_SASL_MECHANISM'),
                    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_SASL_USERNAME")}" password="{os.getenv("KAFKA_SASL_PASSWORD")}";'
                })

            # Read available data
            stream_reader = self.spark.read.format("kafka")
            for key, value in options.items():
                stream_reader = stream_reader.option(key, value)

            available_df = stream_reader.load()

            if available_df.count() == 0:
                logger.info(f"No data available for batch processing from {kafka_type}")
                return

            # Parse and process
            parsed_df = self._parse_kafka_messages(available_df)
            self._process_batch_function(parsed_df, 0)

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise

    def health_check(self) -> bool:
        """Perform health check"""
        try:
            # Check Spark session
            if self.spark is None:
                return False

            # Create base options for connectivity test
            options = {
                "kafka.bootstrap.servers": self.kafka_servers,
                "subscribe": self.kafka_topic,
                "startingOffsets": "latest",
                "endingOffsets": "latest"
            }

            # Add cloud-specific options
            if self.is_cloud_kafka:
                options.update({
                    "kafka.security.protocol": os.getenv('KAFKA_SECURITY_PROTOCOL'),
                    "kafka.sasl.mechanism": os.getenv('KAFKA_SASL_MECHANISM'),
                    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_SASL_USERNAME")}" password="{os.getenv("KAFKA_SASL_PASSWORD")}";'
                })

            # Test connectivity
            stream_reader = self.spark.read.format("kafka")
            for key, value in options.items():
                stream_reader = stream_reader.option(key, value)

            test_df = stream_reader.load()

            # Simple query to test connectivity
            test_df.count()

            kafka_type = "Cloud" if self.is_cloud_kafka else "Local"
            logger.info(f"Health check passed for {kafka_type} Kafka")
            return True

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        elapsed = time.time() - self.start_time
        kafka_type = "cloud" if self.is_cloud_kafka else "local"

        return {
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'elapsed_time': elapsed,
            'average_rate': self.processed_count / elapsed if elapsed > 0 else 0,
            'success_rate': (self.processed_count / (self.processed_count + self.error_count)) * 100
            if (self.processed_count + self.error_count) > 0 else 100,
            'kafka_type': kafka_type
        }

    def cleanup(self):
        """Clean up resources"""
        try:
            if self.spark:
                self.spark.stop()
                kafka_type = "Cloud" if self.is_cloud_kafka else "Local"
                logger.info(f"Spark session stopped ({kafka_type})")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Main execution
if __name__ == "__main__":
    import sys
    import signal

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        processor.cleanup()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Get mode from command line
    mode = sys.argv[1] if len(sys.argv) > 1 else "continuous"

    # Initialize processor
    processor = SparkKafkaProcessor()

    # Health check
    if not processor.health_check():
        logger.error("Health check failed, exiting")
        sys.exit(1)

    try:
        # Start processing
        processor.start_streaming(mode=mode)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    finally:
        # Print final statistics
        stats = processor.get_statistics()
        logger.info(f"Final statistics: {stats}")
        processor.cleanup()