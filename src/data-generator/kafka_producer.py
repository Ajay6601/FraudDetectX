# data-generator/kafka_producer.py
import json
import time
import threading
from typing import List, Dict
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import psycopg2
import psycopg2.extras
from loguru import logger
from generate_transactions import TransactionGenerator

class TransactionStreamer:
    def __init__(self):
        # Kafka configuration - supports both local and cloud
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'transactions')

        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

        # Streaming configuration
        self.streaming_rate = int(os.getenv('STREAMING_RATE', 100))  # TPS
        self.num_users = int(os.getenv('NUM_USERS', 8000))
        self.total_transactions = int(os.getenv('NUM_TRANSACTIONS', 100000))

        # Initialize components
        self.producer = None
        self.generator = None
        self.db_connection = None

        # Statistics
        self.sent_count = 0
        self.error_count = 0
        self.start_time = None

        # Detect if using cloud Kafka
        self.is_cloud_kafka = os.getenv('KAFKA_SECURITY_PROTOCOL') == 'SASL_SSL'

        if self.is_cloud_kafka:
            logger.info(f"Using Confluent Cloud Kafka: {self.kafka_servers}")
        else:
            logger.info(f"Using local Kafka: {self.kafka_servers}")

    def _create_kafka_producer(self) -> KafkaProducer:
        """Create and configure Kafka producer for cloud or local"""
        try:
            # Base configuration
            config = {
                'bootstrap_servers': self.kafka_servers,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                'acks': 'all',  # Wait for all replicas
                'retries': 3,
                'batch_size': 16384,
                'linger_ms': 10,  # Small delay to batch messages
                'buffer_memory': 33554432,
                'compression_type': 'gzip'
            }

            # Add cloud-specific configuration if using Confluent Cloud
            if self.is_cloud_kafka:
                config.update({
                    'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
                    'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
                    'sasl_plain_username': os.getenv('KAFKA_SASL_USERNAME'),
                    'sasl_plain_password': os.getenv('KAFKA_SASL_PASSWORD'),
                })
                logger.info("Configured for Confluent Cloud (SASL_SSL)")

            producer = KafkaProducer(**config)
            logger.info(f"Kafka producer connected successfully")
            return producer

        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def _create_db_connection(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _save_transaction_to_db(self, transaction: Dict):
        """Save transaction to PostgreSQL"""
        try:
            cursor = self.db_connection.cursor()

            insert_query = """
                INSERT INTO transactions (
                    transaction_id, user_id, amount, timestamp, 
                    merchant_category, is_fraud
                ) VALUES (
                    %(transaction_id)s, %(user_id)s, %(amount)s, 
                    %(timestamp)s, %(merchant_category)s, %(is_fraud)s
                )
                ON CONFLICT (transaction_id) DO NOTHING
            """

            cursor.execute(insert_query, transaction)
            cursor.close()

        except Exception as e:
            logger.error(f"Failed to save transaction to DB: {e}")
            # Reconnect if connection lost
            try:
                self.db_connection = self._create_db_connection()
            except:
                pass

    def _delivery_callback(self, err, msg):
        """Callback for Kafka message delivery"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
            self.error_count += 1
        else:
            self.sent_count += 1
            if self.sent_count % 1000 == 0:
                elapsed = time.time() - self.start_time
                rate = self.sent_count / elapsed if elapsed > 0 else 0
                kafka_type = "Cloud" if self.is_cloud_kafka else "Local"
                logger.info(f'Sent {self.sent_count} transactions to {kafka_type} Kafka (Rate: {rate:.1f} TPS)')

    def send_transaction(self, transaction: Dict):
        """Send single transaction to Kafka and save to DB"""
        try:
            # Send to Kafka (cloud or local)
            future = self.producer.send(
                self.kafka_topic,
                key=transaction['transaction_id'],
                value=transaction
            )

            # Add callback
            future.add_callback(lambda metadata: self._delivery_callback(None, metadata))
            future.add_errback(lambda err: self._delivery_callback(err, None))

            # Save to database
            self._save_transaction_to_db(transaction)

        except Exception as e:
            logger.error(f"Failed to send transaction: {e}")
            self.error_count += 1

    def test_connectivity(self) -> bool:
        """Test Kafka connectivity"""
        try:
            logger.info("Testing Kafka connectivity...")

            # Create test producer
            test_producer = self._create_kafka_producer()

            # Send test message
            test_message = {
                'test': 'connectivity',
                'timestamp': datetime.now().isoformat(),
                'kafka_type': 'cloud' if self.is_cloud_kafka else 'local'
            }

            future = test_producer.send(
                self.kafka_topic,
                key='test',
                value=test_message
            )

            # Wait for message to be sent (with timeout)
            future.get(timeout=30)
            test_producer.close()

            logger.info("✅ Kafka connectivity test passed")
            return True

        except Exception as e:
            logger.error(f"❌ Kafka connectivity test failed: {e}")
            return False

    def stream_historical_data(self):
        """Stream pre-generated historical transactions"""
        kafka_type = "Confluent Cloud" if self.is_cloud_kafka else "Local"
        logger.info(f"Starting historical data streaming to {kafka_type}...")

        # Generate historical transactions
        transactions = self.generator.generate_batch(
            self.total_transactions,
            start_date=datetime.now() - timedelta(days=180)
        )

        # Calculate delay between messages to achieve target TPS
        delay = 1.0 / self.streaming_rate if self.streaming_rate > 0 else 0

        self.start_time = time.time()

        for i, transaction in enumerate(transactions):
            self.send_transaction(transaction)

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

            # Periodic flush
            if i % 100 == 0:
                self.producer.flush()

        # Final flush
        self.producer.flush()

        elapsed = time.time() - self.start_time
        actual_rate = self.sent_count / elapsed if elapsed > 0 else 0

        logger.info(f"Historical streaming to {kafka_type} completed:")
        logger.info(f"  - Total sent: {self.sent_count}")
        logger.info(f"  - Errors: {self.error_count}")
        logger.info(f"  - Duration: {elapsed:.1f} seconds")
        logger.info(f"  - Average rate: {actual_rate:.1f} TPS")

    def stream_realtime_data(self):
        """Stream real-time transactions continuously"""
        kafka_type = "Confluent Cloud" if self.is_cloud_kafka else "Local"
        logger.info(f"Starting real-time data streaming to {kafka_type}...")

        delay = 1.0 / self.streaming_rate if self.streaming_rate > 0 else 0
        self.start_time = time.time()

        try:
            while True:
                # Generate transaction with current timestamp
                user = self.generator.users[self.sent_count % len(self.generator.users)]
                transaction = self.generator.generate_transaction(user, datetime.now())

                self.send_transaction(transaction)

                # Rate limiting
                if delay > 0:
                    time.sleep(delay)

                # Periodic flush and statistics
                if self.sent_count % 100 == 0:
                    self.producer.flush()
                    elapsed = time.time() - self.start_time
                    rate = self.sent_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Real-time streaming to {kafka_type} - Sent: {self.sent_count}, Rate: {rate:.1f} TPS")

        except KeyboardInterrupt:
            logger.info("Stopping real-time streaming...")

    def start(self, mode: str = 'historical'):
        """Start the transaction streamer"""
        try:
            # Test connectivity first
            if not self.test_connectivity():
                raise Exception("Kafka connectivity test failed")

            # Initialize components
            self.producer = self._create_kafka_producer()
            self.generator = TransactionGenerator(
                num_users=self.num_users,
                fraud_rate=float(os.getenv('FRAUD_RATE', 0.005))
            )
            self.db_connection = self._create_db_connection()

            # Start streaming based on mode
            if mode == 'historical':
                self.stream_historical_data()
            elif mode == 'realtime':
                self.stream_realtime_data()
            elif mode == 'both':
                # First stream historical data, then switch to real-time
                self.stream_historical_data()
                time.sleep(5)  # Brief pause
                self.stream_realtime_data()
            else:
                logger.error(f"Invalid mode: {mode}. Use 'historical', 'realtime', or 'both'")

        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            kafka_type = "Cloud" if self.is_cloud_kafka else "Local"
            logger.info(f"{kafka_type} Kafka producer closed")

        if self.db_connection:
            self.db_connection.close()
            logger.info("Database connection closed")

# Health check and monitoring
class StreamerMonitor:
    def __init__(self, streamer: TransactionStreamer):
        self.streamer = streamer
        self.monitoring = True

    def start_monitoring(self):
        """Start monitoring thread"""
        monitor_thread = threading.Thread(target=self._monitor_loop)
        monitor_thread.daemon = True
        monitor_thread.start()
        logger.info("Monitoring started")

    def _monitor_loop(self):
        """Monitor streaming performance"""
        while self.monitoring:
            time.sleep(30)  # Check every 30 seconds

            if self.streamer.start_time:
                elapsed = time.time() - self.streamer.start_time
                rate = self.streamer.sent_count / elapsed if elapsed > 0 else 0
                error_rate = self.streamer.error_count / max(1, self.streamer.sent_count) * 100
                kafka_type = "Cloud" if self.streamer.is_cloud_kafka else "Local"

                logger.info(f"Monitor ({kafka_type}) - Sent: {self.streamer.sent_count}, "
                            f"Rate: {rate:.1f} TPS, "
                            f"Errors: {error_rate:.2f}%")

    def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False

# Main execution
if __name__ == "__main__":
    import sys

    # Configuration from environment or command line
    mode = sys.argv[1] if len(sys.argv) > 1 else 'historical'

    # Initialize streamer
    streamer = TransactionStreamer()

    # Start monitoring
    monitor = StreamerMonitor(streamer)
    monitor.start_monitoring()

    try:
        # Start streaming
        streamer.start(mode=mode)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        monitor.stop_monitoring()
        streamer.cleanup()