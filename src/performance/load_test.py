"""
FraudDetectX Performance Load Tester
Simulates high volume transaction processing
"""
import os
import time
import json
import random
import uuid
import threading
import queue
import argparse
import concurrent.futures
from datetime import datetime, timedelta
import numpy as np
from kafka import KafkaProducer
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LoadTester:
    def __init__(self, target_tps=1000, duration_seconds=60, ramp_up_seconds=10):
        """Initialize load tester"""
        self.target_tps = target_tps
        self.duration_seconds = duration_seconds
        self.ramp_up_seconds = ramp_up_seconds
        self.results_queue = queue.Queue()

        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
            'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN'),
            'sasl_plain_username': os.getenv('KAFKA_SASL_USERNAME'),
            'sasl_plain_password': os.getenv('KAFKA_SASL_PASSWORD'),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'linger_ms': 5,
            'batch_size': 65536,
            'compression_type': 'lz4'
        }
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')

        # Database configuration
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'dbname': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

    def generate_transaction(self, user_id=None, is_fraud=None):
        """Generate a single transaction"""
        # Generate random user if not provided
        if user_id is None:
            user_id = f"user-{random.randint(1, 10000)}"

        # Determine fraud status if not provided (0.5% fraud rate)
        if is_fraud is None:
            is_fraud = random.random() < 0.005

        # Generate transaction amount based on fraud status
        if is_fraud:
            # Fraudulent transactions tend to be unusual amounts
            amount = random.choice([
                round(random.uniform(1, 10), 2),  # Very small amount
                round(random.uniform(1000, 5000), 2),  # Large amount
                round(random.uniform(100, 500), 0)  # Suspiciously round amount
            ])
        else:
            # Normal transactions follow log-normal distribution
            amount = round(np.random.lognormal(mean=4, sigma=1), 2)

        # Generate transaction
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user_id,
            'amount': amount,
            'timestamp': datetime.now().isoformat(),
            'merchant_category': random.choice([
                'retail', 'restaurant', 'online', 'travel',
                'gas_station', 'grocery', 'entertainment'
            ]),
            'is_fraud': is_fraud
        }

        return transaction

    def producer_thread(self, thread_id, stop_event, tps_per_thread):
        """Producer thread function"""
        try:
            producer = KafkaProducer(**self.kafka_config)

            # Track metrics
            sent_count = 0
            start_time = time.time()
            last_report_time = start_time

            while not stop_event.is_set():
                current_time = time.time()
                elapsed = current_time - start_time

                # Calculate how many messages should have been sent by now
                target_count = min(
                    int(tps_per_thread * elapsed),
                    int(tps_per_thread * self.duration_seconds)
                )

                # Send any needed messages to catch up
                while sent_count < target_count and not stop_event.is_set():
                    transaction = self.generate_transaction()
                    future = producer.send(self.topic, transaction)

                    try:
                        future.get(timeout=0.1)  # Short timeout
                        sent_count += 1

                        # Report progress every second
                        if current_time - last_report_time >= 1:
                            self.results_queue.put({
                                'thread_id': thread_id,
                                'timestamp': time.time(),
                                'sent_count': sent_count,
                                'elapsed': current_time - start_time
                            })
                            last_report_time = current_time

                    except Exception as e:
                        logger.error(f"Thread {thread_id} error sending message: {e}")

                # Avoid busy waiting
                if sent_count >= target_count:
                    time.sleep(0.01)

            # Final report
            self.results_queue.put({
                'thread_id': thread_id,
                'timestamp': time.time(),
                'sent_count': sent_count,
                'elapsed': time.time() - start_time,
                'final': True
            })

            producer.close()
            logger.info(f"Thread {thread_id} completed: {sent_count} transactions in {time.time() - start_time:.2f}s")

        except Exception as e:
            logger.error(f"Thread {thread_id} failed: {e}")

    def monitor_thread(self, stop_event, target_tps, thread_count):
        """Monitor thread to collect and display results"""
        results = []
        thread_final_counts = {}

        # Set up plot
        plt.figure(figsize=(10, 6))
        plt.title(f'FraudDetectX Load Test - Target: {target_tps} TPS')
        plt.xlabel('Time (s)')
        plt.ylabel('Transactions Per Second')
        plt.grid(True)

        start_time = time.time()
        x_data, y_data = [], []

        try:
            while not stop_event.is_set() or not all(t in thread_final_counts for t in range(thread_count)):
                try:
                    # Collect data with timeout
                    result = self.results_queue.get(timeout=0.5)
                    results.append(result)

                    # Store final counts
                    if result.get('final', False):
                        thread_final_counts[result['thread_id']] = result['sent_count']

                    # Update real-time plot
                    if len(results) % 10 == 0:
                        df = pd.DataFrame(results)
                        if not df.empty:
                            df['relative_time'] = df['timestamp'] - start_time
                            # Group by second and sum
                            tps_data = df.groupby(df['relative_time'].astype(int))['sent_count'].diff().fillna(df['sent_count'])
                            tps_by_second = tps_data.groupby(df['relative_time'].astype(int)).sum()

                            x_data = list(tps_by_second.index)
                            y_data = list(tps_by_second.values)

                            plt.clf()
                            plt.plot(x_data, y_data, 'b-')
                            plt.axhline(y=target_tps, color='r', linestyle='--', label=f'Target: {target_tps} TPS')
                            plt.legend()
                            plt.title(f'FraudDetectX Load Test - Target: {target_tps} TPS')
                            plt.xlabel('Time (s)')
                            plt.ylabel('Transactions Per Second')
                            plt.grid(True)
                            plt.pause(0.01)

                except queue.Empty:
                    continue

        except Exception as e:
            logger.error(f"Monitor thread error: {e}")

        finally:
            # Save final results
            if results:
                total_sent = sum(thread_final_counts.values()) if thread_final_counts else sum(r['sent_count'] for r in results if r.get('final', False))
                total_time = time.time() - start_time
                average_tps = total_sent / total_time if total_time > 0 else 0

                logger.info(f"Load test completed: {total_sent} transactions in {total_time:.2f}s")
                logger.info(f"Average throughput: {average_tps:.2f} TPS")

                # Create final plot
                plt.figure(figsize=(12, 8))
                plt.plot(x_data, y_data, 'b-', label='Actual TPS')
                plt.axhline(y=target_tps, color='r', linestyle='--', label=f'Target: {target_tps} TPS')
                plt.axhline(y=average_tps, color='g', linestyle='-.', label=f'Average: {average_tps:.2f} TPS')
                plt.title(f'FraudDetectX Load Test Results - {total_sent} Transactions')
                plt.xlabel('Time (s)')
                plt.ylabel('Transactions Per Second')
                plt.legend()
                plt.grid(True)
                plt.savefig(f'load_test_results_{int(time.time())}.png')

                # Save CSV data
                if results:
                    pd.DataFrame(results).to_csv(f'load_test_raw_{int(time.time())}.csv', index=False)

                # Check database counts
                try:
                    conn = psycopg2.connect(**self.db_params)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM transactions WHERE created_at >= %s", (datetime.now() - timedelta(hours=1),))
                    db_count = cursor.fetchone()[0]
                    logger.info(f"Database transaction count (last hour): {db_count}")
                    logger.info(f"Kafka to database ratio: {db_count/total_sent:.2%}")
                    conn.close()
                except Exception as e:
                    logger.error(f"Could not check database counts: {e}")

    def run_load_test(self, thread_count=4):
        """Run a load test with multiple threads"""
        logger.info(f"Starting load test: {self.target_tps} TPS for {self.duration_seconds}s using {thread_count} threads")

        # Calculate TPS per thread
        tps_per_thread = self.target_tps / thread_count

        # Create stop event
        stop_event = threading.Event()

        # Start producer threads
        threads = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Start producer threads
            for i in range(thread_count):
                future = executor.submit(self.producer_thread, i, stop_event, tps_per_thread)
                threads.append(future)

            # Start monitor thread
            monitor_future = executor.submit(self.monitor_thread, stop_event, self.target_tps, thread_count)

            # Wait for duration
            time.sleep(self.duration_seconds)

            # Signal threads to stop
            logger.info("Test duration completed, stopping threads")
            stop_event.set()

            # Wait for all threads
            concurrent.futures.wait(threads)

            # Wait for monitor to finish
            monitor_future.result()

        logger.info("Load test completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='FraudDetectX Load Tester')
    parser.add_argument('--tps', type=int, default=1000, help='Target transactions per second')
    parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
    parser.add_argument('--threads', type=int, default=4, help='Number of producer threads')
    parser.add_argument('--ramp-up', type=int, default=10, help='Ramp-up time in seconds')

    args = parser.parse_args()

    tester = LoadTester(
        target_tps=args.tps,
        duration_seconds=args.duration,
        ramp_up_seconds=args.ramp_up
    )

    tester.run_load_test(thread_count=args.threads)