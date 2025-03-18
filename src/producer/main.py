import json
import os
import signal
import random
import time
from datetime import timezone, datetime, timedelta
from typing import Optional, Dict, Any
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging
from faker import Faker
from jsonschema import validate,ValidationError, FormatChecker
from typing import Dict, Any
# from pydantic import ValidationError
# from pylint.checkers.format import FormatChecker

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)
logger=logging.getLogger(__name__)

load_dotenv(dotenv_path="/app/.env")

fake=Faker()

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "number", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 10000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "location": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp",'is_fraud']
}


def validate_transaction(transaction:Dict[str,Any]) -> bool:
    try:
        validate(
            instance=transaction,
            schema=TRANSACTION_SCHEMA,
            format_checker=FormatChecker()
        )
        return True
    except ValidationError as e:
        logger.error(f'Invalid transaction:{e.message}')
        return False


def delivery_report(err, msg):
    """Usage: Callback function for message delivery reports"""

    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]!")


class TransactionProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME')
        self.kafka_password = os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')

        self.running = False

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'transaction-producer',
            'compression.type': 'gzip',
            'linger.ms': '5',
            'batch.size': 16384,
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password,
            })
        else:
            self.producer_config['security.protocol']='PLAINTEXT'
        try:
            self.producer=Producer(self.producer_config)
            logger.info("Confluent kafka producer initialized successfully")
        except Exception as e:
            logger.info('Failed to initialize confluent kafka producer:{}'.format(e))
            raise e

        self.compromised_users = set(random.sample(range(1000, 9999), k=50))  # 0.5%
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,  # 40% of fraud cases
            'card_testing': 0.3,  # 30% of fraud causes
            'merchant_collusion': 0.2,  # 20%
            'geo_anomaly': 0.1  # 10%
        }

        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    # def run_continuous_production(self, interval: float = 0.0):
    #     """Run continuous message production with graceful shutdown"""
    #
    #     self.running = True
    #     logger.info('Starting producer for topic %s...', self.topic)
    #
    #     try:
    #         while self.running:
    #             if self.send_transaction():
    #                 time.sleep(interval)
    #     finally:
    #         self.shutdown()

    def run_continuous_production(self, interval: float = 1.0):  # ✅ Ensure interval > 0
        """Run continuous message production with graceful shutdown"""
        self.running = True
        logger.info('Starting producer for topic %s...', self.topic)

        try:
            while self.running:
                logger.info("Looping... sending transaction")  # ✅ Add logging
                if self.send_transaction():
                    time.sleep(interval)  # ✅ Ensure interval > 0
        finally:
            self.shutdown()

    # def send_transaction(self)->bool:
    #     try:
    #         transaction=self.generate_transaction()
    #         if not transaction:
    #             return False
    #         self.producer.produce(
    #             self.topic,
    #             key=transaction['transaction_id'],
    #             value=json.dumps(transaction),
    #             callback=delivery_report
    #         )
    #
    #         self.producer.poll(0)
    #         return True
    #     except Exception as e:
    #         logger.error('Error producing message:{}'.format(e))
    #         return False

    def send_transaction(self) -> bool:
        try:
            logger.info("Attempting to send a transaction...")  # ✅ Add logging
            transaction = self.generate_transaction()

            if not transaction:
                logger.warning("No transaction generated.")  # ✅ Add warning log
                return False

            # ✅ Add validation check before sending
            if not validate_transaction(transaction):
                logger.error("Skipping invalid transaction")
                return False

            logger.info(f"Producing transaction: {json.dumps(transaction, indent=2)}")  # ✅ Log full transaction

            self.producer.produce(
                self.topic,
                key=transaction['transaction_id'],
                value=json.dumps(transaction),
                callback=delivery_report
            )

            self.producer.poll(0)  # Ensures messages are processed
            return True
        except Exception as e:
            logger.error(f'Error producing message: {e}')
            return False

    def generate_transaction(self) -> Optional[Dict[str,Any]]:
        transaction = {
            'transaction_id': fake.uuid4(),
            'user_id': random.randint(1000, 9999),
            'amount': round(fake.pyfloat(min_value=0.01, max_value=10000), 2),
            'currency': 'USD',
            'merchant': fake.company(),
            'timestamp': (datetime.now(timezone.utc) +
                          timedelta(seconds=random.randint(-300, 3000))).isoformat(),
            'location': fake.country_code(),
            'is_fraud': 0
        }
        logger.info(f"Generated Transaction (before fraud checks): {json.dumps(transaction, indent=2)}")  # ✅ Log before validation

        # Extract transaction details
        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']

        # Account takeover detection
        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3:  # 30% chance of fraud in compromised accounts
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)
                transaction['merchant'] = random.choice(self.high_risk_merchants)

        # Card testing
        if not is_fraud and amount < 2.0:
            # Simulate rapid small transactions
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2), 2)
                transaction['location'] = 'US'

        # Merchant collusion
        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 3000 and random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = random.uniform(300, 2000)

        # geographic anomalies
        if not is_fraud:
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud = 1
                transaction['location'] = random.choice(['CN', 'RU', 'GB'])

        # Baseline random fraud (0.1 - 0.3%)
        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)

        # ensure that final fraud rate is between 1-2%
        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

        # validate modified transaction
        if validate_transaction(transaction):
            return transaction


    def shutdown(self,signum=None,frame=None):
        if self.running:
            logger.info("Initializing shutdown..")
            self.running=False
            if self.producer:
                self.producer.flush(timeout=30)
                # self.producer.close()
            logger.info('Producer stopped')



if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
