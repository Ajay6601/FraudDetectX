import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from faker import Faker
from loguru import logger
import psycopg2
from kafka import KafkaProducer
import os
from dataclasses import dataclass
from enum import Enum

# Configure logging
logger.add("/app/logs/data_generator.log", rotation="100 MB", level="INFO")

fake = Faker()

class FraudType(Enum):
    LEGITIMATE = "legitimate"
    STOLEN_CARD = "stolen_card"
    ACCOUNT_TAKEOVER = "account_takeover"
    CARD_TESTING = "card_testing"
    SYNTHETIC_IDENTITY = "synthetic_identity"
    MERCHANT_COMPROMISE = "merchant_compromise"

@dataclass
class UserProfile:
    user_id: str
    age_group: str
    income_level: str
    location_lat: float
    location_lng: float
    preferred_merchants: List[str]
    preferred_payment_methods: List[str]
    spending_pattern: str
    avg_transaction_amount: float
    transaction_frequency: str
    risk_tolerance: str
    device_ids: List[str]
    ip_addresses: List[str]

class ComprehensiveFraudGenerator:
    def __init__(self):
        self.users = []
        self.merchant_categories = [
            "grocery", "gas_station", "restaurant", "retail", "online",
            "pharmacy", "hotel", "airline", "car_rental", "entertainment",
            "healthcare", "education", "utilities", "insurance", "atm_withdrawal",
            "cash_advance", "gambling", "adult_entertainment", "jewelry",
            "electronics", "home_improvement", "subscription_service"
        ]

        self.payment_methods = [
            "credit_card", "debit_card", "apple_pay", "google_pay",
            "paypal", "venmo", "bank_transfer", "crypto", "buy_now_pay_later"
        ]

        self.high_risk_merchants = [
            "gas_station", "atm_withdrawal", "gambling", "adult_entertainment",
            "cash_advance", "jewelry", "electronics"
        ]

        # Fraud probability by merchant category
        self.merchant_fraud_rates = {
            "gas_station": 0.008,
            "atm_withdrawal": 0.012,
            "gambling": 0.015,
            "adult_entertainment": 0.010,
            "cash_advance": 0.020,
            "jewelry": 0.006,
            "electronics": 0.004,
            "online": 0.003,
            "restaurant": 0.001,
            "grocery": 0.0005,
            "pharmacy": 0.0003,
            "utilities": 0.0001
        }

        # Initialize Kafka producer
        self.kafka_producer = self._create_kafka_producer()
        self.db_connection = self._create_db_connection()

    def _create_kafka_producer(self) -> KafkaProducer:
        config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
            'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN'),
            'sasl_plain_username': os.getenv('KAFKA_SASL_USERNAME'),
            'sasl_plain_password': os.getenv('KAFKA_SASL_PASSWORD'),
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8'),
            'acks': 'all',
            'retries': 3,
            'retry_backoff_ms': 300,
            'request_timeout_ms': 30000,
            'connections_max_idle_ms': 540000,  # 9 minutes
        }

        logger.info("Connecting to Confluent Cloud: {config['bootstrap_servers']}")
        logger.info("Using SASL_SSL authentication")

        try:
            producer = KafkaProducer(**config)

            # Test connectivity with your cloud cluster
            test_message = {
                'test': 'confluent_cloud_connectivity_check',
                'timestamp': datetime.now().isoformat(),
                'cluster': 'pkc-619z3.us-east1.gcp.confluent.cloud'
            }

            future = producer.send(os.getenv('KAFKA_TOPIC', 'transactions'), test_message)
            record_metadata = future.get(timeout=30)  # Longer timeout for cloud

            logger.info("✅ Confluent Cloud connectivity verified")
            logger.info("   Topic: {record_metadata.topic}")
            logger.info("   Partition: {record_metadata.partition}")
            logger.info(" Offset: {record_metadata.offset}")

            return producer

        except Exception as e:
            logger.error("Confluent Cloud connection failed: {e}")
            logger.error("   Check your credentials in .env file")
            logger.error("   Verify topic 'transactions' exists in Confluent Cloud")
            raise

    def _create_db_connection(self):
        """Create PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'fraud_detection'),
                user=os.getenv('POSTGRES_USER', 'mluser'),
                password=os.getenv('POSTGRES_PASSWORD', 'mlpassword')
            )
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return None

    def generate_user_profiles(self, num_users: int = 10000) -> List[UserProfile]:
        """Generate realistic user profiles with diverse characteristics"""
        logger.info(f"Generating {num_users} user profiles...")

        users = []
        for i in range(num_users):
            # Demographics
            age_group = random.choices(
                ["18-25", "26-35", "36-45", "46-55", "56-65", "65+"],
                weights=[15, 25, 25, 20, 10, 5]
            )[0]

            income_level = random.choices(
                ["low", "medium", "high", "very_high"],
                weights=[25, 40, 25, 10]
            )[0]

            # Location (realistic US coordinates)
            location_lat = random.uniform(25.0, 49.0)  # Continental US
            location_lng = random.uniform(-125.0, -66.0)

            # Spending patterns based on demographics
            if income_level == "low":
                avg_amount = random.uniform(15, 75)
                frequency = random.choice(["low", "medium"])
            elif income_level == "medium":
                avg_amount = random.uniform(40, 150)
                frequency = random.choice(["medium", "high"])
            elif income_level == "high":
                avg_amount = random.uniform(100, 500)
                frequency = random.choice(["medium", "high", "very_high"])
            else:  # very_high
                avg_amount = random.uniform(300, 2000)
                frequency = random.choice(["high", "very_high"])

            # Merchant preferences based on demographics
            if age_group in ["18-25", "26-35"]:
                preferred_merchants = random.sample(
                    ["online", "restaurant", "entertainment", "retail", "subscription_service"],
                    k=random.randint(3, 5)
                )
                preferred_payments = ["credit_card", "apple_pay", "venmo", "paypal"]
            elif age_group in ["36-45", "46-55"]:
                preferred_merchants = random.sample(
                    ["grocery", "gas_station", "retail", "healthcare", "utilities"],
                    k=random.randint(3, 5)
                )
                preferred_payments = ["credit_card", "debit_card", "apple_pay"]
            else:
                preferred_merchants = random.sample(
                    ["grocery", "pharmacy", "healthcare", "utilities", "insurance"],
                    k=random.randint(2, 4)
                )
                preferred_payments = ["debit_card", "credit_card"]

            # Device and IP patterns
            num_devices = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
            device_ids = [str(uuid.uuid4()) for _ in range(num_devices)]

            num_ips = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
            ip_addresses = [fake.ipv4() for _ in range(num_ips)]

            user = UserProfile(
                user_id=str(uuid.uuid4()),
                age_group=age_group,
                income_level=income_level,
                location_lat=location_lat,
                location_lng=location_lng,
                preferred_merchants=preferred_merchants,
                preferred_payment_methods=preferred_payments,
                spending_pattern=random.choice(["conservative", "moderate", "aggressive"]),
                avg_transaction_amount=avg_amount,
                transaction_frequency=frequency,
                risk_tolerance=random.choice(["low", "medium", "high"]),
                device_ids=device_ids,
                ip_addresses=ip_addresses
            )

            users.append(user)

            if (i + 1) % 1000 == 0:
                logger.info(f"Generated {i + 1}/{num_users} user profiles")

        logger.info(f"✅ Generated {len(users)} user profiles")
        return users

    def generate_transaction(self, user: UserProfile, base_time: datetime,
                             force_fraud: bool = False, fraud_type: FraudType = None) -> Dict:
        """Generate a single transaction with realistic patterns"""

        # Determine if this should be fraud
        if force_fraud:
            is_fraud = True
            fraud_types = [ft for ft in FraudType if ft != FraudType.LEGITIMATE]
            selected_fraud_type = fraud_type or random.choice(fraud_types) # Exclude LEGITIMATE
        else:
            # Base fraud probability
            base_fraud_prob = 0.005  # 0.5% base rate

            # Adjust based on merchant category
            merchant = random.choice(user.preferred_merchants)
            merchant_fraud_prob = self.merchant_fraud_rates.get(merchant, 0.001)

            # Adjust based on time (higher risk at night)
            hour = base_time.hour
            time_multiplier = 1.5 if 22 <= hour or hour <= 6 else 1.0

            # Adjust based on amount (higher amounts more likely to be fraud)
            amount_base = max(5, np.random.lognormal(
                mean=np.log(user.avg_transaction_amount),
                sigma=0.8
            ))
            amount_multiplier = 1.0 + (amount_base / 1000)  # Higher amounts = higher risk

            final_fraud_prob = base_fraud_prob * merchant_fraud_prob * time_multiplier * amount_multiplier

            is_fraud = random.random() < final_fraud_prob
            selected_fraud_type = FraudType.LEGITIMATE

            if is_fraud:
                selected_fraud_type = random.choices(
                    [FraudType.STOLEN_CARD, FraudType.ACCOUNT_TAKEOVER,
                     FraudType.CARD_TESTING, FraudType.SYNTHETIC_IDENTITY,
                     FraudType.MERCHANT_COMPROMISE],
                    weights=[40, 25, 15, 15, 5]
                )[0]

        # Generate transaction based on fraud type
        if selected_fraud_type == FraudType.STOLEN_CARD:
            transaction = self._generate_stolen_card_transaction(user, base_time)
        elif selected_fraud_type == FraudType.ACCOUNT_TAKEOVER:
            transaction = self._generate_account_takeover_transaction(user, base_time)
        elif selected_fraud_type == FraudType.CARD_TESTING:
            transaction = self._generate_card_testing_transaction(user, base_time)
        elif selected_fraud_type == FraudType.SYNTHETIC_IDENTITY:
            transaction = self._generate_synthetic_identity_transaction(user, base_time)
        elif selected_fraud_type == FraudType.MERCHANT_COMPROMISE:
            transaction = self._generate_merchant_compromise_transaction(user, base_time)
        else:
            transaction = self._generate_legitimate_transaction(user, base_time)

        transaction.update({
            'transaction_id': str(uuid.uuid4()),
            'user_id': user.user_id,
            'timestamp': base_time.isoformat(),
            'is_fraud': is_fraud,
            'fraud_type': selected_fraud_type.value if is_fraud else None
        })

        return transaction

    def _generate_legitimate_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate a legitimate transaction following user patterns"""
        merchant = random.choice(user.preferred_merchants)
        payment_method = random.choice(user.preferred_payment_methods)

        # Amount follows user's normal spending pattern
        amount = max(5, np.random.lognormal(
            mean=np.log(user.avg_transaction_amount),
            sigma=0.6
        ))

        # Location near user's home
        lat_offset = random.uniform(-0.1, 0.1)  # ~11km radius
        lng_offset = random.uniform(-0.1, 0.1)

        return {
            'amount': round(amount, 2),
            'merchant_category': merchant,
            'payment_method': payment_method,
            'location_lat': user.location_lat + lat_offset,
            'location_lng': user.location_lng + lng_offset,
            'device_id': random.choice(user.device_ids),
            'ip_address': random.choice(user.ip_addresses)
        }

    def _generate_stolen_card_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate stolen card fraud transaction"""
        # Stolen card fraud often involves:
        # - Unusual merchant categories
        # - Higher amounts
        # - Different geographic location
        # - Different payment method patterns

        # Unusual merchants (not in user's preferred list)
        unusual_merchants = [m for m in self.merchant_categories if m not in user.preferred_merchants]
        merchant = random.choice(unusual_merchants + self.high_risk_merchants)

        # Higher amounts than normal
        amount = user.avg_transaction_amount * random.uniform(2.0, 8.0)

        # Different location (further from home)
        lat_offset = random.uniform(-2.0, 2.0)  # ~220km radius
        lng_offset = random.uniform(-2.0, 2.0)

        # Usually credit card for stolen card fraud
        payment_method = "credit_card"

        return {
            'amount': round(amount, 2),
            'merchant_category': merchant,
            'payment_method': payment_method,
            'location_lat': user.location_lat + lat_offset,
            'location_lng': user.location_lng + lng_offset,
            'device_id': str(uuid.uuid4()),  # New device
            'ip_address': fake.ipv4()  # New IP
        }

    def _generate_account_takeover_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate account takeover fraud transaction"""
        # Account takeover involves:
        # - Sudden change in behavior
        # - New device/IP
        # - Often starts with small amounts, then escalates

        merchant = random.choice(user.preferred_merchants)  # Familiar merchant

        # Start with smaller amounts to test, then increase
        test_phase = random.choice([True, False])
        if test_phase:
            amount = random.uniform(1, 25)  # Small test transaction
        else:
            amount = user.avg_transaction_amount * random.uniform(3.0, 10.0)

        # New location
        lat_offset = random.uniform(-1.0, 1.0)
        lng_offset = random.uniform(-1.0, 1.0)

        return {
            'amount': round(amount, 2),
            'merchant_category': merchant,
            'payment_method': random.choice(user.preferred_payment_methods),
            'location_lat': user.location_lat + lat_offset,
            'location_lng': user.location_lng + lng_offset,
            'device_id': str(uuid.uuid4()),  # New device
            'ip_address': fake.ipv4()  # New IP
        }

    def _generate_card_testing_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate card testing fraud transaction"""
        # Card testing involves:
        # - Very small amounts
        # - Online merchants
        # - Rapid succession

        amount = random.uniform(0.50, 5.00)  # Very small amounts
        merchant = "online"  # Usually online for testing

        return {
            'amount': round(amount, 2),
            'merchant_category': merchant,
            'payment_method': "credit_card",
            'location_lat': user.location_lat,
            'location_lng': user.location_lng,
            'device_id': str(uuid.uuid4()),
            'ip_address': fake.ipv4()
        }

    def _generate_synthetic_identity_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate synthetic identity fraud transaction"""
        # Synthetic identity fraud:
        # - New user with high-value transactions
        # - Premium merchants
        # - Inconsistent patterns

        premium_merchants = ["jewelry", "electronics", "airline", "hotel"]
        merchant = random.choice(premium_merchants)

        # High-value transactions
        amount = random.uniform(500, 5000)

        return {
            'amount': round(amount, 2),
            'merchant_category': merchant,
            'payment_method': "credit_card",
            'location_lat': user.location_lat,
            'location_lng': user.location_lng,
            'device_id': random.choice(user.device_ids),
            'ip_address': random.choice(user.ip_addresses)
        }

    def _generate_merchant_compromise_transaction(self, user: UserProfile, timestamp: datetime) -> Dict:
        """Generate merchant compromise fraud transaction"""
        # Merchant compromise:
        # - Multiple users affected
        # - Same merchant
        # - Similar time window

        compromised_merchant = random.choice(["gas_station", "restaurant", "retail"])

        # Normal amounts to avoid suspicion
        amount = np.random.lognormal(
            mean=np.log(user.avg_transaction_amount),
            sigma=0.4
        )

        return {
            'amount': round(amount, 2),
            'merchant_category': compromised_merchant,
            'payment_method': "credit_card",  # Usually card present
            'location_lat': user.location_lat + random.uniform(-0.05, 0.05),
            'location_lng': user.location_lng + random.uniform(-0.05, 0.05),
            'device_id': random.choice(user.device_ids),
            'ip_address': random.choice(user.ip_addresses)
        }

    def _save_transaction_to_db(self, transaction: Dict):
        """Save transaction to PostgreSQL"""
        if not self.db_connection:
            return

        try:
            cursor = self.db_connection.cursor()

            insert_sql = """
            INSERT INTO transactions (
                transaction_id, user_id, amount, timestamp, merchant_category,
                payment_method, location_lat, location_lng, device_id, 
                ip_address, is_fraud, fraud_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_sql, (
                transaction['transaction_id'],
                transaction['user_id'],
                transaction['amount'],
                transaction['timestamp'],
                transaction['merchant_category'],
                transaction['payment_method'],
                transaction['location_lat'],
                transaction['location_lng'],
                transaction['device_id'],
                transaction['ip_address'],
                transaction['is_fraud'],
                transaction['fraud_type']
            ))

            self.db_connection.commit()
            cursor.close()

        except Exception as e:
            logger.error(f"Failed to save transaction to DB: {e}")
            self.db_connection.rollback()

    def generate_and_stream_transactions(self, num_transactions: int = 100000):
        """Generate and stream 100,000 comprehensive fraud detection transactions"""
        logger.info(f"🚀 Starting generation of {num_transactions:,} transactions")

        # Generate user profiles
        num_users = 10000
        self.users = self.generate_user_profiles(num_users)

        # Generate transactions over 6 months
        start_date = datetime.now() - timedelta(days=180)
        end_date = datetime.now()

        transactions_sent = 0
        fraud_transactions_sent = 0
        batch_size = 100
        batch = []

        # Target fraud rate: 0.5%
        target_fraud_count = int(num_transactions * 0.005)
        fraud_generated = 0

        logger.info(f"Target fraud transactions: {target_fraud_count:,}")

        for i in range(num_transactions):
            # Random timestamp within the 6-month window
            time_delta = end_date - start_date
            random_seconds = random.randint(0, int(time_delta.total_seconds()))
            transaction_time = start_date + timedelta(seconds=random_seconds)

            # Select random user
            user = random.choice(self.users)

            # Determine if this should be fraud to meet target rate
            force_fraud = (fraud_generated < target_fraud_count and
                           random.random() < (target_fraud_count - fraud_generated) / (num_transactions - i))

            # Generate transaction
            transaction = self.generate_transaction(
                user=user,
                base_time=transaction_time,
                force_fraud=force_fraud
            )

            if transaction['is_fraud']:
                fraud_generated += 1
                fraud_transactions_sent += 1

            # Add to batch
            batch.append(transaction)

            # Send batch to Kafka
            if len(batch) >= batch_size:
                for txn in batch:
                    try:
                        future = self.kafka_producer.send(
                            os.getenv('KAFKA_TOPIC', 'transactions'),
                            key=txn['transaction_id'],
                            value=txn
                        )
                        future.get(timeout=10)

                        # Also save to database
                        self._save_transaction_to_db(txn)

                    except Exception as e:
                        logger.error(f"Failed to send transaction: {e}")
                        continue

                transactions_sent += len(batch)
                batch = []

                # Log progress
                if transactions_sent % 5000 == 0:
                    fraud_rate = (fraud_transactions_sent / transactions_sent) * 100
                    logger.info(
                        f"📊 Progress: {transactions_sent:,}/{num_transactions:,} "
                        f"({transactions_sent/num_transactions*100:.1f}%) | "
                        f"Fraud: {fraud_transactions_sent:,} ({fraud_rate:.2f}%)"
                    )

                # Rate limiting to avoid overwhelming the system
                time.sleep(0.1)

        # Send remaining transactions
        if batch:
            for txn in batch:
                try:
                    future = self.kafka_producer.send(
                        os.getenv('KAFKA_TOPIC', 'transactions'),
                        key=txn['transaction_id'],
                        value=txn
                    )
                    future.get(timeout=10)
                    self._save_transaction_to_db(txn)
                except Exception as e:
                    logger.error(f"Failed to send transaction: {e}")

            transactions_sent += len(batch)
            fraud_transactions_sent += sum(1 for txn in batch if txn['is_fraud'])

        # Flush producer
        self.kafka_producer.flush()

        final_fraud_rate = (fraud_transactions_sent / transactions_sent) * 100

        logger.info("🎉 Transaction generation completed!")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total Transactions: {transactions_sent:,}")
        logger.info(f"   Fraud Transactions: {fraud_transactions_sent:,}")
        logger.info(f"   Fraud Rate: {final_fraud_rate:.3f}%")
        logger.info(f"   Users: {len(self.users):,}")
        logger.info(f"   Time Period: {start_date.date()} to {end_date.date()}")

def main():
    """Main execution function"""
    logger.info("🚀 Starting Comprehensive Fraud Detection Data Generator")

    generator = ComprehensiveFraudGenerator()

    # Generate 100,000 transactions
    generator.generate_and_stream_transactions(num_transactions=100000)

    logger.info("✅ Data generation completed successfully")

if __name__ == "__main__":
    main()