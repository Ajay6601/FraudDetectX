# data-generator/generate_transactions.py
import uuid
import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import numpy as np
from faker import Faker
from loguru import logger
import pandas as pd

fake = Faker()

class TransactionGenerator:
    def __init__(self, num_users: int = 8000, fraud_rate: float = 0.005):
        self.num_users = num_users
        self.fraud_rate = fraud_rate
        self.users = self._create_user_profiles()
        self.merchant_categories = self._get_merchant_categories()

        # Fraud patterns
        self.fraud_patterns = {
            'stolen_card': 0.4,      # 40% of fraud
            'account_takeover': 0.3,  # 30% of fraud
            'card_testing': 0.2,     # 20% of fraud
            'synthetic_identity': 0.1 # 10% of fraud
        }

        logger.info(f"Generated {len(self.users)} user profiles")

    def _create_user_profiles(self) -> List[Dict]:
        """Create realistic user spending profiles"""
        users = []

        for i in range(self.num_users):
            user_type = random.choices(
                ['light', 'regular', 'power'],
                weights=[75, 18.75, 6.25]
            )[0]

            if user_type == 'light':
                avg_amount = random.uniform(15, 50)
                transaction_frequency = random.uniform(0.5, 2)  # per day
                preferred_categories = random.sample(self._get_merchant_categories(), 3)

            elif user_type == 'regular':
                avg_amount = random.uniform(40, 150)
                transaction_frequency = random.uniform(1.5, 4)
                preferred_categories = random.sample(self._get_merchant_categories(), 5)

            else:  # power user
                avg_amount = random.uniform(100, 500)
                transaction_frequency = random.uniform(3, 8)
                preferred_categories = random.sample(self._get_merchant_categories(), 8)

            user = {
                'user_id': f'user_{i:06d}',
                'user_type': user_type,
                'avg_amount': avg_amount,
                'std_amount': avg_amount * random.uniform(0.3, 0.8),
                'transaction_frequency': transaction_frequency,
                'preferred_categories': preferred_categories,
                'home_location': (fake.latitude(), fake.longitude()),
                'usual_hours': self._generate_usual_hours(),
                'is_fraud_prone': random.random() < 0.02,  # 2% of users more likely to be fraud victims
                'created_date': fake.date_between(start_date='-2y', end_date='-6m')
            }
            users.append(user)

        return users

    def _get_merchant_categories(self) -> List[str]:
        """Return list of merchant categories with risk levels"""
        return [
            'grocery', 'gas_station', 'restaurant', 'retail', 'pharmacy',
            'electronics', 'clothing', 'entertainment', 'travel', 'hotel',
            'atm_withdrawal', 'online_purchase', 'subscription', 'utility',
            'insurance', 'healthcare', 'education', 'automotive', 'home_improvement'
        ]

    def _generate_usual_hours(self) -> List[int]:
        """Generate typical hours when user makes transactions"""
        # Morning commute, lunch, evening, night
        patterns = [
            [7, 8, 9, 12, 13, 17, 18, 19, 20],  # Regular worker
            [10, 11, 14, 15, 16, 19, 20, 21],   # Flexible schedule
            [6, 7, 11, 12, 18, 19, 22, 23],     # Early bird
            [9, 10, 13, 14, 16, 17, 21, 22]     # Night owl
        ]
        return random.choice(patterns)

    def generate_transaction(self, user: Dict, timestamp: datetime, force_fraud: bool = False) -> Dict:
        """Generate a single transaction for a user"""

        # Determine if this transaction should be fraud
        is_fraud = force_fraud or (random.random() < self.fraud_rate)

        if is_fraud:
            return self._generate_fraud_transaction(user, timestamp)
        else:
            return self._generate_legitimate_transaction(user, timestamp)

    def _generate_legitimate_transaction(self, user: Dict, timestamp: datetime) -> Dict:
        """Generate a legitimate transaction"""

        # Amount follows user's spending pattern
        amount = max(1.0, np.random.normal(user['avg_amount'], user['std_amount']))

        # Prefer user's usual categories
        if random.random() < 0.8:
            category = random.choice(user['preferred_categories'])
        else:
            category = random.choice(self.merchant_categories)

        # Adjust amount based on category
        category_multipliers = {
            'grocery': 1.0, 'gas_station': 0.8, 'restaurant': 1.2,
            'electronics': 3.0, 'travel': 5.0, 'atm_withdrawal': 0.5
        }
        amount *= category_multipliers.get(category, 1.0)

        # Round amount realistically
        if amount < 10:
            amount = round(amount, 2)
        elif amount < 100:
            amount = round(amount)
        else:
            amount = round(amount / 5) * 5  # Round to nearest $5

        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'amount': round(amount, 2),
            'timestamp': timestamp.isoformat(),
            'merchant_category': category,
            'is_fraud': False,
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_weekend': timestamp.weekday() >= 5,
            'is_night': timestamp.hour < 6 or timestamp.hour > 22
        }

        return transaction

    def _generate_fraud_transaction(self, user: Dict, timestamp: datetime) -> Dict:
        """Generate a fraudulent transaction with realistic patterns"""

        # Choose fraud pattern
        fraud_type = random.choices(
            list(self.fraud_patterns.keys()),
            weights=list(self.fraud_patterns.values())
        )[0]

        if fraud_type == 'stolen_card':
            # Unusual amount and category for user
            amount = random.uniform(200, 2000)
            category = random.choice(['electronics', 'atm_withdrawal', 'online_purchase'])

        elif fraud_type == 'account_takeover':
            # Sudden change in behavior
            amount = user['avg_amount'] * random.uniform(3, 10)
            category = random.choice(['travel', 'electronics', 'online_purchase'])

        elif fraud_type == 'card_testing':
            # Small amounts, round numbers
            amount = random.choice([1.00, 5.00, 10.00, 25.00])
            category = 'online_purchase'

        else:  # synthetic_identity
            # New user with high amounts
            amount = random.uniform(500, 3000)
            category = random.choice(['electronics', 'travel', 'retail'])

        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'amount': round(amount, 2),
            'timestamp': timestamp.isoformat(),
            'merchant_category': category,
            'is_fraud': True,
            'fraud_type': fraud_type,
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_weekend': timestamp.weekday() >= 5,
            'is_night': timestamp.hour < 6 or timestamp.hour > 22
        }

        return transaction

    def generate_batch(self, batch_size: int, start_date: datetime = None) -> List[Dict]:
        """Generate a batch of transactions"""

        if start_date is None:
            start_date = datetime.now() - timedelta(days=180)  # 6 months ago

        transactions = []
        current_date = start_date

        # Calculate how many fraud transactions we need
        total_fraud_needed = int(batch_size * self.fraud_rate)
        fraud_count = 0

        for i in range(batch_size):
            # Select random user
            user = random.choice(self.users)

            # Generate realistic timestamp
            days_offset = random.uniform(0, 180)  # Within 6 months
            hour_offset = random.choice(user['usual_hours']) + random.uniform(-1, 1)
            hour_offset = max(0, min(23, hour_offset))

            transaction_time = current_date + timedelta(
                days=days_offset,
                hours=hour_offset,
                minutes=random.randint(0, 59)
            )

            # Force fraud if we haven't met quota
            force_fraud = fraud_count < total_fraud_needed and random.random() < 0.1

            transaction = self.generate_transaction(user, transaction_time, force_fraud)

            if transaction['is_fraud']:
                fraud_count += 1

            transactions.append(transaction)

        # Sort by timestamp
        transactions.sort(key=lambda x: x['timestamp'])

        logger.info(f"Generated {len(transactions)} transactions with {fraud_count} fraudulent ones ({fraud_count/len(transactions)*100:.2f}%)")

        return transactions

    def save_to_file(self, transactions: List[Dict], filename: str):
        """Save transactions to file"""
        df = pd.DataFrame(transactions)

        if filename.endswith('.csv'):
            df.to_csv(filename, index=False)
        elif filename.endswith('.json'):
            with open(filename, 'w') as f:
                json.dump(transactions, f, indent=2)
        else:
            df.to_parquet(filename, index=False)

        logger.info(f"Saved {len(transactions)} transactions to {filename}")

# Example usage and testing
if __name__ == "__main__":
    generator = TransactionGenerator(num_users=1000, fraud_rate=0.005)

    # Generate sample batch
    transactions = generator.generate_batch(10000)

    # Save to file
    generator.save_to_file(transactions, '/app/data/sample_transactions.csv')

    # Print statistics
    df = pd.DataFrame(transactions)
    print(f"Total transactions: {len(df)}")
    print(f"Fraud transactions: {df['is_fraud'].sum()}")
    print(f"Fraud rate: {df['is_fraud'].mean()*100:.2f}%")
    print(f"Average amount: ${df['amount'].mean():.2f}")
    print(f"Categories: {df['merchant_category'].nunique()}")