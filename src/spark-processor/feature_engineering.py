# spark-processor/feature_engineering.py
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import psycopg2
from loguru import logger

class FeatureEngineer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

        # JDBC URL for Spark
        self.jdbc_url = f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"

        # Merchant risk scores (based on historical fraud rates)
        self.high_risk_merchants = {
            'atm_withdrawal': 0.8,
            'gas_station': 0.6,
            'online_purchase': 0.7,
            'electronics': 0.5,
            'travel': 0.4
        }

        logger.info("FeatureEngineer initialized")

    def get_transaction_schema(self) -> StructType:
        """Define schema for incoming transactions"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("timestamp", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("is_fraud", BooleanType(), False),
            StructField("hour_of_day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("is_night", BooleanType(), True)
        ])

    def parse_timestamp(self, df: DataFrame) -> DataFrame:
        """Parse timestamp string to timestamp type"""
        return df.withColumn(
            "parsed_timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).withColumn(
            "parsed_timestamp",
            when(col("parsed_timestamp").isNull(),
                 to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
            .otherwise(col("parsed_timestamp"))
        )

    def add_temporal_features(self, df: DataFrame) -> DataFrame:
        """Add temporal-based features"""
        df = self.parse_timestamp(df)

        return df.withColumn(
            "hour_of_day", hour(col("parsed_timestamp"))
        ).withColumn(
            "day_of_week", dayofweek(col("parsed_timestamp"))
        ).withColumn(
            "is_weekend", col("day_of_week").isin([1, 7])  # Sunday=1, Saturday=7
        ).withColumn(
            "is_night", (col("hour_of_day") < 6) | (col("hour_of_day") > 22)
        ).withColumn(
            "month", month(col("parsed_timestamp"))
        ).withColumn(
            "day_of_month", dayofmonth(col("parsed_timestamp"))
        )

    def add_amount_features(self, df: DataFrame) -> DataFrame:
        """Add amount-based features"""
        return df.withColumn(
            "amount_log", log(col("amount") + 1)
        ).withColumn(
            "amount_round", (col("amount") % 1 == 0).cast("boolean")
        ).withColumn(
            "amount_large", (col("amount") > 1000).cast("boolean")
        ).withColumn(
            "amount_small", (col("amount") < 10).cast("boolean")
        )

    def add_merchant_features(self, df: DataFrame) -> DataFrame:
        """Add merchant category features"""
        # Create high risk merchant indicator
        high_risk_categories = list(self.high_risk_merchants.keys())

        return df.withColumn(
            "high_risk_merchant",
            col("merchant_category").isin(high_risk_categories)
        ).withColumn(
            "merchant_risk_score",
            when(col("merchant_category") == "atm_withdrawal", 0.8)
            .when(col("merchant_category") == "online_purchase", 0.7)
            .when(col("merchant_category") == "gas_station", 0.6)
            .when(col("merchant_category") == "electronics", 0.5)
            .when(col("merchant_category") == "travel", 0.4)
            .otherwise(0.2)
        )

    def add_user_behavioral_features(self, df: DataFrame) -> DataFrame:
        """Add user behavioral features using window functions"""
        # Define windows for different time periods
        user_window_24h = Window.partitionBy("user_id").orderBy("parsed_timestamp").rangeBetween(-86400, 0)
        user_window_7d = Window.partitionBy("user_id").orderBy("parsed_timestamp").rangeBetween(-604800, 0)
        user_window_30d = Window.partitionBy("user_id").orderBy("parsed_timestamp").rangeBetween(-2592000, 0)

        # Add velocity features (transaction counts)
        df = df.withColumn(
            "transactions_last_24h",
            count("*").over(user_window_24h) - 1  # Exclude current transaction
        ).withColumn(
            "transactions_last_7d",
            count("*").over(user_window_7d) - 1
        ).withColumn(
            "transactions_last_30d",
            count("*").over(user_window_30d) - 1
        )

        # Add amount-based behavioral features
        df = df.withColumn(
            "user_avg_amount_24h",
            avg("amount").over(user_window_24h)
        ).withColumn(
            "user_avg_amount_7d",
            avg("amount").over(user_window_7d)
        ).withColumn(
            "user_std_amount_7d",
            stddev("amount").over(user_window_7d)
        ).withColumn(
            "user_max_amount_7d",
            max("amount").over(user_window_7d)
        )

        # Add Z-score for current transaction
        df = df.withColumn(
            "amount_zscore",
            when(col("user_std_amount_7d") > 0,
                 (col("amount") - col("user_avg_amount_7d")) / col("user_std_amount_7d"))
            .otherwise(0.0)
        )

        # Add time since last transaction
        user_time_window = Window.partitionBy("user_id").orderBy("parsed_timestamp")
        df = df.withColumn(
            "prev_transaction_time",
            lag("parsed_timestamp").over(user_time_window)
        ).withColumn(
            "minutes_since_last",
            when(col("prev_transaction_time").isNotNull(),
                 (unix_timestamp("parsed_timestamp") - unix_timestamp("prev_transaction_time")) / 60)
            .otherwise(999999.0)  # Large number for first transaction
        )

        return df.drop("prev_transaction_time")

    def add_frequency_features(self, df: DataFrame) -> DataFrame:
        """Add frequency-based features"""
        # Merchant frequency for user
        merchant_window = Window.partitionBy("user_id", "merchant_category").orderBy("parsed_timestamp")

        df = df.withColumn(
            "user_merchant_frequency",
            count("*").over(merchant_window)
        )

        # Hour of day frequency for user
        hour_window = Window.partitionBy("user_id", "hour_of_day").orderBy("parsed_timestamp")

        df = df.withColumn(
            "user_hour_frequency",
            count("*").over(hour_window)
        )

        return df

    def add_anomaly_features(self, df: DataFrame) -> DataFrame:
        """Add anomaly detection features"""
        # Flag transactions that are unusual for the user
        df = df.withColumn(
            "amount_unusual",
            (abs(col("amount_zscore")) > 2.0).cast("boolean")
        ).withColumn(
            "time_unusual",
            ((col("hour_of_day") < 6) | (col("hour_of_day") > 23)).cast("boolean")
        ).withColumn(
            "velocity_unusual",
            (col("transactions_last_24h") > 10).cast("boolean")
        ).withColumn(
            "merchant_unusual",
            (col("user_merchant_frequency") == 1).cast("boolean")  # First time at this merchant
        )

        # Composite anomaly score
        df = df.withColumn(
            "anomaly_score",
            (col("amount_unusual").cast("int") +
             col("time_unusual").cast("int") +
             col("velocity_unusual").cast("int") +
             col("merchant_unusual").cast("int")) / 4.0
        )

        return df

    def select_final_features(self, df: DataFrame) -> DataFrame:
        """Select final feature set for ML"""
        feature_columns = [
            # Identifiers
            "transaction_id", "user_id", "parsed_timestamp",

            # Original features
            "amount", "merchant_category",

            # Temporal features
            "hour_of_day", "day_of_week", "is_weekend", "is_night",

            # Amount features
            "amount_log", "amount_round", "amount_zscore",

            # Merchant features
            "high_risk_merchant", "merchant_risk_score",

            # Behavioral features
            "transactions_last_24h", "transactions_last_7d",
            "user_avg_amount_7d", "minutes_since_last",

            # Frequency features
            "user_merchant_frequency", "user_hour_frequency",

            # Anomaly features
            "anomaly_score", "amount_unusual", "velocity_unusual",

            # Target
            "is_fraud"
        ]

        return df.select(*feature_columns)

    def process_batch(self, df: DataFrame) -> DataFrame:
        """Process a batch of transactions with feature engineering"""
        logger.info(f"Processing batch with {df.count()} transactions")

        # Apply feature engineering steps
        df = self.add_temporal_features(df)
        df = self.add_amount_features(df)
        df = self.add_merchant_features(df)
        df = self.add_user_behavioral_features(df)
        df = self.add_frequency_features(df)
        df = self.add_anomaly_features(df)
        df = self.select_final_features(df)

        # Fill null values
        df = df.fillna({
            'user_avg_amount_7d': 0.0,
            'user_std_amount_7d': 1.0,
            'amount_zscore': 0.0,
            'transactions_last_24h': 0,
            'transactions_last_7d': 0,
            'transactions_last_30d': 0,
            'minutes_since_last': 999999.0,
            'user_merchant_frequency': 1,
            'user_hour_frequency': 1,
            'anomaly_score': 0.0
        })

        logger.info("Feature engineering completed")
        return df

    def save_to_postgres(self, df: DataFrame, table_name: str = "processed_transactions"):
        """Save processed data to PostgreSQL"""
        try:
            # Convert timestamp back to string for PostgreSQL
            df_to_save = df.withColumn(
                "timestamp", col("parsed_timestamp").cast("string")
            ).drop("parsed_timestamp")

            # Write to PostgreSQL
            df_to_save.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.db_config['user']) \
                .option("password", self.db_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

            logger.info(f"Saved {df.count()} processed transactions to {table_name}")

        except Exception as e:
            logger.error(f"Failed to save to PostgreSQL: {e}")
            raise

    def get_user_profiles_from_db(self) -> DataFrame:
        """Load user profiles from PostgreSQL for feature engineering"""
        try:
            return self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "user_profiles") \
                .option("user", self.db_config['user']) \
                .option("password", self.db_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        except Exception as e:
            logger.warning(f"Could not load user profiles: {e}")
            return None

# Example usage and testing
if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("FraudDetectionFeatureEngineering") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Initialize feature engineer
    feature_engineer = FeatureEngineer(spark)

    # Test with sample data
    sample_data = [
        ("txn_001", "user_001", 50.0, "2024-01-15T10:30:00", "grocery", False),
        ("txn_002", "user_001", 1500.0, "2024-01-15T22:45:00", "electronics", True),
        ("txn_003", "user_002", 25.0, "2024-01-15T12:15:00", "restaurant", False)
    ]

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("is_fraud", BooleanType(), False)
    ])

    df = spark.createDataFrame(sample_data, schema)
    processed_df = feature_engineer.process_batch(df)

    processed_df.show(truncate=False)
    processed_df.printSchema()

    spark.stop()