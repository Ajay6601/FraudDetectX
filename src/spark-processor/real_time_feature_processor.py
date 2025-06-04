"""
Real-time Spark Streaming Feature Engineering for Fraud Detection
Processes transactions from Kafka and creates comprehensive features
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeFraudFeatureProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()

        # Feature engineering parameters
        self.high_risk_merchants = [
            'gas_station', 'atm_withdrawal', 'gambling', 'adult_entertainment',
            'cash_advance', 'jewelry', 'electronics'
        ]

        # Kafka schema definition
        self.transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("payment_method", StringType(), False),
            StructField("location_lat", DoubleType(), True),
            StructField("location_lng", DoubleType(), True),
            StructField("device_id", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("is_fraud", BooleanType(), False),
            StructField("fraud_type", StringType(), True)
        ])

        # DB connection properties
        self.jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'fraud_detection')}"
        self.db_properties = {
            "user": os.getenv('POSTGRES_USER', 'mluser'),
            "password": os.getenv('POSTGRES_PASSWORD', 'mlpassword'),
            "driver": "org.postgresql.Driver"
        }

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Kafka and PostgreSQL integration"""
        spark = SparkSession.builder \
            .appName("RealTimeFraudFeatureProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.3.1") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created")
        return spark

    def get_user_historical_df(self):
        """Get user historical transaction data as DataFrame"""
        try:
            # Read user transaction data from PostgreSQL
            user_hist_df = self.spark.read \
                .jdbc(
                url=self.jdbc_url,
                table="transactions",
                properties=self.db_properties
            )

            # Create a window spec for user-based aggregations
            window_spec = Window.partitionBy("user_id")

            # Calculate user statistics
            user_stats_df = user_hist_df \
                .withColumn("total_transactions", count("transaction_id").over(window_spec)) \
                .withColumn("user_avg_amount", avg("amount").over(window_spec)) \
                .withColumn("user_std_amount", stddev("amount").over(window_spec)) \
                .withColumn("median_amount", expr("percentile_approx(amount, 0.5)").over(window_spec)) \
                .withColumn("fraud_count", sum(when(col("is_fraud"), 1).otherwise(0)).over(window_spec)) \
                .withColumn("fraud_rate", col("fraud_count") / col("total_transactions") * 100) \
                .select("user_id", "total_transactions", "user_avg_amount", "user_std_amount",
                        "median_amount", "fraud_count", "fraud_rate") \
                .dropDuplicates(["user_id"])

            logger.info(f"Loaded user historical data for {user_stats_df.count()} users")
            return user_stats_df

        except Exception as e:
            logger.error(f"Error loading user historical data: {e}")
            # Return empty DataFrame with the same schema
            return self.spark.createDataFrame([],
                                              StructType([
                                                  StructField("user_id", StringType(), False),
                                                  StructField("total_transactions", LongType(), False),
                                                  StructField("user_avg_amount", DoubleType(), False),
                                                  StructField("user_std_amount", DoubleType(), False),
                                                  StructField("median_amount", DoubleType(), False),
                                                  StructField("fraud_count", LongType(), False),
                                                  StructField("fraud_rate", DoubleType(), False)
                                              ])
                                              )

    def get_processed_transaction_ids(self):
        """Get IDs of already processed transactions to avoid duplicates"""
        try:
            # Read processed transaction IDs from PostgreSQL
            processed_df = self.spark.read \
                .jdbc(
                url=self.jdbc_url,
                table="processed_transactions",
                properties=self.db_properties
            ) \
                .select("transaction_id")

            processed_count = processed_df.count()
            logger.info(f"Found {processed_count} already processed transactions")
            return processed_df

        except Exception as e:
            logger.error(f"Error loading processed transaction IDs: {e}")
            # Return empty DataFrame
            return self.spark.createDataFrame([],
                                              StructType([StructField("transaction_id", StringType(), False)])
                                              )

    def process_streaming_transactions(self):
        """Process transactions using Spark Structured Streaming"""
        logger.info("Starting Spark Structured Streaming for transaction processing...")

        try:
            # Load user historical data
            user_historical_df = self.get_user_historical_df()

            # Get already processed transaction IDs to avoid duplicates
            processed_tx_df = self.get_processed_transaction_ids()

            # Create Kafka stream source
            kafka_options = {
                "kafka.bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
                "kafka.security.protocol": os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
                "kafka.sasl.mechanism": os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN'),
                "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_SASL_USERNAME")}" password="{os.getenv("KAFKA_SASL_PASSWORD")}";',
                "subscribe": os.getenv('KAFKA_TOPIC', 'transactions'),
                "startingOffsets": "earliest",  # Process all messages in Kafka
                "failOnDataLoss": "false"
            }

            # Read stream from Kafka
            transaction_stream_df = self.spark \
                .readStream \
                .format("kafka") \
                .options(**kafka_options) \
                .load()

            # Parse JSON from Kafka
            parsed_df = transaction_stream_df \
                .select(from_json(col("value").cast("string"), self.transaction_schema).alias("data")) \
                .select("data.*")

            # Filter out already processed transactions
            broadcast_processed_tx = broadcast(processed_tx_df)
            new_transactions_df = parsed_df \
                .join(broadcast_processed_tx,
                      parsed_df.transaction_id == broadcast_processed_tx.transaction_id,
                      "left_anti")  # left_anti = exclude matches

            # Extract features using Spark SQL functions
            feature_df = new_transactions_df \
                .withColumn("hour_of_day", hour(col("timestamp"))) \
                .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))) \
                .withColumn("is_night", when((hour(col("timestamp")) >= 22) | (hour(col("timestamp")) <= 6), lit(True)).otherwise(lit(False))) \
                .withColumn("is_holiday", when(
                ((month(col("timestamp")) == 1) & (dayofmonth(col("timestamp")) == 1)) |
                ((month(col("timestamp")) == 7) & (dayofmonth(col("timestamp")) == 4)) |
                ((month(col("timestamp")) == 12) & (dayofmonth(col("timestamp")) == 25)) |
                ((month(col("timestamp")) == 11) & (dayofmonth(col("timestamp")) == 4)) |
                ((month(col("timestamp")) == 12) & (dayofmonth(col("timestamp")) == 31)),
                lit(True)
            ).otherwise(lit(False))) \
                .withColumn("high_risk_merchant", when(col("merchant_category").isin(self.high_risk_merchants), lit(True)).otherwise(lit(False))) \
                .withColumn("amount_round", when(
                (col("amount") == expr("CAST(ROUND(amount) AS DOUBLE)")) &
                (expr("amount % 10") == 0) &
                (col("amount") >= 100),
                lit(True)
            ).otherwise(lit(False)))

            # Join with user historical data to get user statistics
            enriched_df = feature_df.join(
                user_historical_df,
                on="user_id",
                how="left"
            )

            # Calculate risk features
            risk_df = enriched_df \
                .withColumn("amount_zscore", when(col("user_std_amount") > 0,
                                                  (col("amount") - col("user_avg_amount")) / col("user_std_amount")
                                                  ).otherwise(lit(0))) \
                .withColumn("amount_percentile",
                            when(col("user_avg_amount") > 0,
                                 when(col("amount") > col("user_avg_amount"),
                                      lit(75.0) + least(lit(25.0), (col("amount") - col("user_avg_amount")) / col("user_avg_amount") * lit(25.0))
                                      ).otherwise(lit(25.0) + (col("amount") / col("user_avg_amount")) * lit(50.0))
                                 ).otherwise(lit(50.0))) \
                .withColumn("merchant_risk_score",
                            when(col("merchant_category") == "gambling", lit(0.9))
                            .when(col("merchant_category") == "adult_entertainment", lit(0.8))
                            .when(col("merchant_category") == "cash_advance", lit(0.9))
                            .when(col("merchant_category") == "atm_withdrawal", lit(0.7))
                            .when(col("merchant_category") == "gas_station", lit(0.6))
                            .when(col("merchant_category") == "jewelry", lit(0.5))
                            .when(col("merchant_category") == "electronics", lit(0.4))
                            .when(col("merchant_category") == "online", lit(0.3))
                            .when(col("merchant_category") == "restaurant", lit(0.1))
                            .when(col("merchant_category") == "grocery", lit(0.1))
                            .when(col("merchant_category") == "pharmacy", lit(0.1))
                            .when(col("merchant_category") == "utilities", lit(0.05))
                            .otherwise(lit(0.2))) \
                .withColumn("geographic_risk", lit(0.2)) \
                .withColumn("velocity_risk", lit(0.3)) \
                .withColumn("device_risk", lit(0.1)) \
                .withColumn("ip_risk_score", lit(0.2)) \
                .withColumn("device_fingerprint_risk", lit(0.1)) \
                .withColumn("fraud_probability",
                            (col("merchant_risk_score") * 0.2) +
                            (col("geographic_risk") * 0.15) +
                            (col("velocity_risk") * 0.2) +
                            (col("device_risk") * 0.15) +
                            when(col("is_night"), lit(0.05)).otherwise(lit(0)) +
                            when(col("amount_round"), lit(0.05)).otherwise(lit(0)) +
                            (least(abs(col("amount_zscore")) / lit(3.0), lit(1.0)) * 0.1)) \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("model_version", lit("v1.0"))

            # Select final features matching exactly the database schema
            final_df = risk_df.select(
                col("transaction_id"),
                col("user_id"),
                col("amount").cast("decimal(12,2)"),
                col("timestamp"),
                col("merchant_category"),
                col("payment_method"),
                col("hour_of_day").cast("integer"),
                col("day_of_week").cast("integer"),
                col("is_weekend").cast("boolean"),
                col("is_night").cast("boolean"),
                col("is_holiday").cast("boolean"),
                col("amount_zscore").cast("decimal(8,4)"),
                col("amount_percentile").cast("decimal(5,2)"),
                lit(1.0).cast("decimal(8,2)").alias("days_since_last"),
                lit(0).cast("integer").alias("transactions_last_1h"),
                lit(0).cast("integer").alias("transactions_last_24h"),
                lit(0).cast("integer").alias("transactions_last_7d"),
                lit(0).cast("integer").alias("transactions_last_30d"),
                col("user_avg_amount").cast("decimal(12,2)"),
                col("user_std_amount").cast("decimal(12,2)"),
                col("high_risk_merchant").cast("boolean"),
                col("amount_round").cast("boolean"),
                col("geographic_risk").cast("decimal(5,2)"),
                col("velocity_risk").cast("decimal(5,2)"),
                col("device_risk").cast("decimal(5,2)"),
                col("ip_risk_score").cast("decimal(5,2)"),
                col("device_fingerprint_risk").cast("decimal(5,2)"),
                col("is_fraud").cast("boolean"),
                col("fraud_probability").cast("decimal(6,4)"),
                col("model_version").cast("varchar(20)"),
                col("processed_at")
            )

            # Write to PostgreSQL
            query = final_df.writeStream \
                .foreachBatch(self._write_batch_to_postgres) \
                .outputMode("append") \
                .trigger(processingTime="5 seconds") \
                .option("checkpointLocation", "/tmp/checkpoint/postgres") \
                .start()

            logger.info("Spark structured streaming started successfully")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in Spark streaming process: {e}")
            if self.spark:
                self.spark.stop()

    def _write_batch_to_postgres(self, batch_df: DataFrame, batch_id: int):
        """Write a batch of processed transactions to PostgreSQL with foreign key handling"""
        try:
            if batch_df.isEmpty():
                logger.info(f"Batch {batch_id}: Empty batch, skipping")
                return

            # First verify which transactions exist in the database
            transaction_ids = [row.transaction_id for row in batch_df.select("transaction_id").collect()]

            if not transaction_ids:
                logger.info(f"Batch {batch_id}: No transaction IDs found, skipping")
                return

            # Read existing transactions from database
            existing_df = self.spark.read \
                .jdbc(
                url=self.jdbc_url,
                table="transactions",
                properties=self.db_properties
            ) \
                .select("transaction_id") \
                .filter(col("transaction_id").isin(transaction_ids))

            # Filter batch to only include transactions that exist in the database
            valid_batch_df = batch_df.join(
                existing_df,
                on="transaction_id",
                how="inner"
            )

            # Count records
            original_count = batch_df.count()
            valid_count = valid_batch_df.count()
            skipped_count = original_count - valid_count

            if valid_count == 0:
                logger.info(f"Batch {batch_id}: No valid transactions found (all {original_count} transactions missing from database)")
                return

            fraud_count = valid_batch_df.filter(col("is_fraud") == True).count()

            # Write only valid transactions to PostgreSQL
            valid_batch_df.write \
                .jdbc(
                url=self.jdbc_url,
                table="processed_transactions",
                mode="append",
                properties=self.db_properties
            )

            logger.info(f"Batch {batch_id}: Processed {valid_count}/{original_count} transactions " +
                        f"(skipped {skipped_count}) | Fraud: {fraud_count} ({fraud_count/valid_count*100:.2f}%)")

        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")

def main():
    """Main execution function"""
    logger.info("Starting Real-time Fraud Feature Processor")

    processor = RealTimeFraudFeatureProcessor()
    processor.process_streaming_transactions()

if __name__ == "__main__":
    main()