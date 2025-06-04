import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
import os

def wait_for_postgres():
    for i in range(30):
        try:
            # Try to connect to postgres database with mluser
            # If mluser can connect to postgres db, we're good
            conn = psycopg2.connect(
                host="127.0.0.1",      # Use 127.0.0.1 instead of "localhost"
                port="5432",
                database="fraud_detection",
                user="mluser",
                password="mlpassword"
            )
            conn.close()
            print("✅ PostgreSQL is ready and mluser can connect")
            return True
        except Exception as e:
            print(f"⏳ Waiting for PostgreSQL... ({i+1}/30) - {str(e)}")
            time.sleep(2)
    return False

def create_database_if_not_exists():
    """Create fraud_detection database if it doesn't exist"""
    try:
        # First check if fraud_detection database exists by trying to connect to it
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5432",
            database="fraud_detection",
            user="mluser",
            password="mlpassword"
        )
        conn.close()
        print("ℹ️  Database 'fraud_detection' already exists")
        return True
    except psycopg2.OperationalError:
        # Database doesn't exist, try to create it
        print("🔨 Database 'fraud_detection' doesn't exist, attempting to create...")

        try:
            # Connect to postgres database to create new database
            conn = psycopg2.connect(
                host="127.0.0.1",
                port="5432",
                database="postgres",
                user="mluser",
                password="mlpassword"
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute("CREATE DATABASE fraud_detection OWNER mluser;")
            cursor.close()
            conn.close()
            print("✅ Created database 'fraud_detection'")
            return True

        except Exception as e:
            print(f"❌ Failed to create database 'fraud_detection': {str(e)}")
            print("💡 You may need to create the database manually:")
            print("   docker-compose exec postgres createdb -U postgres fraud_detection")
            print("   docker-compose exec postgres psql -U postgres -c \"GRANT ALL PRIVILEGES ON DATABASE fraud_detection TO mluser;\"")
            return False

def create_tables():
    """Create all database tables"""
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="fraud_detection",
        user="mluser",
        password="mlpassword"
    )

    cursor = conn.cursor()

    # Create tables with complete schema
    tables_sql = """
    -- Raw transactions from Kafka
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        merchant_category VARCHAR(50) NOT NULL,
        payment_method VARCHAR(30) NOT NULL,
        location_lat DECIMAL(10,8),
        location_lng DECIMAL(11,8),
        device_id VARCHAR(100),
        ip_address INET,
        is_fraud BOOLEAN NOT NULL DEFAULT FALSE,
        fraud_type VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Feature-engineered transactions
    CREATE TABLE IF NOT EXISTS processed_transactions (
        id SERIAL PRIMARY KEY,
        transaction_id VARCHAR(50) UNIQUE NOT NULL,
        user_id VARCHAR(50) NOT NULL,
        
        -- Original features
        amount DECIMAL(12,2) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        merchant_category VARCHAR(50) NOT NULL,
        payment_method VARCHAR(30) NOT NULL,
        
        -- Temporal features
        hour_of_day INTEGER,
        day_of_week INTEGER,
        is_weekend BOOLEAN,
        is_night BOOLEAN,
        is_holiday BOOLEAN,
        
        -- User behavior features
        amount_zscore DECIMAL(8,4),
        amount_percentile DECIMAL(5,2),
        days_since_last DECIMAL(8,2),
        transactions_last_1h INTEGER,
        transactions_last_24h INTEGER,
        transactions_last_7d INTEGER,
        transactions_last_30d INTEGER,
        user_avg_amount DECIMAL(12,2),
        user_std_amount DECIMAL(12,2),
        
        -- Risk features
        high_risk_merchant BOOLEAN,
        amount_round BOOLEAN,
        geographic_risk DECIMAL(5,2),
        velocity_risk DECIMAL(5,2),
        device_risk DECIMAL(5,2),
        
        -- Network features
        ip_risk_score DECIMAL(5,2),
        device_fingerprint_risk DECIMAL(5,2),
        
        -- Target and metadata
        is_fraud BOOLEAN NOT NULL DEFAULT FALSE,
        fraud_probability DECIMAL(6,4),
        model_version VARCHAR(20),
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    );

    -- User profiles for behavioral analysis
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id VARCHAR(50) PRIMARY KEY,
        
        -- Transaction statistics
        total_transactions INTEGER DEFAULT 0,
        total_amount DECIMAL(15,2) DEFAULT 0.0,
        avg_amount DECIMAL(12,2) DEFAULT 0.0,
        std_amount DECIMAL(12,2) DEFAULT 0.0,
        median_amount DECIMAL(12,2) DEFAULT 0.0,
        
        -- Temporal patterns
        first_transaction TIMESTAMP,
        last_transaction TIMESTAMP,
        avg_days_between_transactions DECIMAL(8,2),
        
        -- Behavioral patterns
        preferred_merchants TEXT[],
        preferred_payment_methods TEXT[],
        typical_transaction_hours INTEGER[],
        home_location_lat DECIMAL(10,8),
        home_location_lng DECIMAL(11,8),
        
        -- Risk indicators
        fraud_count INTEGER DEFAULT 0,
        fraud_rate DECIMAL(6,4) DEFAULT 0.0,
        risk_score DECIMAL(6,4) DEFAULT 0.0,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- ML model training runs
    CREATE TABLE IF NOT EXISTS model_training_runs (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(100) UNIQUE NOT NULL,
        model_name VARCHAR(100) NOT NULL,
        model_version VARCHAR(50),
        algorithm VARCHAR(50) NOT NULL,
        
        -- Training data
        training_data_count INTEGER,
        training_fraud_count INTEGER,
        training_fraud_rate DECIMAL(6,4),
        test_data_count INTEGER,
        test_fraud_count INTEGER,
        
        -- Model performance
        accuracy DECIMAL(8,6),
        precision_score DECIMAL(8,6),
        recall_score DECIMAL(8,6),
        f1_score DECIMAL(8,6),
        auc_roc DECIMAL(8,6),
        auc_pr DECIMAL(8,6),
        
        -- Hyperparameters (JSON)
        hyperparameters JSONB,
        
        -- Feature importance
        feature_importance JSONB,
        
        -- Training metadata
        training_start TIMESTAMP,
        training_end TIMESTAMP,
        training_duration INTERVAL,
        model_size_mb DECIMAL(8,2),
        model_path VARCHAR(500),
        
        -- Status
        status VARCHAR(50) DEFAULT 'training',
        error_message TEXT,
        
        -- MLflow integration
        mlflow_experiment_id VARCHAR(100),
        mlflow_run_id VARCHAR(100),
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Fraud detection alerts
    CREATE TABLE IF NOT EXISTS fraud_alerts (
        id SERIAL PRIMARY KEY,
        transaction_id VARCHAR(50) NOT NULL,
        user_id VARCHAR(50) NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        risk_score DECIMAL(6,4) NOT NULL,
        confidence DECIMAL(6,4) NOT NULL,
        alert_reason TEXT,
        model_version VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        resolved_at TIMESTAMP,
        resolution_status VARCHAR(30),
        
        FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    );
    """

    print("🔨 Creating tables...")
    cursor.execute(tables_sql)

    # Create indexes for performance
    indexes_sql = """
    -- Indexes for transactions
    CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud);
    CREATE INDEX IF NOT EXISTS idx_transactions_merchant_category ON transactions(merchant_category);
    CREATE INDEX IF NOT EXISTS idx_transactions_amount ON transactions(amount);
    
    -- Indexes for processed_transactions
    CREATE INDEX IF NOT EXISTS idx_processed_transactions_user_id ON processed_transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_processed_transactions_timestamp ON processed_transactions(timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_processed_transactions_is_fraud ON processed_transactions(is_fraud);
    CREATE INDEX IF NOT EXISTS idx_processed_transactions_fraud_probability ON processed_transactions(fraud_probability DESC);
    
    -- Indexes for user_profiles
    CREATE INDEX IF NOT EXISTS idx_user_profiles_risk_score ON user_profiles(risk_score DESC);
    CREATE INDEX IF NOT EXISTS idx_user_profiles_fraud_rate ON user_profiles(fraud_rate DESC);
    CREATE INDEX IF NOT EXISTS idx_user_profiles_updated_at ON user_profiles(updated_at DESC);
    
    -- Indexes for model_training_runs
    CREATE INDEX IF NOT EXISTS idx_model_training_runs_created_at ON model_training_runs(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_model_training_runs_status ON model_training_runs(status);
    CREATE INDEX IF NOT EXISTS idx_model_training_runs_model_name ON model_training_runs(model_name);
    
    -- Indexes for fraud_alerts
    CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at ON fraud_alerts(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_fraud_alerts_risk_score ON fraud_alerts(risk_score DESC);
    CREATE INDEX IF NOT EXISTS idx_fraud_alerts_resolution_status ON fraud_alerts(resolution_status);
    """

    print("🔨 Creating indexes...")
    cursor.execute(indexes_sql)

    # Create triggers for user profile updates
    trigger_sql = """
    CREATE OR REPLACE FUNCTION update_user_profile()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO user_profiles (
            user_id, total_transactions, total_amount, avg_amount, 
            first_transaction, last_transaction, fraud_count, fraud_rate
        )
        VALUES (
            NEW.user_id,
            1,
            NEW.amount,
            NEW.amount,
            NEW.timestamp,
            NEW.timestamp,
            CASE WHEN NEW.is_fraud THEN 1 ELSE 0 END,
            CASE WHEN NEW.is_fraud THEN 1.0 ELSE 0.0 END
        )
        ON CONFLICT (user_id) DO UPDATE SET
            total_transactions = user_profiles.total_transactions + 1,
            total_amount = user_profiles.total_amount + NEW.amount,
            avg_amount = (user_profiles.total_amount + NEW.amount) / (user_profiles.total_transactions + 1),
            last_transaction = NEW.timestamp,
            fraud_count = user_profiles.fraud_count + CASE WHEN NEW.is_fraud THEN 1 ELSE 0 END,
            fraud_rate = (user_profiles.fraud_count + CASE WHEN NEW.is_fraud THEN 1 ELSE 0 END)::DECIMAL / (user_profiles.total_transactions + 1),
            updated_at = CURRENT_TIMESTAMP;
        
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS trigger_update_user_profile ON transactions;
    CREATE TRIGGER trigger_update_user_profile
        AFTER INSERT ON transactions
        FOR EACH ROW
        EXECUTE FUNCTION update_user_profile();
    """

    print("🔨 Creating triggers...")
    cursor.execute(trigger_sql)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ All database tables created successfully")

if __name__ == "__main__":
    if wait_for_postgres():
        if create_database_if_not_exists():
            create_tables()
            print("🎉 Database initialization completed!")
        else:
            print("❌ Failed to create database")
    else:
        print("❌ Failed to connect to PostgreSQL")