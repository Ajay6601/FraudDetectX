-- config/init.sql
-- Initialize fraud detection database

-- Create transactions table for raw data
CREATE TABLE IF NOT EXISTS transactions (
                                            transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    is_fraud BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create processed_transactions table for feature-engineered data
CREATE TABLE IF NOT EXISTS processed_transactions (
                                                      id SERIAL PRIMARY KEY,
                                                      transaction_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,

    -- Original features
    amount DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,

    -- Engineered features
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    is_night BOOLEAN,
    amount_zscore DECIMAL(8,4),
    days_since_last DECIMAL(8,2),
    transactions_last_24h INTEGER,
    transactions_last_7d INTEGER,
    user_avg_amount DECIMAL(10,2),
    high_risk_merchant BOOLEAN,
    amount_round BOOLEAN,

    -- Target
    is_fraud BOOLEAN NOT NULL DEFAULT FALSE,

    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    );

-- Create user_profiles table for user behavior tracking
CREATE TABLE IF NOT EXISTS user_profiles (
                                             user_id VARCHAR(50) PRIMARY KEY,
    total_transactions INTEGER DEFAULT 0,
    avg_amount DECIMAL(10,2) DEFAULT 0.0,
    std_amount DECIMAL(10,2) DEFAULT 0.0,
    first_transaction TIMESTAMP,
    last_transaction TIMESTAMP,
    fraud_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create model_training_runs table for MLflow integration
CREATE TABLE IF NOT EXISTS model_training_runs (
                                                   id SERIAL PRIMARY KEY,
                                                   run_id VARCHAR(100) UNIQUE NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50),
    training_data_count INTEGER,
    fraud_count INTEGER,
    accuracy DECIMAL(6,4),
    precision_score DECIMAL(6,4),
    recall_score DECIMAL(6,4),
    f1_score DECIMAL(6,4),
    auc_score DECIMAL(6,4),
    training_start TIMESTAMP,
    training_end TIMESTAMP,
    model_path VARCHAR(500),
    status VARCHAR(50) DEFAULT 'training',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud);

CREATE INDEX IF NOT EXISTS idx_processed_transactions_user_id ON processed_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_transactions_timestamp ON processed_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_transactions_is_fraud ON processed_transactions(is_fraud);

-- Create function to update user profiles
CREATE OR REPLACE FUNCTION update_user_profile()
RETURNS TRIGGER AS $$
BEGIN
INSERT INTO user_profiles (user_id, total_transactions, avg_amount, first_transaction, last_transaction, fraud_count)
VALUES (
           NEW.user_id,
           1,
           NEW.amount,
           NEW.timestamp,
           NEW.timestamp,
           CASE WHEN NEW.is_fraud THEN 1 ELSE 0 END
       )
    ON CONFLICT (user_id) DO UPDATE SET
    total_transactions = user_profiles.total_transactions + 1,
                                 avg_amount = (user_profiles.avg_amount * user_profiles.total_transactions + NEW.amount) / (user_profiles.total_transactions + 1),
                                 last_transaction = NEW.timestamp,
                                 fraud_count = user_profiles.fraud_count + CASE WHEN NEW.is_fraud THEN 1 ELSE 0 END,
        updated_at = CURRENT_TIMESTAMP;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update user profiles
DROP TRIGGER IF EXISTS trigger_update_user_profile ON transactions;
CREATE TRIGGER trigger_update_user_profile
    AFTER INSERT ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_user_profile();

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mluser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mluser;