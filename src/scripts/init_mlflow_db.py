import psycopg2
import sys
import time

# Connection parameters
db_params = {
    'dbname': 'fraud_detection',
    'user': 'mluser',
    'password': 'mlpassword',
    'host': 'postgres',
    'port': 5432
}

# Try to connect with retries
max_retries = 5
retry_interval = 5  # seconds

for attempt in range(max_retries):
    try:
        print(f"Attempt {attempt+1} to connect to PostgreSQL...")
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if MLflow tables already exist
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'experiments');")
        mlflow_exists = cursor.fetchone()[0]

        if not mlflow_exists:
            print("MLflow tables don't exist yet. MLflow will create them on startup.")
        else:
            print("MLflow tables already exist.")

        conn.close()
        print("Database connection test successful!")
        sys.exit(0)
    except Exception as e:
        print(f"Connection failed: {e}")
        if attempt < max_retries - 1:
            print(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
        else:
            print("Maximum retries reached. Exiting.")
            sys.exit(1)