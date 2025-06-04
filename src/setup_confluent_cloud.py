import os
import sys
import json
import subprocess
from typing import Dict, Any

class ConfluentCloudSetup:
    def __init__(self):
        self.config = {}

    def check_confluent_cli(self) -> bool:
        """Check if Confluent CLI is installed"""
        try:
            result = subprocess.run(['confluent', 'version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Confluent CLI is installed")
                return True
            else:
                print("❌ Confluent CLI not found")
                return False
        except FileNotFoundError:
            print("❌ Confluent CLI not found")
            return False

    def install_confluent_cli(self):
        """Install Confluent CLI"""
        print("📦 Installing Confluent CLI...")

        # Instructions for different platforms
        print("""
        Please install Confluent CLI manually:
        
        macOS:
        brew install confluentinc/tap/cli
        
        Linux:
        curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
        
        Windows:
        Download and install from: https://docs.confluent.io/confluent-cli/current/install.html
        """)

    def login_to_confluent(self):
        """Login to Confluent Cloud"""
        print("🔐 Please login to Confluent Cloud:")
        result = subprocess.run(['confluent', 'login'], capture_output=False)
        return result.returncode == 0

    def create_kafka_cluster(self) -> Dict[str, Any]:
        """Create Kafka cluster in Confluent Cloud"""
        print("🏗️ Creating Kafka cluster...")

        # Create environment
        env_result = subprocess.run([
            'confluent', 'environment', 'create', 'ml-fraud-detection',
            '--output', 'json'
        ], capture_output=True, text=True)

        if env_result.returncode != 0:
            print(f"❌ Failed to create environment: {env_result.stderr}")
            return {}

        env_data = json.loads(env_result.stdout)
        env_id = env_data['id']

        # Use environment
        subprocess.run(['confluent', 'environment', 'use', env_id])

        # Create cluster
        cluster_result = subprocess.run([
            'confluent', 'kafka', 'cluster', 'create', 'fraud-detection-cluster',
            '--cloud', 'aws',
            '--region', 'us-west-2',
            '--type', 'basic',
            '--output', 'json'
        ], capture_output=True, text=True)

        if cluster_result.returncode != 0:
            print(f"❌ Failed to create cluster: {cluster_result.stderr}")
            return {}

        cluster_data = json.loads(cluster_result.stdout)

        print(f"✅ Created cluster: {cluster_data['name']} ({cluster_data['id']})")

        return {
            'environment_id': env_id,
            'cluster_id': cluster_data['id'],
            'bootstrap_servers': cluster_data['endpoint']
        }

    def create_api_key(self, cluster_id: str) -> Dict[str, str]:
        """Create API key for cluster"""
        print("🔑 Creating API key...")

        result = subprocess.run([
            'confluent', 'api-key', 'create',
            '--resource', cluster_id,
            '--output', 'json'
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print(f"❌ Failed to create API key: {result.stderr}")
            return {}

        api_data = json.loads(result.stdout)

        print("✅ API key created successfully")
        print("⚠️  Save these credentials securely!")

        return {
            'api_key': api_data['key'],
            'api_secret': api_data['secret']
        }

    def create_topic(self, topic_name: str = 'transactions'):
        """Create Kafka topic"""
        print(f"📝 Creating topic: {topic_name}")

        result = subprocess.run([
            'confluent', 'kafka', 'topic', 'create', topic_name,
            '--partitions', '3',
            '--config', 'retention.ms=86400000'  # 1 day retention
        ], capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✅ Topic '{topic_name}' created successfully")
        else:
            print(f"❌ Failed to create topic: {result.stderr}")

    def generate_env_file(self, cluster_info: Dict, api_key_info: Dict):
        """Generate .env.cloud file"""
        env_content = f"""# Confluent Cloud Configuration
CONFLUENT_BOOTSTRAP_SERVERS={cluster_info['bootstrap_servers']}
CONFLUENT_API_KEY={api_key_info['api_key']}
CONFLUENT_API_SECRET={api_key_info['api_secret']}

# Kafka Configuration for Application
KAFKA_BOOTSTRAP_SERVERS={cluster_info['bootstrap_servers']}
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME={api_key_info['api_key']}
KAFKA_SASL_PASSWORD={api_key_info['api_secret']}
KAFKA_TOPIC=transactions

# Local Services Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=fraud_detection
POSTGRES_USER=mluser
POSTGRES_PASSWORD=mlpassword

REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0

SPARK_MASTER_URL=spark://spark-master:7077

# Data Configuration
NUM_USERS=8000
NUM_TRANSACTIONS=100000
FRAUD_RATE=0.005
STREAMING_RATE=100

LOG_LEVEL=INFO
"""

        with open('.env.cloud', 'w') as f:
            f.write(env_content)

        print("✅ Created .env.cloud file")
        print("📄 Copy .env.cloud to .env to use cloud Kafka")

    def setup(self):
        """Complete setup process"""
        print("🚀 Setting up Confluent Cloud for ML Pipeline")
        print("=" * 50)

        # Check CLI
        if not self.check_confluent_cli():
            self.install_confluent_cli()
            print("Please install Confluent CLI and run this script again")
            return

        # Login
        if not self.login_to_confluent():
            print("❌ Login failed")
            return

        # Create cluster
        cluster_info = self.create_kafka_cluster()
        if not cluster_info:
            return

        # Create API key
        api_key_info = self.create_api_key(cluster_info['cluster_id'])
        if not api_key_info:
            return

        # Create topic
        self.create_topic('transactions')

        # Generate env file
        self.generate_env_file(cluster_info, api_key_info)

        print("\n🎉 Confluent Cloud setup completed!")
        print("\nNext steps:")
        print("1. Copy .env.cloud to .env")
        print("2. Run: docker-compose -f docker-compose.cloud.yml up -d")
        print("3. Monitor your usage at: https://confluent.cloud")

if __name__ == "__main__":
    setup = ConfluentCloudSetup()
    setup.setup()