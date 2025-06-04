#!/bin/bash
# Initialize and configure Vault for FraudDetectX

# Check if vault is available
if ! command -v vault &> /dev/null; then
    echo "Vault CLI not found. Please install vault."
    exit 1
fi

# Set Vault address
export VAULT_ADDR=http://localhost:8200

# Initialize Vault if needed
INIT_STATUS=$(vault status -format=json 2>/dev/null | jq -r '.initialized')

if [ "$INIT_STATUS" != "true" ]; then
    echo "Initializing Vault..."
    vault operator init -key-shares=1 -key-threshold=1 -format=json > vault-keys.json

    # Extract root token and unseal key
    export VAULT_TOKEN=$(cat vault-keys.json | jq -r '.root_token')
    UNSEAL_KEY=$(cat vault-keys.json | jq -r '.unseal_keys_b64[0]')

    # Unseal Vault
    echo "Unsealing Vault..."
    vault operator unseal $UNSEAL_KEY
else
    echo "Vault is already initialized."

    # Prompt for root token
    read -p "Please enter your Vault root token: " VAULT_TOKEN
    export VAULT_TOKEN
fi

# Check if unsealed
SEALED_STATUS=$(vault status -format=json | jq -r '.sealed')
if [ "$SEALED_STATUS" == "true" ]; then
    echo "Vault is sealed. Please unseal it first."
    exit 1
fi

# Enable secrets engines
echo "Configuring Vault secrets engines..."
vault secrets enable -path=frauddetectx kv-v2

# Store database credentials
echo "Storing database credentials..."
vault kv put frauddetectx/database \
    username="mluser" \
    password="mlpassword" \
    host="postgres" \
    port="5432" \
    dbname="fraud_detection"

# Store Kafka credentials
echo "Storing Kafka credentials..."
vault kv put frauddetectx/kafka \
    bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS}" \
    sasl_username="${KAFKA_SASL_USERNAME}" \
    sasl_password="${KAFKA_SASL_PASSWORD}" \
    security_protocol="${KAFKA_SECURITY_PROTOCOL}" \
    sasl_mechanism="${KAFKA_SASL_MECHANISM}"

echo "Vault configuration completed successfully!"
echo "Root token: $VAULT_TOKEN"
echo "Important: Save the root token and unseal key safely."
EOF

# Make script executable
chmod +x scripts/bootstrap-vault.sh