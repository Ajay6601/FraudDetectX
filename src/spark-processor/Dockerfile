FROM bitnami/spark:3.4

# Switch to root to install system packages
USER root

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install system dependencies - Python 3.11 is available in Debian Bookworm repos
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-venv \
        python3.11-dev \
        python3.11-distutils \
        gcc \
        libpq-dev \
        curl \
        ca-certificates && \
    # Install pip for Python 3.11 with --break-system-packages
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11 - --break-system-packages && \
    # Set Python 3.11 as default (but keep explicit commands for clarity)
    update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    # Clean up to reduce image size
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Upgrade pip and install wheel for Python 3.11 specifically
RUN python3.11 -m pip install --no-cache-dir --break-system-packages --upgrade pip setuptools wheel

# Install Python packages using Python 3.11 explicitly
RUN python3.11 -m pip install --no-cache-dir --break-system-packages --timeout 600 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.7 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    scikit-learn==1.3.0 \
    python-dotenv==1.0.0 \
    pyspark==3.4.1

# Create app directory with proper permissions
RUN mkdir -p /opt/spark-apps/logs && \
    chown -R 1001:1001 /opt/spark-apps

WORKDIR /opt/spark-apps

# Copy application files
COPY --chown=1001:1001 . .

# Drop root privilege for security
USER 1001

# Health check (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python3.11", "real_time_feature_processor.py"]