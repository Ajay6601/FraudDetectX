# ml-training/flower_monitor.py
"""
Flower monitoring setup for Celery
"""
import os
import sys
from flower import Flower
from celery_config import CeleryConfig

def start_flower():
    """Start Flower monitoring"""
    flower_app = Flower()

    # Flower configuration
    flower_options = {
        'broker_api': os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
        'address': '0.0.0.0',
        'port': int(os.getenv('FLOWER_PORT', '5555')),
        'url_prefix': os.getenv('FLOWER_URL_PREFIX', ''),
        'basic_auth': os.getenv('FLOWER_BASIC_AUTH', ''),
        'auto_refresh': True,
        'xheaders': True
    }

    # Set options
    for key, value in flower_options.items():
        if value:
            setattr(flower_app.options, key, value)

    # Start Flower
    try:
        flower_app.start()
    except KeyboardInterrupt:
        print("Flower stopped by user")
    except Exception as e:
        print(f"Flower failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_flower()