import sys
import signal
import time
from kafka_consumer import SparkKafkaProcessor
from spark_config import spark_config
from loguru import logger

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if 'processor' in globals():
        processor.cleanup()
    sys.exit(0)

def main():
    """Main function"""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Get mode from command line or environment
    mode = sys.argv[1] if len(sys.argv) > 1 else os.getenv('PROCESSING_MODE', 'continuous')

    logger.info(f"Starting Spark processor in {mode} mode")
    logger.info(f"Spark config: {spark_config.spark_config}")
    logger.info(f"Kafka config: {spark_config.kafka_config}")

    # Initialize processor
    global processor
    processor = SparkKafkaProcessor()

    # Health check
    if not processor.health_check():
        logger.error("Health check failed, exiting")
        sys.exit(1)

    try:
        # Start processing
        processor.start_streaming(mode=mode)
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    finally:
        # Print final statistics
        stats = processor.get_statistics()
        logger.info(f"Final statistics: {stats}")
        processor.cleanup()
        logger.info("Spark processor stopped")

if __name__ == "__main__":
    main()