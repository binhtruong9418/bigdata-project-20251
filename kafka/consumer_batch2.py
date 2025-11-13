import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import time

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_hdfs_consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
KAFKA_TOPIC = 'example_topic'
KAFKA_GROUP_ID = 'group2'

# HDFS configuration
HDFS_HOST = 'http://localhost:9870'
HDFS_USER = 'root'
HDFS_BASE_PATH = '/data/kafka_messages'

# Batch processing configuration
BATCH_SIZE = 10  # Number of messages to collect before writing to HDFS
BATCH_TIMEOUT = 10  # Seconds to wait before writing batch even if not full

def setup_hdfs_client():
    try:
        return InsecureClient(HDFS_HOST, user=HDFS_USER)
    except Exception as e:
        logger.error(f"Failed to create HDFS client: {e}")
        raise

def create_hdfs_path(hdfs_client, date_path):
    try:
        if not hdfs_client.status(date_path, strict=False):
            hdfs_client.makedirs(date_path)
            logger.info(f"Created HDFS directory: {date_path}")
    except Exception as e:
        logger.error(f"Error creating HDFS directory {date_path}: {e}")
        raise

def save_batch_to_hdfs(hdfs_client, batch_messages, base_path):
    try:
        current_time = datetime.now()
        date_path = os.path.join(
            base_path,
            current_time.strftime('%Y'),
            current_time.strftime('%m'),
            current_time.strftime('%d')
        )
        create_hdfs_path(hdfs_client, date_path)

        filename = f"{current_time.strftime('%H_%M_%S_%f')}_batch.json"
        full_path = os.path.join(date_path, filename)

        with hdfs_client.write(full_path, encoding='utf-8') as writer:
            json.dump(batch_messages, writer)
        
        logger.info(f"Batch of {len(batch_messages)} messages saved to {full_path}")
    except Exception as e:
        logger.error(f"Error saving batch to HDFS: {e}")

def kafka_hdfs_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        fetch_max_wait_ms=1,
        enable_auto_commit=False,
        max_poll_records=BATCH_SIZE,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    hdfs_client = setup_hdfs_client()
    batch_messages = []
    last_batch_time = time.time()

    try:
        for message in consumer:
            if message is not None:
                batch_messages.append(message.value)

                # Write to HDFS if batch is full or timeout is reached
                if (len(batch_messages) >= BATCH_SIZE or 
                    time.time() - last_batch_time >= BATCH_TIMEOUT):
                    save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
                    batch_messages = []
                    last_batch_time = time.time()
                    consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        # Save any remaining messages
        if batch_messages:
            save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
        consumer.close()

if __name__ == "__main__":
    kafka_hdfs_consumer()