import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_worker():
    logger.info("Summarizer NLP Worker started. Waiting for Kafka messages...")
    while True:
        time.sleep(10)

if __name__ == "__main__":
    start_worker()
