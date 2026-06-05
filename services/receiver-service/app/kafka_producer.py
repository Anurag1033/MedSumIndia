import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'receiver-service-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka delivery failed: {err}")
    else:
        logger.info(f"Kafka message delivered to {msg.topic()} [{msg.partition()}]")

def dispatch_document_to_kafka(document_id: str, file_path: str):
    payload = {
        "document_id": document_id,
        "file_path": file_path
    }
    try:
        producer.produce(
            topic='medical-docs',
            key=str(document_id),
            value=json.dumps(payload),
            callback=delivery_report
        )
        producer.flush()
        logger.info(f"Successfully queued document {document_id} to Kafka.")
    except Exception as e:
        logger.error(f"Failed to publish to Kafka: {e}")
        raise e