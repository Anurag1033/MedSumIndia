import os
import json
import time
import logging
import requests
from confluent_kafka import Consumer, KafkaError
from app.nlp_engine import MedicalUnderwritingEngine

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment Configurations
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
RECEIVER_API_URL = os.getenv("RECEIVER_API_URL", "http://receiver-service:8000")

def start_consumer_loop():
    # Initialize the NLP Engine we built in Phase 2
    engine = MedicalUnderwritingEngine()
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': 'summarizer-workers',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(['medical-docs'])
    
    logger.info("Summarizer Consumer service successfully hooked into 'medical-docs' stream.")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error encountered: {msg.error()}")
                    break
            
            # Extract and parse incoming payload
            payload = json.loads(msg.value().decode('utf-8'))
            document_id = payload.get("document_id")
            file_path = payload.get("file_path")
            
            logger.info(f"Processing Event Recieved -> Document ID: {document_id}")
            
            # 1. Fetch file bytes (Assuming shared volume or endpoint download)
            # For a pure local test, we can read directly if paths match, or fetch via API:
            try:
                # Mock step or real file system read depending on your storage mapping
                with open(file_path, "rb") as f:
                    file_bytes = f.read()
            except Exception as e:
                logger.error(f"Could not read document file path {file_path}: {e}")
                continue
                
            # 2. Run through the Phase 2 text extraction & OpenRouter evaluation
            extracted_text = engine.extract_text(file_bytes)
            analysis_result = engine.analyze_clinical_risk(extracted_text)
            
            # 3. Callback Mechanism: Ship the structured data back to the Receiver
            callback_endpoint = f"{RECEIVER_API_URL}/api/v1/documents/{document_id}/callback"
            logger.info(f"Pushing payload back to storage layer: {callback_endpoint}")
            
            try:
                response = requests.post(callback_endpoint, json=analysis_result, timeout=10)
                if response.status_code in [200, 204]:
                    logger.info(f"Successfully finalized pipeline workflow for document {document_id}")
                else:
                    logger.error(f"Receiver rejected callback payload. Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to communicate callback to Receiver Service: {e}")
                
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down consumer orchestration loop...")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Small buffer to let Kafka broker warm up entirely on clean builds
    time.sleep(5)
    start_consumer_loop()