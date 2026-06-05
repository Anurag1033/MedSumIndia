import os
import uuid
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, Dict, Any
from fastapi import File, UploadFile
import shutil
from app.kafka_producer import dispatch_document_to_kafka

# Configure logging for production observability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.makedirs("/app/storage", exist_ok=True)

app = FastAPI(
    title="MedSum-India Receiver API",
    description="Ingestion gateway for medical underwriting documents.",
    version="1.0.0"
)

# Fetch from docker-compose environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://medsum_user:supersecret123@postgres:5432/medsum_db")

def get_db_connection():
    """Establish a synchronous connection to PostgreSQL."""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# ==========================================
# Pydantic Schemas (Data Validation)
# ==========================================
class MedicalDocumentPayload(BaseModel):
    patient_name: str
    document_text: str

class ClaimStatusResponse(BaseModel):
    request_id: str
    status: str
    risk_rating: Optional[str] = None
    summary_json: Optional[Dict[str, Any]] = None

class AIResultPayload(BaseModel):
    patient_summary: Optional[str] = None
    key_diagnoses: Optional[list] = None
    risk_rating: Optional[str] = None
    underwriting_reason: Optional[str] = None
    error: Optional[str] = None

# ==========================================
# API Endpoints
# ==========================================

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "receiver"}


@app.post("/v1/summarize", status_code=status.HTTP_202_ACCEPTED)
async def submit_document(file: UploadFile = File(...)):
    """
    Receives a medical PDF/Image, saves it to shared storage, tracks it in DB, 
    and triggers the Kafka asynchronous pipeline.
    """
    request_id = str(uuid.uuid4())
    
    # 1. Save the file to the shared volume
    safe_filename = f"{request_id}_{file.filename}"
    file_path = f"/app/storage/{safe_filename}"
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        logger.error(f"Failed to save file: {e}")
        raise HTTPException(status_code=500, detail="Could not save file to storage")
        
    # 2. Persist initial state to PostgreSQL
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO claim_requests (request_id, status, raw_payload)
                VALUES (%s, %s, %s)
                """,
                (request_id, "PROCESSING", f"File path: {file_path}")
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database insertion failed: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

    # 3. Fire-and-forget to Kafka
    try:
        dispatch_document_to_kafka(request_id, file_path)
    except Exception as e:
        logger.error(f"Kafka dispatch failed: {e}")
        # In a strict production environment, we might rollback the DB here
        
    return {
        "message": "Document accepted for processing",
        "request_id": request_id,
        "status": "PROCESSING"
    }

@app.get("/v1/status/{request_id}", response_model=ClaimStatusResponse)
def get_claim_status(request_id: str):
    """
    Allows the client/UI to poll the database and check if the AI has 
    completed the risk summarization.
    """
    conn = get_db_connection()
    try:
        # RealDictCursor returns rows as dictionaries instead of tuples
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT request_id, status, risk_rating, summary_json 
                FROM claim_requests 
                WHERE request_id = %s
                """,
                (request_id,)
            )
            result = cursor.fetchone()
            
        if not result:
            raise HTTPException(status_code=404, detail="Request ID not found")
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching status for {request_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

@app.post("/api/v1/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    # ... Your existing code to save the file and generate a document_id ...
    document_id = "some-unique-uuid"
    saved_file_path = f"/app/storage/{file.filename}" 
    
    # 2. Trigger the asynchronous background pipeline here
    try:
        dispatch_document_to_kafka(document_id, saved_file_path)
    except Exception as e:
        # Return a 500 if Kafka fails to queue the job
        return {"status": "failed", "detail": "Internal messaging queue error"}
        
    # 3. Return a 202 Accepted status code indicating background processing
    return {
        "status": "pending",
        "document_id": document_id,
        "message": "Document uploaded successfully. Processing has started in the background."
    }



@app.post("/v1/documents/{request_id}/callback", include_in_schema=False)
def ai_processing_callback(request_id: str, payload: AIResultPayload):
    """Internal webhook called by the Summarizer Service when NLP is done."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if payload.error:
                # Handle AI failure gracefully
                cursor.execute(
                    "UPDATE claim_requests SET status = %s WHERE request_id = %s",
                    ("FAILED", request_id)
                )
            else:
                # Update with success data
                cursor.execute(
                    """
                    UPDATE claim_requests 
                    SET status = %s, risk_rating = %s, summary_json = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE request_id = %s
                    """,
                    ("COMPLETED", payload.risk_rating, payload.model_dump_json(), request_id)
                )
        conn.commit()
        logger.info(f"Database updated for {request_id}. Status: COMPLETED")
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update database for {request_id}: {e}")
        raise HTTPException(status_code=500, detail="Database update failed")
    finally:
        conn.close()