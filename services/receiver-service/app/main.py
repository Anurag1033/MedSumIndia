import os
import uuid
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, Dict, Any

# Configure logging for production observability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# ==========================================
# API Endpoints
# ==========================================

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "receiver"}

@app.post("/v1/summarize", status_code=status.HTTP_202_ACCEPTED)
def submit_document(payload: MedicalDocumentPayload):
    """
    Receives raw medical text, generates a tracking ID, and persists it to the DB 
    with a 'PROCESSING' status. 
    """
    request_id = str(uuid.uuid4())
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO claim_requests (request_id, status, raw_payload)
                VALUES (%s, %s, %s)
                """,
                (request_id, "PROCESSING", payload.document_text)
            )
        conn.commit()
        logger.info(f"Successfully ingested document. Tracking ID: {request_id}")
        
        return {
            "message": "Document accepted for processing",
            "request_id": request_id,
            "status": "PROCESSING"
        }
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert record {request_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save document")
    finally:
        conn.close()

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