
# 🏥 MedSum-India: AI-Powered Medical Summarization Pipeline

An asynchronous, event-driven microservices architecture designed to automate medical underwriting for the Indian health insurance sector. This Proof of Concept (POC) ingests unstructured medical documents (e.g., hospital discharge summaries, Ayushman Bharat records), extracts clinical text, and leverages LLMs to output structured risk stratifications.

## 🚀 Business Value
Manual medical underwriting is a bottleneck in claim processing. This platform accelerates decision-making by:
1. **Automating Text Extraction**: Handling both native PDFs and scanned images via OCR.
2. **Clinical Summarization**: Distilling multi-page medical histories into concise summaries.
3. **Risk Stratification**: Automatically classifying patients into deterministic risk buckets (**RED**, **YELLOW**, **GREEN**) based on strict underwriting guidelines.

---

## 🏗️ System Architecture

The system utilizes an asynchronous worker pattern decoupled by Apache Kafka to ensure high availability and scalability during heavy document ingestion.

### Workflow Diagram

```mermaid
graph TD
    %% Define Styles
    classDef client fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef api fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef db fill:#fff3e0,stroke:#f57c00,stroke-width:2px;
    classDef kafka fill:#e8f5e9,stroke:#388e3c,stroke-width:2px;
    classDef worker fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px;
    classDef external fill:#ffebee,stroke:#d32f2f,stroke-width:2px;

    %% Nodes
    Client([Client / Frontend UI]):::client
    Receiver[Receiver Service <br/> FastAPI]:::api
    Postgres[(PostgreSQL <br/> Database)]:::db
    Storage[(Shared Docker Volume <br/> /app/storage)]:::db
    Broker{Apache Kafka <br/> Topic: medical-docs}:::kafka
    Summarizer[Summarizer Service <br/> Async Worker]:::worker
    OpenRouter((OpenRouter API <br/> Llama-3-8B)):::external

    %% Flow
    Client -- "1. POST /v1/summarize (PDF)" --> Receiver
    Receiver -- "2. Save File" --> Storage
    Receiver -- "3. Insert Status: PROCESSING" --> Postgres
    Receiver -- "4. Publish Event" --> Broker
    Receiver -- "5. Return 202 Accepted + UUID" --> Client
    
    Broker -- "6. Consume Event" --> Summarizer
    Summarizer -- "7. Fetch File Bytes" --> Storage
    Summarizer -- "8. Extract Text (PyMuPDF / Tesseract)" --> Summarizer
    Summarizer -- "9. Send Text + Prompt" --> OpenRouter
    OpenRouter -- "10. Return Strict JSON" --> Summarizer
    
    Summarizer -- "11. POST /v1/documents/{id}/callback" --> Receiver
    Receiver -- "12. Update Status: COMPLETED + JSON" --> Postgres
    Client -- "13. Polling: GET /v1/status/{id}" --> Postgres

```

---

## 🛠️ Technology Stack

* **API Gateway & Routing**: FastAPI, Uvicorn, Pydantic
* **Message Broker**: Apache Kafka (Official Apache image with KRaft mode)
* **Database**: PostgreSQL (Relational states + JSONB storage)
* **NLP & Extraction**: PyMuPDF (Native Text), Tesseract OCR / Poppler (Image fallback)
* **AI Engine**: OpenAI Python SDK routed through OpenRouter (`meta-llama/llama-3-8b-instruct`)
* **Infrastructure**: Docker & Docker Compose (Multi-stage builds, shared volumes)

---

## 📁 Repository Structure

```text
medsum-india/
├── docker-compose.yml          # Orchestrates Postgres, Kafka, and Microservices
├── .env                        # Environment variables (Ignored by Git)
├── .gitignore                  # Production security rules
├── infrastructure/
│   └── init.sql                # Auto-provisions PostgreSQL tables on boot
└── services/
    ├── receiver-service/       # Ingestion & Database Interface
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── app/
    │       ├── main.py         # FastAPI Endpoints & Callbacks
    │       └── kafka_producer.py
    └── summarizer-service/     # AI & NLP Processing Worker
        ├── Dockerfile
        ├── requirements.txt
        └── app/
            ├── main.py         # Kafka Consumer Loop
            ├── nlp_engine.py   # Hybrid OCR & LLM Integration
            └── test_phase2.py  # Standalone testing module

```

---

## ⚙️ Getting Started

### Prerequisites

* Docker Desktop installed and running.
* Git installed.
* An active [OpenRouter](https://openrouter.ai/) API key.

### 1. Clone the Repository

```bash
git clone [https://github.com/your-username/medsum-india.git](https://github.com/your-username/medsum-india.git)
cd medsum-india

```

### 2. Environment Setup

Create a `.env` file in the root directory:

```bash
DB_USER=medsum_user
DB_PASSWORD=supersecret123
DB_NAME=medsum_db
OPENROUTER_API_KEY=sk-or-v1-your-actual-api-key-here

```

### 3. Launch the Architecture

Build and deploy the microservices using Docker Compose. The initial boot will provision the database, establish Kafka KRaft brokers, and install all Python/C++ dependencies.

```bash
docker compose up -d --build

```

*Note: To view the live logs of the background AI worker, use `docker logs -f medsum_summarizer`.*

---

## 🔌 API Documentation

Once the containers are running, the interactive Swagger UI is available at:
👉 **[http://localhost:8000/docs](https://www.google.com/search?q=http://localhost:8000/docs)**

### 1. Upload a Document

* **Endpoint**: `POST /v1/summarize`
* **Content-Type**: `multipart/form-data`
* **Response**: Returns a tracking `request_id`.

```json
{
  "message": "Document accepted for processing",
  "request_id": "c1b2a3d4-e5f6-7890-abcd-1234567890ab",
  "status": "PROCESSING"
}

```

### 2. Check Claim Status

* **Endpoint**: `GET /v1/status/{request_id}`
* **Response**: Returns the asynchronous processing state. Once `COMPLETED`, it includes the structured clinical risk JSON.

```json
{
  "request_id": "c1b2a3d4-e5f6-7890-abcd-1234567890ab",
  "status": "COMPLETED",
  "risk_rating": "YELLOW",
  "summary_json": {
    "patient_summary": "Patient admitted for dizziness, diagnosed with essential hypertension...",
    "key_diagnoses": ["Essential Hypertension"],
    "risk_rating": "YELLOW",
    "underwriting_reason": "Managed lifestyle disease requiring premium loading."
  }
}

```

---

## 🛣️ Project Roadmap

* [x] **Phase 1:** Foundation & API Scaffolding (FastAPI + PostgreSQL)
* [x] **Phase 2:** Core NLP Engine (OCR Fallback + OpenRouter JSON Enforcement)
* [x] **Phase 3:** Event-Driven Integration (Apache Kafka Asynchronous Loops)
* [ ] **Phase 4:** Reliability Engineering (PyTest, Testcontainers, and CI/CD pipelines) *<- Current Phase*

```
***

### How to add this to your project:
1. Create a file named `README.md` in your `C:\Users\Anurag\OneDrive\Desktop\MedSumIndia` folder.
2. Paste the contents of the code block above into it.
3. Commit it to GitHub:
   ```bash
   git add README.md
   git commit -m "docs: generate comprehensive README with mermaid architecture diagrams"
   git push origin <your-branch-name>

```
