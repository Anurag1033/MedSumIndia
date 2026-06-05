from fastapi import FastAPI

app = FastAPI(title="MedSum Receiver Service")

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "receiver"}
