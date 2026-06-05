FROM python:3.10-slim

WORKDIR /app

# Lean system dependencies strictly for PDF conversion and OCR
RUN apt-get update && apt-get install -y \
    gcc \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./app

CMD ["python", "-m", "app.main"]