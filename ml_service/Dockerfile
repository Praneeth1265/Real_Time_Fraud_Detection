FROM python:3.10

WORKDIR /app

# Copy requirements first (better caching)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy model files and service code
COPY app.py .
COPY fraud_detection_model.json .
COPY model_metadata.json .

# Run FastAPI with uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
