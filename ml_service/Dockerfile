FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ml_inference_service.py .

# fraud_model.json and model_metadata.json are volume-mounted at runtime
# (see docker-compose.yml) rather than baked in, so a retrained model or a
# recalibrated threshold can be picked up with a container restart, not a
# rebuild.

CMD ["python", "ml_inference_service.py"]
