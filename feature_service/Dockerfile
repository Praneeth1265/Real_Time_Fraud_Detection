FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY feature_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY feature_service/ .

CMD ["python", "feature_service.py"]
