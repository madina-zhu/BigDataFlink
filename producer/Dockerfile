FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2

COPY producer/producer.py ./producer/producer.py
COPY mock_data ./mock_data

CMD ["python", "producer/producer.py"]
