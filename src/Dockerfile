FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir websockets==13.1 confluent-kafka==2.6.1
COPY producer.py .
CMD ["python", "-u", "producer.py"]
