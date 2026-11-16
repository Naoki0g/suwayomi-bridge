FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY gateway.py /app/gateway.py

EXPOSE 8192
CMD ["python3", "/app/gateway.py"]
