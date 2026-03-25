FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collect.py parse.py state.py entrypoint.py ./

ENV STATE_DIR=/data
CMD ["python3", "entrypoint.py"]
