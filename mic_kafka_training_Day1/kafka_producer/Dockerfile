FROM python:3.9-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y nano && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "producer.py"]