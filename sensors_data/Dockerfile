FROM python:3.11-slim

WORKDIR /app

# Install minimal build tools for some wheels if necessary
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "simulate_sensors_data.py"]
