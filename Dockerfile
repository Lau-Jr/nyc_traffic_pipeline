FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so Docker can cache this layer
COPY requirements.txt .

# Upgrade pip then install — ARG busts cache if you need a forced rebuild
ARG CACHEBUST=1
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && python -c "from dotenv import load_dotenv; from loguru import logger; print('deps OK')"

# Copy application source
COPY config/       ./config/
COPY producers/    ./producers/
COPY consumers/    ./consumers/
COPY monitoring/   ./monitoring/
COPY scripts/      ./scripts/

RUN mkdir -p /app/logs

RUN useradd -m -u 1000 pipeline && chown -R pipeline:pipeline /app
USER pipeline

CMD ["python", "producers/traffic_producer.py"]
