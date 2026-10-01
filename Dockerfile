# syntax=docker/dockerfile:1
# Fraud-alert service: a small image with only what the service imports (no Spark, no DB drivers).
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# Behind a TLS-inspecting proxy (common in banks), pass its CA without baking it into a layer:
#   docker build --secret id=ca,src=/path/to/proxy-ca.crt .
RUN --mount=type=secret,id=ca,required=false \
    if [ -f /run/secrets/ca ]; then export PIP_CERT=/run/secrets/ca; fi; \
    pip install --no-cache-dir "confluent-kafka>=2.5" "fastapi>=0.115" "uvicorn>=0.30" \
        "pyyaml>=6.0" \
    && pip install --no-cache-dir --no-deps .

RUN mkdir /data && chown nobody /data
ENV ALERTS_DB=/data/alerts.db
EXPOSE 8080
USER nobody
CMD ["uvicorn", "banking_cdc.alerts_service.api:create_app_from_env", "--factory", \
     "--host", "0.0.0.0", "--port", "8080"]
