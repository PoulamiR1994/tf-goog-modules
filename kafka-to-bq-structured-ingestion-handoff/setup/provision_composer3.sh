#!/usr/bin/env bash
# ==============================================================================
# Stage PySpark Scripts & Deploy the 2 Interactive Kafka-to-BQ Iceberg DAGs
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config/job_config.yaml"
CFG_PROJECT_ID=$(awk -F': ' '/^project_id:/ {gsub(/["'\'' ]/, "", $2); print $2}' "${CONFIG_FILE}" 2>/dev/null || true)

PROJECT_ID="${PROJECT_ID:-${CFG_PROJECT_ID}}"
REGION="${REGION:-us-central1}"
ENV_NAME="${ENV_NAME:-enterprise-kafka-bq-composer3}"
DATAPROC_SA="${DATAPROC_SA:-sa-spark-kafka-bq@${PROJECT_ID}.iam.gserviceaccount.com}"
COMPOSER_SA="${COMPOSER_SA:-${DATAPROC_SA}}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:-${PROJECT_ID}-kafka-landing-zone}"

echo "======================================================================"
echo "1. Staging PySpark Ingestion, Connectors & Utilities to GCS..."
echo "======================================================================"
gcloud storage cp \
    src/jobs/kafka_bq_ingestion.py \
    src/utils/schema_and_dlq_handler.py \
    src/connectors/gmk_connector.py \
    src/connectors/external_kafka_connector.py \
    "gs://${OUTPUT_BUCKET}/scripts/" \
    --project="${PROJECT_ID}"

gcloud storage cp \
    config/job_config.yaml \
    "gs://${OUTPUT_BUCKET}/config/job_config.yaml" \
    --project="${PROJECT_ID}"

python3 test_setup/setup_kafka_sandbox.py \
    --project-id "${PROJECT_ID}" \
    --bucket "${OUTPUT_BUCKET}"

echo "======================================================================"
echo "2. Checking / Creating Cloud Composer 3 Environment: ${ENV_NAME}..."
echo "======================================================================"
if gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    STATE=$(gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(state)")
    echo "Environment ${ENV_NAME} exists (current state: ${STATE})."
else
    echo "Creating Cloud Composer 3 environment ${ENV_NAME}..."
    gcloud composer environments create "${ENV_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --image-version="composer-3-airflow-2" \
        --environment-size="small" \
        --service-account="${COMPOSER_SA}"
fi

echo "Waiting for ${ENV_NAME} to reach RUNNING state..."
while true; do
    STATE=$(gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(state)")
    if [[ "${STATE}" == "RUNNING" ]]; then
        break
    elif [[ "${STATE}" == "ERROR" ]]; then
        echo "ERROR: Composer environment creation failed!"
        exit 1
    fi
    echo "  Current state: ${STATE} (checking again in 30s)..."
    sleep 30
done

DAG_GCS_PREFIX=$(gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(config.dagGcsPrefix)")
AIRFLOW_URI=$(gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" --project="${PROJECT_ID}" --format="value(config.airflowUri)")

echo "======================================================================"
echo "3. Deploying Separated GMK & External Kafka Interactive DAGs to ${DAG_GCS_PREFIX}/..."
echo "======================================================================"
gcloud storage cp \
    dags/gmk_to_bq_iceberg_dag.py \
    dags/external_kafka_to_bq_iceberg_dag.py \
    "${DAG_GCS_PREFIX}/" \
    --project="${PROJECT_ID}"

gcloud storage cp \
    config/job_config.yaml \
    "${DAG_GCS_PREFIX}/config/job_config.yaml" \
    --project="${PROJECT_ID}"

echo "======================================================================"
echo "Deployment Complete!"
echo "Composer 3 Environment : ${ENV_NAME}"
echo "Airflow Web UI         : ${AIRFLOW_URI}"
echo "DAG GCS Folder         : ${DAG_GCS_PREFIX}"
echo "======================================================================"
