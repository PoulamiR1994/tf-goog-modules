#!/usr/bin/env bash
# ==============================================================================
# [GMK Path Only] Submit Dataproc Serverless Spark GMK-to-BigQuery Iceberg Batch
# Region: us-central1 | Auth: Keyless IAM OAUTHBEARER
# ==============================================================================
# Dedicated submission script for the Google Cloud Managed Service for Apache
# Kafka (GMK) path. Uses `src/connectors/gmk_connector.py` (OAUTHBEARER +
# GcpLoginCallbackHandler) and the shared core ingestion engine
# `src/jobs/kafka_bq_ingestion.py`.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config/job_config.yaml"
CFG_PROJECT_ID=$(awk -F': ' '/^project_id:/ {gsub(/["'\'' ]/, "", $2); print $2}' "${CONFIG_FILE}" 2>/dev/null || true)

PROJECT_ID="${PROJECT_ID:-${CFG_PROJECT_ID}}"
REGION="${REGION:-us-central1}"
SUBNET_NAME="${SUBNET_NAME:-snet-dataproc-usc1}"
OUTPUT_BUCKET="${OUTPUT_BUCKET:-${PROJECT_ID}-kafka-landing-zone}"
SA_EMAIL="${SA_EMAIL:-sa-spark-kafka-bq@${PROJECT_ID}.iam.gserviceaccount.com}"

GMK_BOOTSTRAP_SERVERS="${GMK_BOOTSTRAP_SERVERS:-bootstrap.sample-kafka-target-cluster.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-fab.equipment.telemetry.v1}"
INPUT_FORMAT="${INPUT_FORMAT:-JSON}"

EXCLUDE_PATTERNS="${EXCLUDE_PATTERNS:-*.tmp,*.bak,~\$*,*.log,*_debug_*,*heartbeat*}"
EXCLUDE_FILES="${EXCLUDE_FILES:-desktop.ini,thumbs.db,.DS_Store,TEST_TOOL_IGNORE}"
BATCH_MODE="${BATCH_MODE:-available_now}"
BATCH_SIZE="${BATCH_SIZE:-500000}"
BATCH_INTERVAL_MINUTES="${BATCH_INTERVAL_MINUTES:-5}"
STARTING_OFFSETS="${STARTING_OFFSETS:-earliest}"
CONSUMER_GROUP_ID="${CONSUMER_GROUP_ID:-cg-spark-gmk-bq-bronze}"

OUTPUT_FORMAT="${OUTPUT_FORMAT:-ICEBERG}"
OUTPUT_DATASET="${OUTPUT_DATASET:-bq_bronze_fab_telemetry_poc}"
OUTPUT_TABLE="${OUTPUT_TABLE:-fab_telemetry_events_bronze}"
DLQ_TABLE="${DLQ_TABLE:-fab_telemetry_events_dlq}"
DRY_RUN="${DRY_RUN:-false}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-id) PROJECT_ID="$2"; shift 2 ;;
        --project-id=*) PROJECT_ID="${1#*=}"; shift ;;
        --region) REGION="$2"; shift 2 ;;
        --region=*) REGION="${1#*=}"; shift ;;
        --gmk-bootstrap-servers|--kafka-bootstrap-servers) GMK_BOOTSTRAP_SERVERS="$2"; shift 2 ;;
        --gmk-bootstrap-servers=*|--kafka-bootstrap-servers=*) GMK_BOOTSTRAP_SERVERS="${1#*=}"; shift ;;
        --kafka-topic|--topic) KAFKA_TOPIC="$2"; shift 2 ;;
        --kafka-topic=*|--topic=*) KAFKA_TOPIC="${1#*=}"; shift ;;
        --input-format) INPUT_FORMAT="$2"; shift 2 ;;
        --input-format=*) INPUT_FORMAT="${1#*=}"; shift ;;
        --exclude-patterns) EXCLUDE_PATTERNS="$2"; shift 2 ;;
        --exclude-patterns=*) EXCLUDE_PATTERNS="${1#*=}"; shift ;;
        --exclude-files) EXCLUDE_FILES="$2"; shift 2 ;;
        --exclude-files=*) EXCLUDE_FILES="${1#*=}"; shift ;;
        --batch-mode) BATCH_MODE="$2"; shift 2 ;;
        --batch-mode=*) BATCH_MODE="${1#*=}"; shift ;;
        --batch-size) BATCH_SIZE="$2"; shift 2 ;;
        --batch-size=*) BATCH_SIZE="${1#*=}"; shift ;;
        --batch-interval-minutes) BATCH_INTERVAL_MINUTES="$2"; shift 2 ;;
        --batch-interval-minutes=*) BATCH_INTERVAL_MINUTES="${1#*=}"; shift ;;
        --output-format) OUTPUT_FORMAT="$2"; shift 2 ;;
        --output-format=*) OUTPUT_FORMAT="${1#*=}"; shift ;;
        --output-dataset) OUTPUT_DATASET="$2"; shift 2 ;;
        --output-dataset=*) OUTPUT_DATASET="${1#*=}"; shift ;;
        --output-table) OUTPUT_TABLE="$2"; shift 2 ;;
        --output-table=*) OUTPUT_TABLE="${1#*=}"; shift ;;
        --dlq-table) DLQ_TABLE="$2"; shift 2 ;;
        --dlq-table=*) DLQ_TABLE="${1#*=}"; shift ;;
        --output-bucket) OUTPUT_BUCKET="$2"; shift 2 ;;
        --output-bucket=*) OUTPUT_BUCKET="${1#*=}"; shift ;;
        --dry-run) DRY_RUN="true"; shift ;;
        *) shift ;;
    esac
done

SUBNET="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}"
FORMAT_CLEAN=$(echo "${INPUT_FORMAT}" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
BATCH_ID="dp-gmktobq-${FORMAT_CLEAN}-$(date +%Y%m%d-%H%M%S)"

AVRO_SCHEMA_PATH="gs://${OUTPUT_BUCKET}/schemas/fab_telemetry_event.avsc"
SCHEMA_MAPPING_URI="gs://${OUTPUT_BUCKET}/schemas/fab_schema_mapping.json"
SCHEMA_REGISTRY_URL="https://managedkafka.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/schemaRegistries/mfg-telemetry-sr-poc"
CHECKPOINT_URI="gs://${OUTPUT_BUCKET}/checkpoints/gmk_to_bq/${KAFKA_TOPIC}_${FORMAT_CLEAN}"

echo "======================================================================"
echo "[GMK Path] Submitting Dataproc Serverless Batch: ${BATCH_ID}"
echo "GMK Bootstrap Servers: ${GMK_BOOTSTRAP_SERVERS}"
echo "Auth Mechanism:        SASL_SSL / OAUTHBEARER (GcpLoginCallbackHandler)"
echo "Kafka Topic:           ${KAFKA_TOPIC}"
echo "Input Format:          ${INPUT_FORMAT}"
echo "Exclude Patterns:      ${EXCLUDE_PATTERNS}"
echo "Exclude Exact List:    ${EXCLUDE_FILES}"
echo "Batch Trigger Mode:    ${BATCH_MODE} (size=${BATCH_SIZE}, interval=${BATCH_INTERVAL_MINUTES}m)"
echo "Target Bronze Table:   ${PROJECT_ID}.${OUTPUT_DATASET}.${OUTPUT_TABLE} (${OUTPUT_FORMAT})"
echo "======================================================================"

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[DRY-RUN] Skipping gcloud dataproc batches submit execution."
    exit 0
fi

gcloud storage cp \
    src/jobs/kafka_bq_ingestion.py \
    src/utils/schema_and_dlq_handler.py \
    src/connectors/gmk_connector.py \
    src/connectors/external_kafka_connector.py \
    "gs://${OUTPUT_BUCKET}/scripts/" \
    --project="${PROJECT_ID}"

gcloud dataproc batches submit pyspark \
    "gs://${OUTPUT_BUCKET}/scripts/kafka_bq_ingestion.py" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="${BATCH_ID}" \
    --version="2.2" \
    --service-account="${SA_EMAIL}" \
    --subnet="${SUBNET}" \
    --deps-bucket="gs://${OUTPUT_BUCKET}" \
    --py-files="gs://${OUTPUT_BUCKET}/scripts/schema_and_dlq_handler.py,gs://${OUTPUT_BUCKET}/scripts/gmk_connector.py,gs://${OUTPUT_BUCKET}/scripts/external_kafka_connector.py" \
    --properties="^#^spark.dynamicAllocation.enabled=true#spark.dynamicAllocation.minExecutors=2#spark.dynamicAllocation.maxExecutors=12#spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-avro_2.13:3.5.1,com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.5#spark.jars.excludes=io.confluent:kafka-schema-registry-client" \
    --labels="env=poc,domain=fab-telemetry,source=gmk,track=dataproc-pyspark,format=${FORMAT_CLEAN}" \
    -- \
    "--project-id=${PROJECT_ID}" \
    "--region=${REGION}" \
    "--kafka-bootstrap-servers=${GMK_BOOTSTRAP_SERVERS}" \
    "--kafka-topic=${KAFKA_TOPIC}" \
    "--kafka-source-type=GMK" \
    "--input-format=${INPUT_FORMAT}" \
    "--avro-schema-path=${AVRO_SCHEMA_PATH}" \
    "--schema-mapping-uri=${SCHEMA_MAPPING_URI}" \
    "--schema-registry-url=${SCHEMA_REGISTRY_URL}" \
    "--exclude-patterns=${EXCLUDE_PATTERNS}" \
    "--exclude-files=${EXCLUDE_FILES}" \
    "--batch-mode=${BATCH_MODE}" \
    "--batch-size=${BATCH_SIZE}" \
    "--batch-interval-minutes=${BATCH_INTERVAL_MINUTES}" \
    "--starting-offsets=${STARTING_OFFSETS}" \
    "--consumer-group-id=${CONSUMER_GROUP_ID}" \
    "--output-format=${OUTPUT_FORMAT}" \
    "--output-dataset=${OUTPUT_DATASET}" \
    "--output-table=${OUTPUT_TABLE}" \
    "--dlq-table=${DLQ_TABLE}" \
    "--output-bucket=${OUTPUT_BUCKET}" \
    "--checkpoint-location=${CHECKPOINT_URI}" \
    "--write-method=direct"
