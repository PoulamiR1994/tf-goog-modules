#!/usr/bin/env bash
# ==============================================================================
# [GMK Path Only] Provision Google Cloud Managed Service for Apache Kafka (GMK)
# Region: us-central1 | Cluster: sample-kafka-target-cluster
# ==============================================================================
# Companion to `setup/provision_external_kafka_vm.sh`.
# Provisions the Google Cloud Managed Service for Apache Kafka (GMK) cluster
# on private subnet `snet-dataproc-usc1` and creates
# the 4 test topics corresponding to the 4 input data formats:
#   1. fab.equipment.telemetry.v1          (Default JSON topic)
#   2. fab.equipment.telemetry.json.v1     (Format 1: JSON)
#   3. fab.equipment.telemetry.avro.binary.v1 (Format 2: AVRO_BINARY)
#   4. fab.equipment.telemetry.avro.sr.v1  (Format 3: AVRO_SCHEMA_REGISTRY)
#   5. fab.cleanroom.metrology.struct.v1   (Format 4: SCHEMA_MAPPED_STRUCT)
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config/job_config.yaml"
CFG_PROJECT_ID=$(awk -F': ' '/^project_id:/ {gsub(/["'\'' ]/, "", $2); print $2}' "${CONFIG_FILE}" 2>/dev/null || true)

PROJECT_ID="${PROJECT_ID:-${CFG_PROJECT_ID}}"
REGION="${REGION:-us-central1}"
SUBNET_NAME="${SUBNET_NAME:-snet-dataproc-usc1}"
GMK_CLUSTER_ID="${GMK_CLUSTER_ID:-sample-kafka-target-cluster}"

echo "======================================================================"
echo "1. Provisioning Google Cloud Managed Service for Apache Kafka (GMK)"
echo "   Cluster: ${GMK_CLUSTER_ID} (${REGION})"
echo "======================================================================"
gcloud services enable managedkafka.googleapis.com --project="${PROJECT_ID}"

if ! gcloud managed-kafka clusters describe "${GMK_CLUSTER_ID}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud managed-kafka clusters create "${GMK_CLUSTER_ID}" \
        --location="${REGION}" \
        --cpu=3 \
        --memory="3GiB" \
        --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
        --auto-rebalance \
        --project="${PROJECT_ID}"
else
    echo "GMK cluster ${GMK_CLUSTER_ID} already exists."
fi

echo "Waiting for GMK cluster ${GMK_CLUSTER_ID} to reach ACTIVE state..."
while true; do
    STATE=$(gcloud managed-kafka clusters describe "${GMK_CLUSTER_ID}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(state)")
    if [[ "${STATE}" == "ACTIVE" ]]; then
        break
    fi
    echo "  GMK state: ${STATE} (retrying in 30s)..."
    sleep 30
done

echo "======================================================================"
echo "2. Creating GMK Topics for all 4 Input Data Formats..."
echo "======================================================================"
GMK_TOPICS=(
    "fab.equipment.telemetry.v1"
    "fab.equipment.telemetry.json.v1"
    "fab.equipment.telemetry.avro.binary.v1"
    "fab.equipment.telemetry.avro.sr.v1"
    "fab.cleanroom.metrology.struct.v1"
)

for TOPIC in "${GMK_TOPICS[@]}"; do
    if ! gcloud managed-kafka topics describe "${TOPIC}" \
        --cluster="${GMK_CLUSTER_ID}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud managed-kafka topics create "${TOPIC}" \
            --cluster="${GMK_CLUSTER_ID}" \
            --location="${REGION}" \
            --partitions=6 \
            --replication-factor=3 \
            --configs="cleanup.policy=delete,retention.ms=604800000" \
            --project="${PROJECT_ID}"
        echo "  Created GMK topic: ${TOPIC}"
    else
        echo "  GMK topic already exists: ${TOPIC}"
    fi
done

echo "======================================================================"
echo "GMK Cluster & Topics Ready!"
echo "Bootstrap Endpoint: bootstrap.${GMK_CLUSTER_ID}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092"
echo "======================================================================"
