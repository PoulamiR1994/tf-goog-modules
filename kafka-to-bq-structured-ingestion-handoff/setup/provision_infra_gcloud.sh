#!/usr/bin/env bash
# ==============================================================================
# GCP Infrastructure Provisioning Script for Kafka to BigQuery Iceberg Ingestion
# Region: us-central1 | Dataproc Serverless
# ==============================================================================
# Implements all networking, Cloud DNS, firewall, IAM, BigLake Iceberg, and
# Secret Manager prerequisites.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config/job_config.yaml"
CFG_PROJECT_ID=$(awk -F': ' '/^project_id:/ {gsub(/["'\'' ]/, "", $2); print $2}' "${CONFIG_FILE}" 2>/dev/null || true)

PROJECT_ID="${PROJECT_ID:-${CFG_PROJECT_ID}}"
REGION="${REGION:-us-central1}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-kafka-landing-zone}"
SA_NAME="${SA_NAME:-sa-spark-kafka-bq}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Networking Primitives
NETWORK_NAME="${NETWORK_NAME:-kafka-bq-vpc}"
SUBNET_NAME="${SUBNET_NAME:-snet-dataproc-usc1}"
SUBNET_RANGE="${SUBNET_RANGE:-10.20.0.0/24}"
GMK_SUBNET_NAME="${GMK_SUBNET_NAME:-snet-gmk-usc1}"
GMK_SUBNET_RANGE="${GMK_SUBNET_RANGE:-10.20.1.0/24}"
ONPREM_KAFKA_CIDR="${ONPREM_KAFKA_CIDR:-10.100.0.0/16}"
RESTRICTED_GOOGLEAPIS_CIDR="199.36.153.4/30"

# BigQuery & BigLake Iceberg Primitives
BRONZE_DATASET="${BRONZE_DATASET:-bq_bronze_fab_telemetry_poc}"
GOVERNANCE_DATASET="${GOVERNANCE_DATASET:-governance}"
BIGLAKE_CONNECTION_ID="${BIGLAKE_CONNECTION_ID:-gcp_lakehouse_connection}"

# Optional GMK Cluster Provisioning (Set PROVISION_GMK=true or PROVISION_GMK_CLUSTER=true to create a GMK cluster)
PROVISION_GMK="${PROVISION_GMK:-${PROVISION_GMK_CLUSTER:-false}}"
GMK_CLUSTER_ID="${GMK_CLUSTER_ID:-gmk-fab-telemetry-poc}"

echo "======================================================================"
echo "Starting GCP Infrastructure Provisioning for Project: ${PROJECT_ID}"
echo "======================================================================"

# 0. Enable Required GCP APIs
echo "[Step 0] Enabling required Google Cloud APIs..."
gcloud services enable \
    dataproc.googleapis.com \
    managedkafka.googleapis.com \
    composer.googleapis.com \
    bigquery.googleapis.com \
    bigquerystorage.googleapis.com \
    biglake.googleapis.com \
    bigqueryconnection.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com \
    compute.googleapis.com \
    dns.googleapis.com \
    dataplex.googleapis.com \
    artifactregistry.googleapis.com \
    cloudkms.googleapis.com \
    logging.googleapis.com \
    --project="${PROJECT_ID}"

PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
echo "Resolved Project Number: ${PROJECT_NUMBER}"

# 1. Provision Destination GCS Bucket (Checkpoints, Schemas, Iceberg Warehouse)
echo "[Step 1] Provisioning GCS Bucket: gs://${BUCKET_NAME}..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Bucket gs://${BUCKET_NAME} already exists. Enforcing security configurations..."
else
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --default-storage-class=STANDARD \
        --uniform-bucket-level-access
fi

gcloud storage buckets update "gs://${BUCKET_NAME}" \
    --public-access-prevention \
    --versioning \
    --project="${PROJECT_ID}"

# 2. Create Custom Spark Runtime Service Account & Grant Least-Privilege IAM
echo "[Step 2] Creating Custom Spark Runtime Service Account: ${SA_EMAIL}..."
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Service account ${SA_EMAIL} already exists."
else
    gcloud iam service-accounts create "${SA_NAME}" \
        --project="${PROJECT_ID}" \
        --display-name="Dataproc Serverless Spark Kafka-to-BQ SA" \
        --description="Zero-trust customer-managed runtime SA for Kafka to BigQuery Iceberg ingestion"
fi

echo "Granting mandatory project-level IAM roles to ${SA_EMAIL}..."
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectAdmin"

for ROLE in \
    roles/dataproc.worker \
    roles/dataproc.editor \
    roles/composer.worker \
    roles/iam.serviceAccountUser \
    roles/managedkafka.client \
    roles/managedkafka.schemaRegistryViewer \
    roles/bigquery.jobUser \
    roles/biglake.admin \
    roles/logging.logWriter; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${ROLE}" \
        --condition=None >/dev/null
    echo "  Granted ${ROLE} to ${SA_EMAIL}"
done

# 3. Configure VPC Network, Subnets (/24 with PGA), Cloud DNS & Firewall Rules
echo "[Step 3] Configuring Zero-Public-Internet VPC, Cloud DNS & Firewall Rules..."
if gcloud compute networks describe "${NETWORK_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "VPC Network ${NETWORK_NAME} already exists."
else
    gcloud compute networks create "${NETWORK_NAME}" \
        --project="${PROJECT_ID}" \
        --subnet-mode=custom
fi

# 3a. Dataproc Serverless Compute Subnet (/24 with Private Google Access enabled)
if gcloud compute networks subnets describe "${SUBNET_NAME}" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute networks subnets update "${SUBNET_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --enable-private-ip-google-access
else
    gcloud compute networks subnets create "${SUBNET_NAME}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --network="${NETWORK_NAME}" \
        --range="${SUBNET_RANGE}" \
        --enable-private-ip-google-access
fi

# 3b. GMK Broker Connected Subnet (for PSC endpoints on tcp:9092)
if gcloud compute networks subnets describe "${GMK_SUBNET_NAME}" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute networks subnets update "${GMK_SUBNET_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --enable-private-ip-google-access
else
    gcloud compute networks subnets create "${GMK_SUBNET_NAME}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --network="${NETWORK_NAME}" \
        --range="${GMK_SUBNET_RANGE}" \
        --enable-private-ip-google-access
fi

# 3c. Optional Restricted Google Access DNS & Route (Set ENABLE_VPCSC_RESTRICTED_DNS=true when VPC-SC perimeter is active)
ENABLE_VPCSC_RESTRICTED_DNS="${ENABLE_VPCSC_RESTRICTED_DNS:-false}"
if [[ "${ENABLE_VPCSC_RESTRICTED_DNS}" == "true" ]]; then
    if ! gcloud compute routes describe "rt-restricted-googleapis" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud compute routes create "rt-restricted-googleapis" \
            --project="${PROJECT_ID}" \
            --network="${NETWORK_NAME}" \
            --destination-range="${RESTRICTED_GOOGLEAPIS_CIDR}" \
            --next-hop-gateway="default-internet-gateway" \
            --priority=1000
    fi

    if ! gcloud dns managed-zones describe "dz-restricted-googleapis" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud dns managed-zones create "dz-restricted-googleapis" \
            --project="${PROJECT_ID}" \
            --dns-name="googleapis.com." \
            --description="Restricted Google Access (199.36.153.4/30) for VPC-SC compliance" \
            --visibility="private" \
            --networks="${NETWORK_NAME}"

        gcloud dns record-sets create "restricted.googleapis.com." \
            --project="${PROJECT_ID}" \
            --zone="dz-restricted-googleapis" \
            --type="A" \
            --ttl=300 \
            --rrdatas="199.36.153.4,199.36.153.5,199.36.153.6,199.36.153.7"

        gcloud dns record-sets create "*.googleapis.com." \
            --project="${PROJECT_ID}" \
            --zone="dz-restricted-googleapis" \
            --type="CNAME" \
            --ttl=300 \
            --rrdatas="restricted.googleapis.com."
    fi

    if ! gcloud dns managed-zones describe "dz-restricted-pkg-dev" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud dns managed-zones create "dz-restricted-pkg-dev" \
            --project="${PROJECT_ID}" \
            --dns-name="pkg.dev." \
            --description="Restricted Google Access for Artifact Registry (pkg.dev) under VPC-SC" \
            --visibility="private" \
            --networks="${NETWORK_NAME}"

        gcloud dns record-sets create "*.pkg.dev." \
            --project="${PROJECT_ID}" \
            --zone="dz-restricted-pkg-dev" \
            --type="CNAME" \
            --ttl=300 \
            --rrdatas="restricted.googleapis.com."
    fi

    if ! gcloud dns managed-zones describe "dz-restricted-gcr-io" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud dns managed-zones create "dz-restricted-gcr-io" \
            --project="${PROJECT_ID}" \
            --dns-name="gcr.io." \
            --description="Restricted Google Access for Container Registry (gcr.io) under VPC-SC" \
            --visibility="private" \
            --networks="${NETWORK_NAME}"

        gcloud dns record-sets create "*.gcr.io." \
            --project="${PROJECT_ID}" \
            --zone="dz-restricted-gcr-io" \
            --type="CNAME" \
            --ttl=300 \
            --rrdatas="restricted.googleapis.com."
    fi
fi

# 3e. VPC Firewall Rules Matrix
# Rule 1: fw-allow-google-apis-egress (tcp:443 to 199.36.153.4/30)
if ! gcloud compute firewall-rules describe "fw-allow-google-apis-egress" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "fw-allow-google-apis-egress" \
        --project="${PROJECT_ID}" \
        --network="${NETWORK_NAME}" \
        --direction=EGRESS \
        --action=ALLOW \
        --rules=tcp:443 \
        --destination-ranges="${RESTRICTED_GOOGLEAPIS_CIDR}" \
        --priority=1000
fi

# Rule 2: fw-allow-gmk-psc-brokers (tcp:9092 SASL, tcp:9192 mTLS to GMK Connected Subnet CIDR)
if ! gcloud compute firewall-rules describe "fw-allow-gmk-psc-brokers" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "fw-allow-gmk-psc-brokers" \
        --project="${PROJECT_ID}" \
        --network="${NETWORK_NAME}" \
        --direction=EGRESS \
        --action=ALLOW \
        --rules=tcp:9092,tcp:9192 \
        --destination-ranges="${GMK_SUBNET_RANGE}" \
        --priority=1000
fi

# Rule 3: fw-allow-onprem-fab-kafka-egress (tcp:9092,9093,8081 to External/On-Prem Fab Kafka CIDRs)
if ! gcloud compute firewall-rules describe "fw-allow-onprem-fab-kafka-egress" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "fw-allow-onprem-fab-kafka-egress" \
        --project="${PROJECT_ID}" \
        --network="${NETWORK_NAME}" \
        --direction=EGRESS \
        --action=ALLOW \
        --rules=tcp:9092,tcp:9093,tcp:8081 \
        --destination-ranges="${ONPREM_KAFKA_CIDR}" \
        --priority=1000
fi

# Rule 4: fw-allow-dataproc-intra-subnet (Ingress & Egress tcp/udp/icmp within Dataproc Primary Subnet CIDR)
if ! gcloud compute firewall-rules describe "fw-allow-dataproc-intra-subnet" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "fw-allow-dataproc-intra-subnet" \
        --project="${PROJECT_ID}" \
        --network="${NETWORK_NAME}" \
        --direction=INGRESS \
        --action=ALLOW \
        --rules=tcp,udp,icmp \
        --source-ranges="${SUBNET_RANGE}" \
        --priority=1000
fi

if ! gcloud compute firewall-rules describe "fw-allow-dataproc-intra-subnet-egress" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute firewall-rules create "fw-allow-dataproc-intra-subnet-egress" \
        --project="${PROJECT_ID}" \
        --network="${NETWORK_NAME}" \
        --direction=EGRESS \
        --action=ALLOW \
        --rules=tcp,udp,icmp \
        --destination-ranges="${SUBNET_RANGE}" \
        --priority=1000
fi

# 3f. Cloud Router & Cloud NAT (Allows --no-address Dataproc Serverless & External Kafka VM to pull Maven jars / Docker images)
if ! gcloud compute routers describe "cr-kafka-bq-usc1" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute routers create "cr-kafka-bq-usc1" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --network="${NETWORK_NAME}"
fi

if ! gcloud compute routers nats describe "nat-kafka-bq-usc1" --router="cr-kafka-bq-usc1" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute routers nats create "nat-kafka-bq-usc1" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --router="cr-kafka-bq-usc1" \
        --auto-allocate-nat-external-ips \
        --nat-all-subnet-ip-ranges
fi

# 4. Provision BigQuery Cloud Resource Connection & Datasets
echo "[Step 4] Configuring BigLake Connection & BigQuery Bronze Dataset..."
if ! bq show --location="${REGION}" --connection "${PROJECT_ID}.${REGION}.${BIGLAKE_CONNECTION_ID}" >/dev/null 2>&1; then
    bq mk --connection \
        --connection_type=CLOUD_RESOURCE \
        --project_id="${PROJECT_ID}" \
        --location="${REGION}" \
        "${BIGLAKE_CONNECTION_ID}"
fi

echo "Granting BigQuery Connection User on ${BIGLAKE_CONNECTION_ID} to ${SA_EMAIL}..."
bq --location="${REGION}" query --use_legacy_sql=false --label datacloud:antigravity --project_id="${PROJECT_ID}" \
    "GRANT \`roles/bigquery.connectionUser\` ON EXTERNAL CONNECTION \`${PROJECT_ID}.${REGION}.${BIGLAKE_CONNECTION_ID}\` TO \"serviceAccount:${SA_EMAIL}\"" >/dev/null 2>&1 || \
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.connectionUser" \
    --condition=None >/dev/null

CONN_SA=$(bq show --location="${REGION}" --format=json --connection "${PROJECT_ID}.${REGION}.${BIGLAKE_CONNECTION_ID}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('cloudResource', {}).get('serviceAccountId', ''))")
if [[ -n "${CONN_SA}" ]]; then
    echo "Granting Storage Admin & Object Admin to BigLake Connection SA: ${CONN_SA}..."
    gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
        --member="serviceAccount:${CONN_SA}" \
        --role="roles/storage.admin" >/dev/null
    gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
        --member="serviceAccount:${CONN_SA}" \
        --role="roles/storage.objectAdmin" >/dev/null
    gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
        --member="serviceAccount:${CONN_SA}" \
        --role="roles/storage.legacyBucketReader" >/dev/null
fi

for DS in "${BRONZE_DATASET}" "${GOVERNANCE_DATASET}"; do
    if ! bq --location="${REGION}" show --dataset "${PROJECT_ID}:${DS}" >/dev/null 2>&1; then
        bq --location="${REGION}" mk --dataset \
            --label datacloud:antigravity \
            --description="Kafka to BigQuery Iceberg Lakehouse Dataset (${DS})" \
            "${PROJECT_ID}:${DS}"
    fi
    echo "Granting dataset-scoped BigQuery Data Editor on ${PROJECT_ID}:${DS} to ${SA_EMAIL}..."
    bq --location="${REGION}" query --use_legacy_sql=false --label datacloud:antigravity --project_id="${PROJECT_ID}" \
        "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`${PROJECT_ID}.${DS}\` TO \"serviceAccount:${SA_EMAIL}\"" >/dev/null
done

# Enable PHYSICAL (Compressed) Storage Billing on Bronze Dataset
bq --location="${REGION}" query --use_legacy_sql=false --label datacloud:antigravity --project_id="${PROJECT_ID}" \
    "ALTER SCHEMA \`${PROJECT_ID}.${BRONZE_DATASET}\` SET OPTIONS(storage_billing_model = 'PHYSICAL')" >/dev/null 2>&1 || true

# 5. Provision Secret Manager Placeholders & Secret-Scoped IAM for PAM Credentials
echo "[Step 5] Creating Secret Manager entries and secret-scoped IAM bindings for Kafka PAM credentials..."
for SECRET in kafka-pam-credentials kafka-sasl-username kafka-sasl-password kafka-truststore-password; do
    if gcloud secrets describe "${SECRET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        echo "Secret ${SECRET} already exists."
    else
        gcloud secrets create "${SECRET}" \
            --project="${PROJECT_ID}" \
            --replication-policy="automatic"
        echo "Created secret: ${SECRET}"
    fi
    gcloud secrets add-iam-policy-binding "${SECRET}" \
        --project="${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="roles/secretmanager.secretAccessor" >/dev/null
    echo "  Granted secret-scoped roles/secretmanager.secretAccessor on ${SECRET} to ${SA_EMAIL}"
done

# 6. Optional: Provision Google Cloud Managed Service for Apache Kafka (GMK) Cluster
if [[ "${PROVISION_GMK}" == "true" ]]; then
    echo "[Step 6] Provisioning GMK Cluster ${GMK_CLUSTER_ID} in ${REGION}..."
    if ! gcloud managed-kafka clusters describe "${GMK_CLUSTER_ID}" --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud managed-kafka clusters create "${GMK_CLUSTER_ID}" \
            --project="${PROJECT_ID}" \
            --location="${REGION}" \
            --cpu=3 \
            --memory=3GiB \
            --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${GMK_SUBNET_NAME}" \
            --auto-rebalance \
            --labels="env=poc,domain=fab-telemetry,pattern=kafka-to-bq" \
            --async
        echo "GMK cluster creation initiated asynchronously."
    else
        echo "GMK cluster ${GMK_CLUSTER_ID} already exists."
    fi
fi

# 7. Optional: Provision External Kafka Broker on GCE VM (10.20.0.50:9093 with SASL/SCRAM-SHA-512)
if [[ "${PROVISION_EXTERNAL_KAFKA_VM:-false}" == "true" ]]; then
    echo "[Step 7] Provisioning External Kafka Broker on GCE VM (vm-external-kafka-broker)..."
    bash "$(dirname "$0")/provision_external_kafka_vm.sh"
fi

echo "======================================================================"
echo "GCP Infrastructure Provisioning Completed Successfully!"
echo "Project ID:          ${PROJECT_ID}"
echo "Destination Bucket:  gs://${BUCKET_NAME}"
echo "Spark Runtime SA:    ${SA_EMAIL}"
echo "Dataproc Subnet:     projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}"
echo "GMK Subnet:          projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${GMK_SUBNET_NAME}"
echo "BigQuery Dataset:    ${PROJECT_ID}:${BRONZE_DATASET}"
echo "BigLake Connection:  ${PROJECT_ID}.${REGION}.${BIGLAKE_CONNECTION_ID}"
echo "======================================================================"

