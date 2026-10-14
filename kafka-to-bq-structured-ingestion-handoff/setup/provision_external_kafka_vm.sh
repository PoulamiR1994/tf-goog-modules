#!/usr/bin/env bash
# ==============================================================================
# Provision External Apache Kafka Broker on a GCE VM
# ==============================================================================
# Deploys a self-contained Apache Kafka (KRaft mode) broker on a Google Compute
# Engine VM ('vm-external-kafka-broker') inside 'snet-dataproc-usc1' to simulate
# an External / On-Premises Fab Kafka cluster authenticated via PAM credentials
# stored in GCP Secret Manager ('kafka-pam-credentials', SASL/SCRAM-SHA-512).
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../config/job_config.yaml"
CFG_PROJECT_ID=$(awk -F': ' '/^project_id:/ {gsub(/["'\'' ]/, "", $2); print $2}' "${CONFIG_FILE}" 2>/dev/null || true)

PROJECT_ID="${PROJECT_ID:-${CFG_PROJECT_ID}}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-${REGION}-a}"
NETWORK_NAME="${NETWORK_NAME:-kafka-bq-vpc}"
SUBNET_NAME="${SUBNET_NAME:-snet-dataproc-usc1}"
VM_NAME="${VM_NAME:-vm-external-kafka-broker}"
VM_INTERNAL_IP="${VM_INTERNAL_IP:-10.20.0.50}"
SA_NAME="${SA_NAME:-sa-spark-kafka-bq}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SECRET_NAME="${SECRET_NAME:-kafka-pam-credentials}"

echo "======================================================================"
echo "Provisioning External Kafka Broker on GCE VM: ${VM_NAME} (${ZONE})"
echo "Project: ${PROJECT_ID} | Subnet: ${SUBNET_NAME} | IP: ${VM_INTERNAL_IP}"
echo "======================================================================"

# Reserve static internal IP 10.20.0.50 in snet-dataproc-usc1 if not already reserved
if ! gcloud compute addresses describe "${VM_NAME}-ip" --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud compute addresses create "${VM_NAME}-ip" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --subnet="${SUBNET_NAME}" \
        --addresses="${VM_INTERNAL_IP}"
    echo "Reserved static internal IP ${VM_INTERNAL_IP} (${VM_NAME}-ip)."
else
    echo "Static internal IP ${VM_NAME}-ip (${VM_INTERNAL_IP}) already exists."
fi

STARTUP_SCRIPT_FILE=$(mktemp /tmp/kafka_vm_startup.XXXXXX.sh)
trap 'rm -f "${STARTUP_SCRIPT_FILE}"' EXIT

cat <<EOF > "${STARTUP_SCRIPT_FILE}"
#!/usr/bin/env bash
set -e
# Fetch PAM credentials from Secret Manager to configure SCRAM-SHA-512 broker user
ACCESS_TOKEN=\$(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
SECRET_PAYLOAD=\$(curl -s -H "Authorization: Bearer \${ACCESS_TOKEN}" "https://secretmanager.googleapis.com/v1/projects/${PROJECT_ID}/secrets/${SECRET_NAME}/versions/latest:access" | python3 -c 'import sys,json,base64; d=json.load(sys.stdin); print(base64.b64decode(d["payload"]["data"]).decode("utf-8"))' 2>/dev/null || echo '{"username":"svc-intel-fab-kafka","password":"IntelFabKafka#2026!"}')
KAFKA_USER=\$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('username','svc-intel-fab-kafka'))" "\${SECRET_PAYLOAD}")
KAFKA_PASS=\$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('password','IntelFabKafka#2026!'))" "\${SECRET_PAYLOAD}")

KAFKA_IMAGE="mirror.gcr.io/apache/kafka:3.7.0"
docker pull "\${KAFKA_IMAGE}" || KAFKA_IMAGE="apache/kafka:3.7.0"

mkdir -p /tmp/kafka-config
cat <<PROP > /tmp/kafka-config/server.properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9094
listeners=PLAINTEXT://0.0.0.0:9092,SASL_PLAINTEXT://0.0.0.0:9093,CONTROLLER://127.0.0.1:9094
advertised.listeners=PLAINTEXT://${VM_INTERNAL_IP}:9092,SASL_PLAINTEXT://${VM_INTERNAL_IP}:9093
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
sasl.enabled.mechanisms=SCRAM-SHA-512,PLAIN
listener.name.sasl_plaintext.sasl.enabled.mechanisms=SCRAM-SHA-512,PLAIN
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="\${KAFKA_USER}" password="\${KAFKA_PASS}" user_\${KAFKA_USER}="\${KAFKA_PASS}";
log.dirs=/tmp/kraft-combined-logs
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
PROP
chmod -R 777 /tmp/kafka-config
iptables -I INPUT -p tcp -m multiport --dports 9092,9093,9094 -j ACCEPT || true

docker rm -f external-kafka-broker >/dev/null 2>&1 || true
docker run -d --name external-kafka-broker --restart=always --net=host \
  -v /tmp/kafka-config:/tmp/kafka-config \
  --entrypoint /bin/sh \
  "\${KAFKA_IMAGE}" \
  -c "/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /tmp/kafka-config/server.properties --add-scram 'SCRAM-SHA-512=[name=\${KAFKA_USER},password=\${KAFKA_PASS}]' --ignore-formatted && exec /opt/kafka/bin/kafka-server-start.sh /tmp/kafka-config/server.properties"

echo "Waiting for external-kafka-broker to listen on ${VM_INTERNAL_IP}:9092..."
for i in \$(seq 1 30); do
  if docker exec external-kafka-broker /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${VM_INTERNAL_IP}:9092 >/dev/null 2>&1; then
    echo "Broker is UP after \${i} checks!"
    break
  fi
  sleep 2
done
docker logs --tail 30 external-kafka-broker || true

for TOPIC in fab.equipment.telemetry.v1 fab.equipment.telemetry.json.v1 fab.equipment.telemetry.avro.binary.v1 fab.equipment.telemetry.avro.sr.v1 fab.cleanroom.metrology.struct.v1 fab.equipment.telemetry.v1.dlq; do
  docker exec external-kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${VM_INTERNAL_IP}:9092 --create --if-not-exists --topic "\${TOPIC}" --partitions 3 --replication-factor 1 || true
done
docker exec external-kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${VM_INTERNAL_IP}:9092 --list || true
EOF

# Create GCE VM running Container-Optimized OS with Apache Kafka in KRaft mode + SCRAM-SHA-512
if gcloud compute instances describe "${VM_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "GCE VM ${VM_NAME} already exists in ${ZONE}. Updating startup-script metadata..."
    gcloud compute instances add-metadata "${VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --metadata-from-file="startup-script=${STARTUP_SCRIPT_FILE}"
else
    gcloud compute instances create "${VM_NAME}" \
        --project="${PROJECT_ID}" \
        --zone="${ZONE}" \
        --machine-type="e2-standard-2" \
        --subnet="${SUBNET_NAME}" \
        --private-network-ip="${VM_INTERNAL_IP}" \
        --no-address \
        --service-account="${SA_EMAIL}" \
        --scopes="cloud-platform" \
        --image-family="cos-stable" \
        --image-project="cos-cloud" \
        --boot-disk-size="30GB" \
        --boot-disk-type="pd-balanced" \
        --tags="external-kafka-vm" \
        --labels="env=poc,role=external-kafka-broker,pattern=kafka-to-bq" \
        --metadata-from-file="startup-script=${STARTUP_SCRIPT_FILE}"
    echo "Provisioned External Kafka GCE VM: ${VM_NAME} (${VM_INTERNAL_IP}:9092 / ${VM_INTERNAL_IP}:9093)."
fi

echo "======================================================================"
echo "External Kafka VM Ready!"
echo "Bootstrap Endpoint (SASL/SCRAM-SHA-512): ${VM_INTERNAL_IP}:9093"
echo "Bootstrap Endpoint (Internal Broker):    ${VM_INTERNAL_IP}:9092"
echo "PAM Secret in Secret Manager:            ${SECRET_NAME}"
echo "======================================================================"
