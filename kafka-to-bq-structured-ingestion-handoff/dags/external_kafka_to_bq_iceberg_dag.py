"""
Cloud Composer 3 (Apache Airflow) DAG: External Kafka (GCE VM / On-Premises) Path via PAM
=========================================================================================
Dedicated exclusively to the **External Kafka Path** (Self-Managed Apache Kafka on GCE VM
`10.20.0.50:9093` or On-Premises Fab Kafka) using PAM credentials retrieved at runtime from
Google Cloud Secret Manager (`kafka-pam-credentials`) with `SASL_SSL` / `SASL_PLAINTEXT`
and `SCRAM-SHA-512`.

Provides a single interactive, parameterized DAG (`schedule=None`, manual trigger via Airflow UI or CLI):
- `external_kafka_to_bq_iceberg_ingestion`:
  Reads from a specified External Kafka topic (`JSON`, `AVRO_BINARY`, `AVRO_SCHEMA_REGISTRY`, or
  `SCHEMA_MAPPED_STRUCT`), authenticates via PAM Secret Manager credentials, applies wildcard and
  exact-match exclusion filters, writes valid records to the BigQuery Bronze Apache Iceberg table
  (`fab_telemetry_events_bronze`), and routes malformed/poison-pill records to the BigQuery
  Dead-Letter Queue table (`fab_telemetry_events_dlq`).
"""

from __future__ import annotations

import datetime
import os
from typing import Dict, List, Optional

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


def _load_job_config_defaults() -> Dict[str, str]:
    """Loads environment defaults from config/job_config.yaml if present."""
    defaults: Dict[str, str] = {}
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.environ.get("JOB_CONFIG_PATH", ""),
        os.path.join(dag_dir, "config", "job_config.yaml"),
        os.path.join(dag_dir, "job_config.yaml"),
        os.path.join(dag_dir, "..", "config", "job_config.yaml"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    for raw_line in fh:
                        line = raw_line.split("#", 1)[0].strip()
                        if ":" in line:
                            k, v = line.split(":", 1)
                            v_clean = v.strip().strip('"').strip("'")
                            if v_clean:
                                defaults[k.strip()] = v_clean
                break
            except Exception:
                pass
    return defaults


_CFG = _load_job_config_defaults()
_CFG_PROJECT_ID = (
    _CFG.get("project_id")
    or os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or ""
)
_CFG_REGION = _CFG.get("region", "us-central1")
_CFG_SUBNET_NAME = _CFG.get("subnet_name", "snet-dataproc-usc1")
_CFG_SA = _CFG.get(
    "service_account",
    f"sa-spark-kafka-bq@{_CFG_PROJECT_ID}.iam.gserviceaccount.com",
)
_CFG_BUCKET = _CFG.get(
    "output_bucket",
    f"{_CFG_PROJECT_ID}-kafka-landing-zone",
)
_CFG_DATASET = _CFG.get("output_dataset", "bq_bronze_fab_telemetry_poc")
_CFG_BRONZE_TABLE = _CFG.get("output_table", "fab_telemetry_events_bronze")
_CFG_DLQ_TABLE = _CFG.get("dlq_table", "fab_telemetry_events_dlq")
_CFG_AVRO_SCHEMA_URI = _CFG.get(
    "avro_schema_path",
    f"gs://{_CFG_BUCKET}/schemas/fab_telemetry_event.avsc",
)
_CFG_SCHEMA_MAPPING_URI = _CFG.get(
    "schema_mapping_uri",
    f"gs://{_CFG_BUCKET}/schemas/fab_schema_mapping.json",
)
_CFG_SCHEMA_REGISTRY_URL = _CFG.get(
    "schema_registry_url",
    f"https://managedkafka.googleapis.com/v1/projects/{_CFG_PROJECT_ID}/locations/{_CFG_REGION}/schemaRegistries/mfg-telemetry-sr-poc",
)

DEFAULT_PROJECT_ID = f"{{{{ var.value.get('gcp_project_id', '{_CFG_PROJECT_ID}') }}}}"
DEFAULT_REGION = f"{{{{ var.value.get('gcp_region', '{_CFG_REGION}') }}}}"
DEFAULT_SUBNET = (
    f"projects/{{{{ var.value.get('gcp_project_id', '{_CFG_PROJECT_ID}') }}}}"
    f"/regions/{{{{ var.value.get('gcp_region', '{_CFG_REGION}') }}}}"
    f"/subnetworks/{{{{ var.value.get('dataproc_subnet', '{_CFG_SUBNET_NAME}') }}}}"
)
DEFAULT_SERVICE_ACCOUNT = (
    f"{{{{ var.value.get('dataproc_service_account', '{_CFG_SA}') }}}}"
)

EXTERNAL_VM_BOOTSTRAP_DEFAULT = _CFG.get(
    "external_kafka_bootstrap_servers",
    "10.20.0.50:9093",
)

EXTERNAL_BOOTSTRAP_SERVERS = (
    "{{ dag_run.conf.get('external_kafka_bootstrap_servers', "
    "var.value.get('external_kafka_bootstrap_servers', "
    f"'{EXTERNAL_VM_BOOTSTRAP_DEFAULT}')) }}}}"
)
KAFKA_TOPIC = (
    "{{ dag_run.conf.get('kafka_topic', "
    "var.value.get('external_kafka_topic', 'fab.equipment.telemetry.json.v1')) }}"
)
INPUT_FORMAT = (
    "{{ dag_run.conf.get('input_format', "
    "var.value.get('external_kafka_input_format', 'JSON')) }}"
)
SECRET_NAME = (
    "{{ dag_run.conf.get('secret_name', "
    "var.value.get('kafka_secret_name', 'kafka-pam-credentials')) }}"
)
EXCLUDE_PATTERNS = (
    "{{ dag_run.conf.get('exclude_patterns', "
    "var.value.get('kafka_exclude_patterns', '*.tmp,*.bak,~$*,*.log,*_debug_*,*heartbeat*')) }}"
)
EXCLUDE_FILES = (
    "{{ dag_run.conf.get('exclude_files', "
    "var.value.get('kafka_exclude_files', 'desktop.ini,thumbs.db,.DS_Store,TEST_TOOL_IGNORE')) }}"
)
BATCH_MODE = (
    "{{ dag_run.conf.get('batch_mode', "
    "var.value.get('kafka_batch_mode', 'available_now')) }}"
)
BATCH_SIZE = (
    "{{ dag_run.conf.get('batch_size', "
    "var.value.get('kafka_batch_size', '500000')) }}"
)
BATCH_INTERVAL_MINUTES = (
    "{{ dag_run.conf.get('batch_interval_minutes', "
    "var.value.get('kafka_batch_interval_minutes', '5')) }}"
)
STARTING_OFFSETS = (
    "{{ dag_run.conf.get('starting_offsets', "
    "var.value.get('kafka_starting_offsets', 'earliest')) }}"
)
CONSUMER_GROUP_ID = (
    "{{ dag_run.conf.get('consumer_group_id', "
    "var.value.get('external_kafka_consumer_group_id', 'cg-spark-extkafka-bq-bronze')) }}"
)
OUTPUT_FORMAT = (
    "{{ dag_run.conf.get('output_format', "
    "var.value.get('kafka_output_format', 'ICEBERG')) }}"
)
OUTPUT_DATASET = (
    "{{ dag_run.conf.get('output_dataset', "
    f"var.value.get('kafka_output_dataset', '{_CFG_DATASET}')) }}}}"
)
OUTPUT_TABLE = (
    "{{ dag_run.conf.get('output_table', "
    f"var.value.get('kafka_output_table', '{_CFG_BRONZE_TABLE}')) }}}}"
)
DLQ_TABLE = (
    "{{ dag_run.conf.get('dlq_table', "
    f"var.value.get('kafka_dlq_table', '{_CFG_DLQ_TABLE}')) }}}}"
)
OUTPUT_BUCKET = (
    "{{ dag_run.conf.get('output_bucket', "
    f"var.value.get('kafka_output_bucket', '{_CFG_BUCKET}')) }}}}"
)
AVRO_SCHEMA_PATH = (
    "{{ dag_run.conf.get('avro_schema_path', "
    "var.value.get('kafka_avro_schema_path', "
    f"'{_CFG_AVRO_SCHEMA_URI}')) }}}}"
)
SCHEMA_MAPPING_URI = (
    "{{ dag_run.conf.get('schema_mapping_uri', "
    "var.value.get('kafka_schema_mapping_uri', "
    f"'{_CFG_SCHEMA_MAPPING_URI}')) }}}}"
)
SCHEMA_REGISTRY_URL = (
    "{{ dag_run.conf.get('schema_registry_url', "
    "var.value.get('kafka_schema_registry_url', "
    f"'{_CFG_SCHEMA_REGISTRY_URL}')) }}}}"
)

EXTERNAL_KAFKA_SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
    "org.apache.spark:spark-avro_2.13:3.5.1"
)

default_args = {
    "owner": "enterprise-data-ingestion-external-kafka",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=30),
}


def _build_external_kafka_dataproc_batch_config(
    output_bucket: str,
    args: List[str],
    extra_python_files: Optional[List[str]] = None,
) -> Dict[str, object]:
    """Builds the Dataproc Serverless Spark 2.2 batch configuration for the External Kafka (PAM) path."""
    py_files = [
        f"gs://{output_bucket}/scripts/schema_and_dlq_handler.py",
        f"gs://{output_bucket}/scripts/external_kafka_connector.py",
        f"gs://{output_bucket}/scripts/gmk_connector.py",
    ]
    if extra_python_files:
        py_files.extend(extra_python_files)
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{output_bucket}/scripts/kafka_bq_ingestion.py",
            "python_file_uris": py_files,
            "args": args,
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "12",
                "spark.sql.shuffle.partitions": "8",
                "spark.jars.packages": EXTERNAL_KAFKA_SPARK_PACKAGES,
            },
        },
        "environment_config": {
            "execution_config": {
                "service_account": DEFAULT_SERVICE_ACCOUNT,
                "subnetwork_uri": DEFAULT_SUBNET,
            }
        },
    }


with DAG(
    dag_id="external_kafka_to_bq_iceberg_ingestion",
    description=(
        "External Kafka on GCE VM (10.20.0.50:9093) via PAM Secret Manager (SCRAM-SHA-512) "
        "to BigQuery Bronze Apache Iceberg + DLQ Ingestion Pipeline"
    ),
    default_args=default_args,
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=[
        "external-kafka",
        "gce-vm-kafka",
        "pam-secret-manager",
        "scram-sha-512",
        "bigquery",
        "iceberg",
        "dlq",
    ],
    params={
        "external_kafka_bootstrap_servers": Param(
            default=EXTERNAL_VM_BOOTSTRAP_DEFAULT,
            type="string",
            description="External Kafka bootstrap servers (e.g., GCE VM '10.20.0.50:9093' or On-Premises brokers)",
        ),
        "kafka_topic": Param(
            default="fab.equipment.telemetry.json.v1",
            type="string",
            description=(
                "Source External Kafka topic name. Pre-seeded topics by format: "
                "fab.equipment.telemetry.json.v1 (JSON), "
                "fab.equipment.telemetry.avro.binary.v1 (AVRO_BINARY), "
                "fab.equipment.telemetry.avro.sr.v1 (AVRO_SCHEMA_REGISTRY), "
                "fab.cleanroom.metrology.struct.v1 (SCHEMA_MAPPED_STRUCT)"
            ),
        ),
        "secret_name": Param(
            default="kafka-pam-credentials",
            type="string",
            description=(
                "GCP Secret Manager PAM credential secret name containing "
                "username, password, sasl_mechanism (SCRAM-SHA-512), and security_protocol"
            ),
        ),
        "input_format": Param(
            default="JSON",
            enum=[
                "JSON",
                "AVRO_BINARY",
                "AVRO_SCHEMA_REGISTRY",
                "SCHEMA_MAPPED_STRUCT",
            ],
            description="Input payload format (JSON, AVRO_BINARY, AVRO_SCHEMA_REGISTRY, SCHEMA_MAPPED_STRUCT)",
        ),
        "exclude_patterns": Param(
            default="*.tmp,*.bak,~$*,*.log,*_debug_*,*heartbeat*",
            type="string",
            description="Exclude file list (wildcard/glob patterns): comma-separated patterns dropped before ingestion",
        ),
        "exclude_files": Param(
            default="desktop.ini,thumbs.db,.DS_Store,TEST_TOOL_IGNORE",
            type="string",
            description="Exclude file list (exact match): comma-separated exact file, key, or equipment names to exclude",
        ),
        "batch_mode": Param(
            default="available_now",
            enum=["available_now", "processing_time"],
            description="Batch option: 'available_now' (bounded catch-up batch) or 'processing_time' (timed micro-batches)",
        ),
        "batch_size": Param(
            default=500000,
            type="integer",
            description="Batch option (size): Maximum Kafka offsets processed per micro-batch wave (maxOffsetsPerTrigger)",
        ),
        "batch_interval_minutes": Param(
            default=5,
            type="integer",
            description="Batch option (minutes): Micro-batch trigger interval in minutes when batch_mode='processing_time'",
        ),
        "starting_offsets": Param(
            default="earliest",
            enum=["earliest", "latest"],
            description="Optional parameter: Kafka starting offset strategy when no prior checkpoint exists",
        ),
        "consumer_group_id": Param(
            default="cg-spark-extkafka-bq-bronze",
            type="string",
            description="Optional parameter: External Kafka consumer group ID prefix",
        ),
        "avro_schema_path": Param(
            default=_CFG_AVRO_SCHEMA_URI,
            type="string",
            description="Optional parameter: GCS URI to .avsc schema file for AVRO_BINARY",
        ),
        "schema_mapping_uri": Param(
            default=_CFG_SCHEMA_MAPPING_URI,
            type="string",
            description="Optional parameter: GCS URI to declarative JSON schema mapping spec for SCHEMA_MAPPED_STRUCT",
        ),
        "schema_registry_url": Param(
            default=_CFG_SCHEMA_REGISTRY_URL,
            type="string",
            description="Optional parameter: Confluent or Managed Schema Registry URL for AVRO_SCHEMA_REGISTRY",
        ),
        "output_format": Param(
            default="ICEBERG",
            enum=["ICEBERG", "NATIVE_BQ"],
            description="Output parameter: Apache Iceberg table format ('ICEBERG') on Bronze layer",
        ),
        "output_dataset": Param(
            default=_CFG_DATASET,
            type="string",
            description="Output parameter: Destination BigQuery Bronze dataset ID",
        ),
        "output_table": Param(
            default=_CFG_BRONZE_TABLE,
            type="string",
            description="Output parameter: Destination BigQuery Bronze Apache Iceberg table name",
        ),
        "dlq_table": Param(
            default=_CFG_DLQ_TABLE,
            type="string",
            description="Output parameter: Destination BigQuery Dead-Letter Queue (DLQ) table name",
        ),
        "output_bucket": Param(
            default=_CFG_BUCKET,
            type="string",
            description="Output parameter: Destination GCS bucket for Iceberg warehouse and Spark checkpoints",
        ),
    },
) as external_kafka_ingestion_dag:

    run_external_kafka_to_bq_ingestion = DataprocCreateBatchOperator(
        task_id="run_external_kafka_to_bq_pyspark_ingestion",
        project_id=DEFAULT_PROJECT_ID,
        region=DEFAULT_REGION,
        batch_id="dp-extkafkatobq-{{ ts_nodash | lower }}-{{ ti.try_number }}",
        deferrable=True,
        polling_interval_seconds=15,
        batch=_build_external_kafka_dataproc_batch_config(
            output_bucket=OUTPUT_BUCKET,
            args=[
                f"--project-id={DEFAULT_PROJECT_ID}",
                f"--region={DEFAULT_REGION}",
                f"--kafka-bootstrap-servers={EXTERNAL_BOOTSTRAP_SERVERS}",
                f"--kafka-topic={KAFKA_TOPIC}",
                "--kafka-source-type=EXTERNAL",
                f"--secret-name={SECRET_NAME}",
                f"--input-format={INPUT_FORMAT}",
                f"--avro-schema-path={AVRO_SCHEMA_PATH}",
                f"--schema-mapping-uri={SCHEMA_MAPPING_URI}",
                f"--schema-registry-url={SCHEMA_REGISTRY_URL}",
                f"--exclude-patterns={EXCLUDE_PATTERNS}",
                f"--exclude-files={EXCLUDE_FILES}",
                f"--batch-mode={BATCH_MODE}",
                f"--batch-size={BATCH_SIZE}",
                f"--batch-interval-minutes={BATCH_INTERVAL_MINUTES}",
                f"--starting-offsets={STARTING_OFFSETS}",
                f"--consumer-group-id={CONSUMER_GROUP_ID}",
                f"--output-format={OUTPUT_FORMAT}",
                f"--output-dataset={OUTPUT_DATASET}",
                f"--output-table={OUTPUT_TABLE}",
                f"--dlq-table={DLQ_TABLE}",
                f"--output-bucket={OUTPUT_BUCKET}",
                "--write-method=direct",
            ],
        ),
    )

    verify_external_kafka_bronze_iceberg = BigQueryInsertJobOperator(
        task_id="verify_external_kafka_bronze_iceberg_and_dlq",
        project_id=DEFAULT_PROJECT_ID,
        location=_CFG_REGION,
        configuration={
            "query": {
                "query": (
                    f"SELECT "
                    f"  (SELECT COUNT(*) FROM `{DEFAULT_PROJECT_ID}.{OUTPUT_DATASET}.{OUTPUT_TABLE}`) AS bronze_iceberg_rows, "
                    f"  (SELECT COUNT(*) FROM `{DEFAULT_PROJECT_ID}.{OUTPUT_DATASET}.vw_fab_telemetry_events_curated`) AS curated_dedup_rows, "
                    f"  (SELECT COUNT(*) FROM `{DEFAULT_PROJECT_ID}.{OUTPUT_DATASET}.{DLQ_TABLE}` WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)) AS dlq_quarantined_rows"
                ),
                "useLegacySql": False,
            }
        },
    )

    run_external_kafka_to_bq_ingestion >> verify_external_kafka_bronze_iceberg
