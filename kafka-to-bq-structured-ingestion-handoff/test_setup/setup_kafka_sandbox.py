#!/usr/bin/env python3
"""
Kafka Sandbox & Schema Provisioner.

Prepares the testing environment for the Kafka-to-BigQuery Iceberg Ingestion Pipeline:
1. Exports the canonical Intel Fab Telemetry Avro schema (.avsc) locally and uploads
   it to gs://<output_bucket>/schemas/fab_telemetry_event.avsc.
2. Optionally creates the 4 test Kafka topics (JSON, AVRO_BINARY, AVRO_SCHEMA_REGISTRY,
   SCHEMA_MAPPED_STRUCT) and Dead-Letter Queue (DLQ) topic on Google Cloud Managed
   Service for Apache Kafka (GMK) or an External Kafka cluster.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from typing import Dict, List

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
_SRC_ROOT = os.path.join(_REPO_ROOT, "src")
for _p in (_CURRENT_DIR, _REPO_ROOT, _SRC_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from test_setup.generate_enterprise_test_data import (  # noqa: E402
        export_test_batches_to_dir,
        generate_avro_binary_test_messages,
        generate_avro_schema_registry_test_messages,
        generate_json_test_messages,
        generate_schema_mapped_struct_test_messages,
    )
except ImportError:
    from generate_enterprise_test_data import (  # type: ignore[no-redef] # noqa: E402
        export_test_batches_to_dir,
        generate_avro_binary_test_messages,
        generate_avro_schema_registry_test_messages,
        generate_json_test_messages,
        generate_schema_mapped_struct_test_messages,
    )

try:
    from utils.schema_and_dlq_handler import (  # noqa: E402
        DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
        DEFAULT_SCHEMA_MAPPING_SPEC,
    )
except ImportError:
    from schema_and_dlq_handler import (  # type: ignore[no-redef] # noqa: E402
        DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
        DEFAULT_SCHEMA_MAPPING_SPEC,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("setup_kafka_sandbox")


def _load_job_config_defaults() -> Dict[str, str]:
    """Loads environment defaults from config/job_config.yaml if present."""
    defaults: Dict[str, str] = {}
    cfg_path = os.environ.get(
        "JOB_CONFIG_PATH", os.path.join(_REPO_ROOT, "config", "job_config.yaml")
    )
    if os.path.isfile(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.split("#", 1)[0].strip()
                if ":" in line:
                    k, v = line.split(":", 1)
                    v_clean = v.strip().strip('"').strip("'")
                    if v_clean:
                        defaults[k.strip()] = v_clean
    return defaults


_CFG = _load_job_config_defaults()
DEFAULT_PROJECT_ID = (
    os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or _CFG.get("project_id", "")
)
DEFAULT_REGION = _CFG.get("region", "us-central1")
DEFAULT_BUCKET = _CFG.get(
    "output_bucket",
    f"{DEFAULT_PROJECT_ID}-kafka-landing-zone" if DEFAULT_PROJECT_ID else "",
)
DEFAULT_GMK_CLUSTER = "sample-kafka-target-cluster"
DEFAULT_EXTERNAL_VM_BOOTSTRAP = _CFG.get(
    "external_kafka_bootstrap_servers", "10.20.0.50:9093"
)

SANDBOX_TOPICS: List[str] = [
    "fab.equipment.telemetry.v1",
    "fab.equipment.telemetry.json.v1",
    "fab.equipment.telemetry.avro.binary.v1",
    "fab.equipment.telemetry.avro.sr.v1",
    "fab.cleanroom.metrology.struct.v1",
    "fab.equipment.telemetry.v1.dlq",
]


def stage_avro_schema(
    project_id: str,
    bucket_name: str,
    upload_to_gcs: bool = True,
) -> str:
    """Writes the Fab Telemetry .avsc schema, schema mapping spec, and 4 format test batches locally and uploads to GCS."""
    local_dir = os.path.join(_CURRENT_DIR, "schemas")
    os.makedirs(local_dir, exist_ok=True)
    local_avsc_path = os.path.join(local_dir, "fab_telemetry_event.avsc")
    local_map_path = os.path.join(local_dir, "fab_schema_mapping.json")

    schema_json = json.dumps(DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA, indent=2)
    with open(local_avsc_path, "w", encoding="utf-8") as f:
        f.write(schema_json)
    logger.info("Wrote local Avro schema file: %s", local_avsc_path)

    mapping_json = json.dumps(DEFAULT_SCHEMA_MAPPING_SPEC, indent=2)
    with open(local_map_path, "w", encoding="utf-8") as f:
        f.write(mapping_json)
    logger.info("Wrote local Schema Mapping spec file: %s", local_map_path)

    batches_dir = os.path.join(_CURRENT_DIR, "generated_batches")
    batch_files = export_test_batches_to_dir(batches_dir)

    if upload_to_gcs:
        try:
            from google.cloud import storage

            client = storage.Client(project=project_id)
            bucket = client.bucket(bucket_name)

            avsc_blob = bucket.blob("schemas/fab_telemetry_event.avsc")
            avsc_blob.upload_from_string(schema_json, content_type="application/json")
            logger.info(
                "Uploaded Avro schema to gs://%s/schemas/fab_telemetry_event.avsc",
                bucket_name,
            )

            map_blob = bucket.blob("schemas/fab_schema_mapping.json")
            map_blob.upload_from_string(mapping_json, content_type="application/json")
            logger.info(
                "Uploaded Schema Mapping spec to gs://%s/schemas/fab_schema_mapping.json",
                bucket_name,
            )

            for fmt_name, local_batch_file in batch_files.items():
                blob_name = f"test_batches/{os.path.basename(local_batch_file)}"
                bucket.blob(blob_name).upload_from_filename(
                    local_batch_file, content_type="application/json"
                )
                logger.info(
                    "Uploaded %s test batch to gs://%s/%s",
                    fmt_name,
                    bucket_name,
                    blob_name,
                )
        except Exception as gcs_err:
            logger.info(
                "Python google.cloud.storage unavailable (%s); uploading via gcloud storage cp...",
                gcs_err,
            )
            try:
                subprocess.run(
                    [
                        "gcloud",
                        "storage",
                        "cp",
                        local_avsc_path,
                        local_map_path,
                        f"gs://{bucket_name}/schemas/",
                        f"--project={project_id}",
                    ],
                    check=True,
                )
                for local_batch_file in batch_files.values():
                    subprocess.run(
                        [
                            "gcloud",
                            "storage",
                            "cp",
                            local_batch_file,
                            f"gs://{bucket_name}/test_batches/",
                            f"--project={project_id}",
                        ],
                        check=True,
                    )
                logger.info(
                    "Uploaded schemas and 4 format test batches to gs://%s/ via gcloud storage cp.",
                    bucket_name,
                )
            except Exception as cli_err:
                logger.warning(
                    "Skipping GCS upload (%s); local schemas at %s and batches at %s are ready.",
                    cli_err,
                    local_dir,
                    batches_dir,
                )

    return local_avsc_path


def create_gmk_topics(
    project_id: str,
    region: str,
    cluster_id: str,
    partitions: int = 6,
    replication_factor: int = 3,
) -> None:
    """Creates the sandbox Kafka topics in Google Cloud Managed Service for Apache Kafka (GMK)."""
    for topic in SANDBOX_TOPICS:
        cmd = [
            "gcloud",
            "managed-kafka",
            "topics",
            "create",
            topic,
            f"--cluster={cluster_id}",
            f"--location={region}",
            f"--project={project_id}",
            f"--partitions={partitions}",
            f"--replication-factor={replication_factor}",
        ]
        logger.info("Creating GMK topic '%s' on cluster '%s'...", topic, cluster_id)
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("Created GMK topic: %s", topic)
        except subprocess.CalledProcessError as ex:
            if "ALREADY_EXISTS" in (ex.stderr or ""):
                logger.info("GMK topic '%s' already exists.", topic)
            else:
                logger.warning("Could not create GMK topic '%s': %s", topic, ex.stderr)


def publish_test_batches_to_kafka(
    bootstrap_servers: str,
    source_type: str = "EXTERNAL",
    project_id: str = DEFAULT_PROJECT_ID,
    secret_name: str = "kafka-pam-credentials",
) -> None:
    """Publishes the 4 input format test batches (valid, duplicate, excluded, DLQ) to a live GMK or External Kafka VM broker."""
    if not bootstrap_servers:
        return
    topic_batches = {
        "fab.equipment.telemetry.v1": generate_json_test_messages(),
        "fab.equipment.telemetry.json.v1": generate_json_test_messages(),
        "fab.equipment.telemetry.avro.binary.v1": generate_avro_binary_test_messages(),
        "fab.equipment.telemetry.avro.sr.v1": generate_avro_schema_registry_test_messages(),
        "fab.cleanroom.metrology.struct.v1": generate_schema_mapped_struct_test_messages(),
    }

    # First try Spark native Kafka writer (available on Dataproc Serverless with spark-sql-kafka + managed-kafka-auth-login-handler)
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import BinaryType, StringType, StructField, StructType

        try:
            from jobs.kafka_bq_ingestion import (
                KafkaSparkConfigBuilder,
                resolve_pam_credentials,
            )
        except ImportError:
            from kafka_bq_ingestion import (  # type: ignore[no-redef]
                KafkaSparkConfigBuilder,
                resolve_pam_credentials,
            )

        spark = SparkSession.builder.appName("KafkaSandboxTopicSeeder").getOrCreate()
        pam = resolve_pam_credentials(
            project_id=project_id,
            secret_name=secret_name,
            kafka_source_type=source_type.upper(),
        )
        reader_opts = KafkaSparkConfigBuilder.build_reader_options(
            bootstrap_servers=bootstrap_servers,
            topic="fab.equipment.telemetry.json.v1",
            source_type=source_type.upper(),
            pam_creds=pam,
        )
        writer_opts = {
            k: v
            for k, v in reader_opts.items()
            if k.startswith("kafka.")
        }

        rows = []
        for topic_name, records in topic_batches.items():
            for rec in records:
                raw_key = rec.get("key")
                key_bytes = (
                    raw_key.encode("utf-8")
                    if isinstance(raw_key, str)
                    else (raw_key or b"")
                )
                if rec.get("value_is_hex"):
                    val_bytes = bytes.fromhex(rec["value"])
                elif isinstance(rec.get("value"), str):
                    val_bytes = rec["value"].encode("utf-8")
                else:
                    val_bytes = rec.get("value") or b""
                rows.append((topic_name, key_bytes, val_bytes))

        schema = StructType(
            [
                StructField("topic", StringType(), False),
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), True),
            ]
        )
        df = spark.createDataFrame(rows, schema=schema)
        df.write.format("kafka").options(**writer_opts).save()
        logger.info(
            "Successfully published %d total messages across 4 topics to %s (%s) via Spark Kafka writer.",
            len(rows),
            bootstrap_servers,
            source_type,
        )
        return
    except Exception as spark_pub_err:
        logger.info(
            "Spark Kafka writer not active in this runtime (%s); trying kafka-python...",
            spark_pub_err,
        )

    try:
        from kafka import KafkaProducer  # type: ignore

        producer_kwargs = {"bootstrap_servers": bootstrap_servers.split(",")}
        if source_type.upper() == "EXTERNAL" and ":9093" in bootstrap_servers:
            try:
                from jobs.kafka_bq_ingestion import resolve_pam_credentials
            except ImportError:
                from kafka_bq_ingestion import resolve_pam_credentials  # type: ignore[no-redef]

            pam = resolve_pam_credentials(
                project_id=project_id,
                secret_name=secret_name,
                kafka_source_type="EXTERNAL",
            )
            producer_kwargs.update(
                {
                    "security_protocol": pam.get("security_protocol", "SASL_PLAINTEXT"),
                    "sasl_mechanism": pam.get("sasl_mechanism", "SCRAM-SHA-512"),
                    "sasl_plain_username": pam.get("username", "svc-intel-fab-kafka"),
                    "sasl_plain_password": pam.get("password", ""),
                }
            )
        producer = KafkaProducer(**producer_kwargs)
        for topic_name, records in topic_batches.items():
            for rec in records:
                raw_key = rec.get("key")
                key_bytes = (
                    raw_key.encode("utf-8")
                    if isinstance(raw_key, str)
                    else (raw_key or b"")
                )
                if rec.get("value_is_hex"):
                    val_bytes = bytes.fromhex(rec["value"])
                elif isinstance(rec.get("value"), str):
                    val_bytes = rec["value"].encode("utf-8")
                else:
                    val_bytes = rec.get("value") or b""
                producer.send(topic_name, key=key_bytes, value=val_bytes)
            logger.info(
                "Published %d test messages to %s on %s (%s)",
                len(records),
                topic_name,
                bootstrap_servers,
                source_type,
            )
        producer.flush()
        producer.close()
    except Exception as pub_err:
        logger.warning(
            "Direct Kafka broker publish skipped (%s); GCS staged batches in gs://%s/test_batches/ will be used.",
            pub_err,
            DEFAULT_BUCKET,
        )


def main() -> None:
    """CLI entrypoint for setting up the Kafka test sandbox."""
    parser = argparse.ArgumentParser(
        description="Set up Kafka sandbox topics, schemas, and test batches"
    )
    parser.add_argument(
        "--project-id", default=DEFAULT_PROJECT_ID, help="GCP Project ID"
    )
    parser.add_argument("--region", default=DEFAULT_REGION, help="GCP Region")
    parser.add_argument(
        "--output-bucket",
        "--bucket",
        dest="output_bucket",
        default=DEFAULT_BUCKET,
        help="GCS staging bucket",
    )
    parser.add_argument(
        "--gmk-cluster-id",
        default=DEFAULT_GMK_CLUSTER,
        help="GMK Cluster ID if provisioning GMK topics",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="",
        help="Optional GMK or External Kafka bootstrap servers endpoint to publish live test messages",
    )
    parser.add_argument(
        "--external-vm-bootstrap",
        default="",
        help="Optional External Kafka GCE VM bootstrap endpoint (e.g. 10.20.0.50:9093) to publish live test messages",
    )
    parser.add_argument(
        "--secret-name",
        default="kafka-pam-credentials",
        help="Secret Manager PAM secret name for External Kafka authentication",
    )
    parser.add_argument(
        "--create-gmk-topics",
        action="store_true",
        help="Create the sandbox topics on the specified GMK cluster via gcloud",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Only generate local schema and batch files without calling GCP APIs",
    )
    args = parser.parse_args()

    stage_avro_schema(
        project_id=args.project_id,
        bucket_name=args.output_bucket,
        upload_to_gcs=(not args.local_only),
    )
    if args.create_gmk_topics and not args.local_only:
        create_gmk_topics(
            project_id=args.project_id,
            region=args.region,
            cluster_id=args.gmk_cluster_id,
        )
    if args.bootstrap_servers and not args.local_only:
        src_type = (
            "GMK" if "managedkafka" in args.bootstrap_servers.lower() else "EXTERNAL"
        )
        publish_test_batches_to_kafka(
            bootstrap_servers=args.bootstrap_servers,
            source_type=src_type,
            project_id=args.project_id,
            secret_name=args.secret_name,
        )
    if args.external_vm_bootstrap and not args.local_only:
        publish_test_batches_to_kafka(
            bootstrap_servers=args.external_vm_bootstrap,
            source_type="EXTERNAL",
            project_id=args.project_id,
            secret_name=args.secret_name,
        )
    logger.info("Kafka sandbox setup completed.")


if __name__ == "__main__":
    main()

