#!/usr/bin/env python3
"""
Standalone DDL Provisioner for BigQuery-Managed Apache Iceberg Bronze Tables,
Dead-Letter Queue (DLQ) Quarantine Tables, and Curated Deduplication Views.

Strictly separates table/schema creation (DDL) from the runtime ingestion pipeline:
1. BigQuery Bronze Apache Iceberg Table:
   `<project_id>.<dataset_id>.fab_telemetry_events_bronze`
2. BigQuery Dead-Letter Queue (DLQ) Table:
   `<project_id>.<dataset_id>.fab_telemetry_events_dlq`
3. Unified Deduplication & Envelope Normalization View:
   `<project_id>.<dataset_id>.vw_fab_telemetry_events_curated`
"""

import argparse
import logging
import os
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("create_iceberg_tables")


def _load_job_config_defaults() -> Dict[str, str]:
    """Loads environment defaults from config/job_config.yaml if present."""
    defaults: Dict[str, str] = {}
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    cfg_path = os.environ.get(
        "JOB_CONFIG_PATH", os.path.join(repo_root, "config", "job_config.yaml")
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
DEFAULT_DATASET_ID = _CFG.get("output_dataset", "bq_bronze_fab_telemetry_poc")
DEFAULT_BUCKET = _CFG.get(
    "output_bucket",
    f"{DEFAULT_PROJECT_ID}-kafka-landing-zone" if DEFAULT_PROJECT_ID else "",
)
DEFAULT_CONNECTION_ID = _CFG.get("biglake_connection_id", "gcp_lakehouse_connection")


def build_bronze_iceberg_ddl(
    project_id: str,
    region: str,
    dataset_id: str,
    table_name: str,
    bucket: str,
    connection_id: str,
    kms_key_name: Optional[str] = None,
) -> str:
    """
    Builds the BigQuery-Managed Apache Iceberg Table DDL.
    BigQuery-Managed Apache Iceberg tables use CLUSTER BY and WITH CONNECTION +
    OPTIONS(table_format='ICEBERG', file_format='PARQUET', storage_uri='gs://...').
    """
    kms_opt = f',\n  kms_key_name = "{kms_key_name}"' if kms_key_name else ""
    storage_uri = f"gs://{bucket}/iceberg/bronze/{table_name}"
    return f"""
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_name}`
(
  -- 1. Business Payload Columns (All NULLABLE to prevent non-nullable rejection errors)
  event_id STRING OPTIONS(description="Unique producer event UUID"),
  equipment_id STRING OPTIONS(description="Fab cleanroom tool / equipment identifier"),
  fab_site_code STRING OPTIONS(description="Intel Fab facility site code (e.g., D1X, Ocotillo, Leixlip)"),
  wafer_lot_id STRING OPTIONS(description="Silicon wafer lot identifier"),
  metric_name STRING OPTIONS(description="Sensor or metrology parameter name"),
  metric_value FLOAT64 OPTIONS(description="Measured telemetry value"),
  status_code STRING OPTIONS(description="Equipment operational status code"),
  event_timestamp TIMESTAMP OPTIONS(description="Producer event generation UTC timestamp"),
  payload_ext_json STRING OPTIONS(description="Extensible semi-structured metrology attributes (JSON string)"),
  -- 2. Kafka Message Key Columns
  kafka_key STRUCT<
    key STRING
  > OPTIONS(description="Structured Kafka message key"),
  _key BYTES OPTIONS(description="Raw bytes Kafka message key"),
  -- 3. Kafka Metadata Struct (topic, partition, offset, insertTime)
  kafka_metadata STRUCT<
    topic STRING,
    `partition` INT64,
    offset INT64,
    insertTime TIMESTAMP
  > OPTIONS(description="Kafka broker offset & partition metadata injected by Spark")
)
CLUSTER BY fab_site_code, equipment_id, metric_name
WITH CONNECTION `{project_id}.{region}.{connection_id}`
OPTIONS (
  description = "Bronze schema-mapped Apache Iceberg streaming ingestion table for Apache Kafka telemetry events",
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = '{storage_uri}'{kms_opt}
);
""".strip()


def build_bronze_native_fallback_ddl(
    project_id: str,
    dataset_id: str,
    table_name: str,
    kms_key_name: Optional[str] = None,
    require_partition_filter: bool = True,
) -> str:
    """
    Builds the Partitioned & Clustered Native BigQuery Table DDL
    (used when BigLake Connection is not attached or when native mode is requested).
    """
    kms_opt = f',\n  kms_key_name = "{kms_key_name}"' if kms_key_name else ""
    req_filter = "true" if require_partition_filter else "false"
    return f"""
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_name}`
(
  event_id STRING OPTIONS(description="Unique producer event UUID"),
  equipment_id STRING OPTIONS(description="Fab cleanroom tool / equipment identifier"),
  fab_site_code STRING OPTIONS(description="Intel Fab facility site code (e.g., D1X, Ocotillo, Leixlip)"),
  wafer_lot_id STRING OPTIONS(description="Silicon wafer lot identifier"),
  metric_name STRING OPTIONS(description="Sensor or metrology parameter name"),
  metric_value FLOAT64 OPTIONS(description="Measured telemetry value"),
  status_code STRING OPTIONS(description="Equipment operational status code"),
  event_timestamp TIMESTAMP OPTIONS(description="Producer event generation UTC timestamp (Partition Key)"),
  payload_ext_json STRING OPTIONS(description="Extensible semi-structured metrology attributes (JSON string)"),
  kafka_key STRUCT<
    key STRING
  > OPTIONS(description="Structured Kafka message key"),
  _key BYTES OPTIONS(description="Raw bytes Kafka message key"),
  kafka_metadata STRUCT<
    topic STRING,
    `partition` INT64,
    offset INT64,
    insertTime TIMESTAMP
  > OPTIONS(description="Kafka broker offset & partition metadata injected by Spark")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY fab_site_code, equipment_id, metric_name
OPTIONS (
  description = "Bronze schema-mapped streaming ingestion table for Apache Kafka telemetry events",
  partition_expiration_days = 730,
  require_partition_filter = {req_filter}{kms_opt}
);
""".strip()


def build_dlq_table_ddl(
    project_id: str,
    dataset_id: str,
    dlq_table: str = "fab_telemetry_events_dlq",
    kms_key_name: Optional[str] = None,
    require_partition_filter: bool = True,
) -> str:
    """
    Builds the Dead-Letter Queue (DLQ) Quarantine Table DDL.
    """
    kms_opt = f',\n  kms_key_name = "{kms_key_name}"' if kms_key_name else ""
    req_filter = "true" if require_partition_filter else "false"
    return f"""
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{dlq_table}`
(
  timestamp TIMESTAMP OPTIONS(description="UTC timestamp when the record failed ingestion in Spark"),
  payloadString STRING OPTIONS(description="UTF-8 string representation of the rejected Kafka message (NULLABLE for binary decoding failures)"),
  payloadBytes BYTES OPTIONS(description="Exact uncorrupted raw Kafka message bytes for replay"),
  attributes ARRAY<STRUCT<
    key STRING,
    value STRING
  >> OPTIONS(description="Kafka record headers, topic, partition, offset, and input_format metadata"),
  errorMessage STRING OPTIONS(description="Exception message or schema validation rejection reason"),
  stacktrace STRING OPTIONS(description="Full exception stack trace for root-cause triage")
)
PARTITION BY DATE(timestamp)
CLUSTER BY errorMessage
OPTIONS (
  description = "Dead-Letter Queue (DLQ) table for failed Apache Kafka to BigQuery records",
  partition_expiration_days = 730,
  require_partition_filter = {req_filter}{kms_opt}
);
""".strip()


def build_curated_deduplication_view_ddl(
    project_id: str,
    dataset_id: str,
    bronze_table: str = "fab_telemetry_events_bronze",
    view_name: str = "vw_fab_telemetry_events_curated",
) -> str:
    """
    Builds the Unified Deduplication & Envelope Normalization View DDL.
    """
    return f"""
CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}`
AS
SELECT
  event_id,
  equipment_id,
  fab_site_code,
  wafer_lot_id,
  event_timestamp,
  metric_name,
  metric_value,
  status_code,
  payload_ext_json,
  JSON_VALUE(payload_ext_json, '$.chamber_id') AS chamber_id,
  JSON_VALUE(payload_ext_json, '$.metric_unit') AS metric_unit,
  COALESCE(
    kafka_key.key,
    SAFE_CONVERT_BYTES_TO_STRING(_key)
  ) AS normalized_kafka_key,
  kafka_metadata.topic AS kafka_topic,
  kafka_metadata.`partition` AS kafka_partition,
  kafka_metadata.offset AS kafka_offset,
  kafka_metadata.insertTime AS bq_landing_timestamp
FROM
  `{project_id}.{dataset_id}.{bronze_table}`
WHERE
  event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      COALESCE(
        event_id,
        FORMAT('%s:%d:%d', kafka_metadata.topic, kafka_metadata.`partition`, kafka_metadata.offset)
      )
    ORDER BY
      event_timestamp DESC,
      kafka_metadata.insertTime DESC
  ) = 1;
""".strip()


def build_dlq_triage_query_sql(
    project_id: str,
    dataset_id: str,
    dlq_table: str = "fab_telemetry_events_dlq",
) -> str:
    """
    Builds the Dead-Letter Queue (DLQ) Triage Query extracting Kafka
    topic, partition, offset, and input_format from the repeated `attributes` array.
    """
    return f"""
SELECT
  timestamp,
  errorMessage,
  (SELECT value FROM UNNEST(attributes) WHERE key = 'topic') AS kafka_topic,
  SAFE_CAST((SELECT value FROM UNNEST(attributes) WHERE key = 'partition') AS INT64) AS kafka_partition,
  SAFE_CAST((SELECT value FROM UNNEST(attributes) WHERE key = 'offset') AS INT64) AS kafka_offset,
  (SELECT value FROM UNNEST(attributes) WHERE key = 'input_format') AS input_format,
  COALESCE(payloadString, TO_HEX(payloadBytes)) AS inspectable_payload,
  stacktrace
FROM
  `{project_id}.{dataset_id}.{dlq_table}`
WHERE
  timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY
  timestamp DESC;
""".strip()


def provision_bigquery_tables_and_views(
    project_id: str,
    region: str,
    dataset_id: str,
    bucket: str,
    connection_id: str,
    kms_key_name: Optional[str] = None,
    require_partition_filter: bool = True,
    recreate_clean: bool = False,
) -> None:
    """Executes BigQuery DDL statements to provision the Bronze Iceberg table, DLQ table, and Curated View."""
    from google.cloud import bigquery

    bq_client = bigquery.Client(project=project_id, location=region)
    job_config = bigquery.QueryJobConfig(labels={"datacloud": "antigravity"})

    # 0. Ensure Target Dataset Exists in Region
    schema_ddl = (
        f"CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset_id}` "
        f"OPTIONS(location='{region}', description='Kafka to BigQuery Iceberg Lakehouse Dataset');"
    )
    bq_client.query(schema_ddl, job_config=job_config).result()

    if recreate_clean:
        logger.info("Recreating clean Bronze and DLQ tables (--recreate-clean)...")
        bq_client.query(
            f"DROP TABLE IF EXISTS `{project_id}.{dataset_id}.fab_telemetry_events_bronze`;",
            job_config=job_config,
        ).result()
        bq_client.query(
            f"DROP TABLE IF EXISTS `{project_id}.{dataset_id}.fab_telemetry_events_dlq`;",
            job_config=job_config,
        ).result()

    # 1. Provision Bronze Table (Try BigLake Managed Iceberg first, fallback to Native Partitioned if connection not ready)
    iceberg_ddl = build_bronze_iceberg_ddl(
        project_id=project_id,
        region=region,
        dataset_id=dataset_id,
        table_name="fab_telemetry_events_bronze",
        bucket=bucket,
        connection_id=connection_id,
        kms_key_name=kms_key_name,
    )
    try:
        logger.info(
            "Creating BigQuery-Managed Apache Iceberg Bronze Table: %s.%s.fab_telemetry_events_bronze...",
            project_id,
            dataset_id,
        )
        bq_client.query(iceberg_ddl, job_config=job_config).result()
        logger.info(
            "Successfully created BigQuery-Managed Apache Iceberg Bronze table!"
        )
    except Exception as ice_err:
        logger.warning(
            "BigLake Managed Iceberg DDL note (%s); provisioning Native Partitioned Bronze table...",
            ice_err,
        )
        fallback_ddl = build_bronze_native_fallback_ddl(
            project_id=project_id,
            dataset_id=dataset_id,
            table_name="fab_telemetry_events_bronze",
            kms_key_name=kms_key_name,
            require_partition_filter=require_partition_filter,
        )
        bq_client.query(fallback_ddl, job_config=job_config).result()
        logger.info("Successfully created Bronze table!")

    # 2. Provision DLQ Quarantine Table
    dlq_ddl = build_dlq_table_ddl(
        project_id=project_id,
        dataset_id=dataset_id,
        dlq_table="fab_telemetry_events_dlq",
        kms_key_name=kms_key_name,
        require_partition_filter=require_partition_filter,
    )
    logger.info(
        "Creating BigQuery Dead-Letter Queue (DLQ) Table: %s.%s.fab_telemetry_events_dlq...",
        project_id,
        dataset_id,
    )
    bq_client.query(dlq_ddl, job_config=job_config).result()
    logger.info("Successfully created DLQ table!")

    # 3. Provision Unified Deduplication & Envelope Normalization View
    view_ddl = build_curated_deduplication_view_ddl(
        project_id=project_id,
        dataset_id=dataset_id,
    )
    logger.info(
        "Creating Unified Deduplication View: %s.%s.vw_fab_telemetry_events_curated...",
        project_id,
        dataset_id,
    )
    bq_client.query(view_ddl, job_config=job_config).result()
    logger.info("Successfully created Unified Deduplication View!")


def main() -> None:
    """CLI entrypoint for provisioning BigQuery & Apache Iceberg Bronze/DLQ tables and Curated View."""
    parser = argparse.ArgumentParser(
        description="Provision BigQuery-Managed Apache Iceberg Bronze, DLQ & Curated View"
    )
    parser.add_argument(
        "--project-id", default=DEFAULT_PROJECT_ID, help="GCP Project ID"
    )
    parser.add_argument("--region", default=DEFAULT_REGION, help="GCP Region")
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help="Target BigQuery Bronze Dataset ID",
    )
    parser.add_argument(
        "--output-bucket",
        "--bucket",
        dest="output_bucket",
        default=DEFAULT_BUCKET,
        help="GCS Bucket for Iceberg warehouse",
    )
    parser.add_argument(
        "--connection-id",
        default=DEFAULT_CONNECTION_ID,
        help="BigLake Cloud Resource Connection ID",
    )
    parser.add_argument(
        "--kms-key-name",
        default=None,
        help="Optional Cloud KMS CryptoKey resource path for CMEK encryption",
    )
    parser.add_argument(
        "--require-partition-filter",
        default="true",
        choices=["true", "false"],
        help="Whether to enforce require_partition_filter on native partitioned Bronze/DLQ tables (default: true)",
    )
    parser.add_argument(
        "--recreate-clean",
        action="store_true",
        help="Drop and recreate Bronze and DLQ tables for a clean demonstration slate",
    )
    parser.add_argument(
        "--dry-run-ddl",
        action="store_true",
        help="Print generated DDL statements without executing against GCP",
    )
    args = parser.parse_args()
    req_filter_bool = args.require_partition_filter.lower() == "true"

    if args.dry_run_ddl:
        logger.info(
            "--- BigQuery-Managed Iceberg Bronze DDL ---\n%s",
            build_bronze_iceberg_ddl(
                args.project_id,
                args.region,
                args.dataset_id,
                "fab_telemetry_events_bronze",
                args.output_bucket,
                args.connection_id,
                args.kms_key_name,
            ),
        )
        logger.info(
            "--- BigQuery DLQ Table DDL ---\n%s",
            build_dlq_table_ddl(
                args.project_id,
                args.dataset_id,
                "fab_telemetry_events_dlq",
                args.kms_key_name,
                require_partition_filter=req_filter_bool,
            ),
        )
        logger.info(
            "--- Unified Deduplication View DDL ---\n%s",
            build_curated_deduplication_view_ddl(args.project_id, args.dataset_id),
        )
        logger.info(
            "--- DLQ Triage Query SQL ---\n%s",
            build_dlq_triage_query_sql(args.project_id, args.dataset_id),
        )
        return

    provision_bigquery_tables_and_views(
        project_id=args.project_id,
        region=args.region,
        dataset_id=args.dataset_id,
        bucket=args.output_bucket,
        connection_id=args.connection_id,
        kms_key_name=args.kms_key_name,
        require_partition_filter=req_filter_bool,
        recreate_clean=args.recreate_clean,
    )


if __name__ == "__main__":
    main()
