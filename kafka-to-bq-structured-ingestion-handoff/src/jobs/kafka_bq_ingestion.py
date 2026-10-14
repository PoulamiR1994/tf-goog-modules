#!/usr/bin/env python3
"""
Apache Kafka to BigQuery Apache Iceberg Bronze Ingestion Pipeline
(Standard PySpark Structured Streaming Template for Dataproc Serverless)

Core Capabilities:
1. Pluggable Kafka Source Connectivity:
   - GMK (`--kafka-source-type=GMK`): Google Cloud Managed Service for Apache Kafka via
     keyless IAM (`SASL_SSL` + `OAUTHBEARER` with `GcpLoginCallbackHandler`).
   - External Kafka (`--kafka-source-type=EXTERNAL`): External / GCE VM / On-Premises Kafka
     authenticated via PAM credentials in GCP Secret Manager (`SCRAM-SHA-512` / `PLAIN`).
2. Multi-Format Payload Parsing in Spark Structured Streaming (`--input-format`):
   - `JSON`: Parsed via Spark SQL `from_json` (supports flat JSON & Kafka Connect envelopes).
   - `AVRO_BINARY`: Decoded via Spark SQL `from_avro` against an explicit `.avsc` schema.
   - `AVRO_SCHEMA_REGISTRY`: Strips 5-byte Confluent/GMK wire header (`0x00` + 4B schema ID)
     and decodes via Spark SQL `from_avro`.
   - `SCHEMA_MAPPED_STRUCT`: Parses nested/heterogeneous JSON structs and applies declarative
     field mappings and required-field validation from `--schema-mapping-uri`.
3. Pre-Ingestion Exclusion Filtering:
   - Filters out wildcard patterns (`--exclude-patterns`) and exact matches (`--exclude-files`).
4. Dual BigQuery Sink Routing (`foreachBatch`):
   - Writes valid, deduplicated records to the BigQuery Bronze Apache Iceberg table
     (`fab_telemetry_events_bronze`) via the BigQuery Storage Write API (`writeMethod=direct`).
   - Routes malformed/corrupt records to the BigQuery Dead-Letter Queue table
     (`fab_telemetry_events_dlq`).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

try:
    from google.cloud import secretmanager, storage
except ImportError:
    secretmanager = None  # type: ignore[assignment]
    storage = None  # type: ignore[assignment]

try:
    from pyspark.sql import Column, DataFrame, SparkSession, Window
    from pyspark.sql.avro.functions import from_avro
    import pyspark.sql.functions as F
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:
    Column = Any  # type: ignore[assignment,misc]
    DataFrame = Any  # type: ignore[assignment,misc]
    SparkSession = Any  # type: ignore[assignment,misc]
    Window = Any  # type: ignore[assignment,misc]
    from_avro = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

    class _MockSparkType:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.fields = args[0] if args and isinstance(args[0], list) else []
            self.name = args[0] if args and isinstance(args[0], str) else ""
            self.dataType = args[1] if len(args) > 1 else None
            self.nullable = args[2] if len(args) > 2 else True

    ArrayType = BinaryType = DoubleType = LongType = _MockSparkType  # type: ignore[assignment,misc]
    StringType = StructField = StructType = TimestampType = _MockSparkType  # type: ignore[assignment,misc]

# Ensure connector and utility modules are importable both locally and via --py-files
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_SRC_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_SRC_ROOT, ".."))
for _path in (
    _CURRENT_DIR,
    _REPO_ROOT,
    _SRC_ROOT,
    os.path.join(_SRC_ROOT, "utils"),
    os.path.join(_SRC_ROOT, "connectors"),
):
    if _path not in sys.path:
        sys.path.insert(0, _path)

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

try:
    from connectors.external_kafka_connector import (  # noqa: E402
        ExternalKafkaEndpointResolver,
        ExternalKafkaPAMResolver,
        ExternalKafkaSparkConfigBuilder,
    )
    from connectors.gmk_connector import (  # noqa: E402
        GMKEndpointResolver,
        GMKSparkConfigBuilder,
    )
except ImportError:
    from external_kafka_connector import (  # type: ignore[no-redef] # noqa: E402
        ExternalKafkaEndpointResolver,
        ExternalKafkaPAMResolver,
        ExternalKafkaSparkConfigBuilder,
    )
    from gmk_connector import (  # type: ignore[no-redef] # noqa: E402
        GMKEndpointResolver,
        GMKSparkConfigBuilder,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kafka_bq_ingestion")

def _load_job_config_defaults() -> Dict[str, str]:
    """Loads environment defaults from config/job_config.yaml if present."""
    defaults: Dict[str, str] = {}
    candidates = [
        os.environ.get("JOB_CONFIG_PATH", ""),
        os.path.join(_REPO_ROOT, "config", "job_config.yaml"),
        os.path.join(_CURRENT_DIR, "config", "job_config.yaml"),
        os.path.join(_CURRENT_DIR, "job_config.yaml"),
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


_JOB_CFG = _load_job_config_defaults()
DEFAULT_PROJECT_ID = (
    os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or _JOB_CFG.get("project_id", "")
)
DEFAULT_REGION = _JOB_CFG.get("region", "us-central1")
DEFAULT_DATASET_ID = _JOB_CFG.get("output_dataset", "bq_bronze_fab_telemetry_poc")
DEFAULT_BRONZE_TABLE = _JOB_CFG.get("output_table", "fab_telemetry_events_bronze")
DEFAULT_DLQ_TABLE = _JOB_CFG.get("dlq_table", "fab_telemetry_events_dlq")
DEFAULT_BUCKET_NAME = _JOB_CFG.get(
    "output_bucket",
    f"{DEFAULT_PROJECT_ID}-kafka-landing-zone" if DEFAULT_PROJECT_ID else "",
)
DEFAULT_PAM_SECRET = _JOB_CFG.get("secret_name", "kafka-pam-credentials")
DEFAULT_CONSUMER_GROUP = _JOB_CFG.get("consumer_group_id", "cg-spark-fab-telemetry-poc")
SUPPORTED_INPUT_FORMATS = (
    "JSON",
    "AVRO_BINARY",
    "AVRO_SCHEMA_REGISTRY",
    "SCHEMA_MAPPED_STRUCT",
)

# Target BigQuery Bronze Apache Iceberg Table Schema
BRONZE_TABLE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("equipment_id", StringType(), True),
        StructField("fab_site_code", StringType(), True),
        StructField("wafer_lot_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("status_code", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("payload_ext_json", StringType(), True),
        StructField(
            "kafka_key",
            StructType([StructField("key", StringType(), True)]),
            True,
        ),
        StructField("_key", BinaryType(), True),
        StructField(
            "kafka_metadata",
            StructType(
                [
                    StructField("topic", StringType(), True),
                    StructField("partition", LongType(), True),
                    StructField("offset", LongType(), True),
                    StructField("insertTime", TimestampType(), True),
                ]
            ),
            True,
        ),
    ]
)

# Target BigQuery Dead-Letter Queue (DLQ) Table Schema
DLQ_TABLE_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("payloadString", StringType(), True),
        StructField("payloadBytes", BinaryType(), True),
        StructField(
            "attributes",
            ArrayType(
                StructType(
                    [
                        StructField("key", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("errorMessage", StringType(), True),
        StructField("stacktrace", StringType(), True),
    ]
)

# Spark SQL Schema for parsing JSON messages (including optional Kafka Connect 'payload' wrapper)
_FLAT_JSON_FIELDS = [
    StructField("event_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("fab_site_code", StringType(), True),
    StructField("wafer_lot_id", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("payload_ext_json", StringType(), True),
    StructField("chamber_id", StringType(), True),
    StructField("metric_unit", StringType(), True),
]

JSON_WIRE_SCHEMA = StructType(
    _FLAT_JSON_FIELDS
    + [
        StructField("payload", StructType(_FLAT_JSON_FIELDS), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


# ------------------------------------------------------------------------------
# 1. Secret Manager, GCS Schema Loader & Pluggable Kafka Connector Routers
# ------------------------------------------------------------------------------
def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Fetches a secret payload from Google Cloud Secret Manager."""
    if secret_id.startswith("projects/"):
        secret_path = (
            secret_id
            if "/versions/" in secret_id
            else f"{secret_id}/versions/{version_id}"
        )
    else:
        secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8").strip()


def resolve_pam_credentials(
    project_id: str,
    secret_name: Optional[str] = DEFAULT_PAM_SECRET,
    kafka_source_type: str = "AUTO",
) -> Dict[str, Any]:
    """Resolves keyless GMK OAUTHBEARER config or External Kafka PAM credentials from Secret Manager."""
    if kafka_source_type.upper() == "GMK":
        return {
            "auth_mode": "GMK_OAUTHBEARER",
            "sasl_mechanism": "OAUTHBEARER",
            "security_protocol": "SASL_SSL",
        }
    return ExternalKafkaPAMResolver.resolve_pam_credentials(
        project_id=project_id,
        secret_fetcher=get_secret,
        secret_name=secret_name,
    )


def load_json_config(
    uri_or_path: Optional[str],
    default_config: Dict[str, Any],
    project_id: str = DEFAULT_PROJECT_ID,
) -> Dict[str, Any]:
    """Loads a JSON file (.avsc schema or field mapping spec) from GCS (gs://) or local disk."""
    if not uri_or_path or not uri_or_path.strip():
        return default_config
    path = uri_or_path.strip()
    try:
        if path.startswith("gs://"):
            bucket_name, blob_path = path[5:].split("/", 1)
            client = storage.Client(project=project_id)
            raw_text = client.bucket(bucket_name).blob(blob_path).download_as_text()
            return json.loads(raw_text)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                return json.loads(fh.read())
    except Exception as ex:
        logger.warning(
            "Could not load JSON config from '%s' (%s); using default.", path, ex
        )
    return default_config


class KafkaEndpointResolver:
    """Routes endpoint normalization to `GMKEndpointResolver` or `ExternalKafkaEndpointResolver`."""

    @classmethod
    def resolve_endpoints(
        cls,
        raw_bootstrap: str,
        raw_topic: Optional[str] = None,
        explicit_source_type: str = "AUTO",
    ) -> Tuple[str, str, str]:
        explicit_upper = (explicit_source_type or "AUTO").upper()
        is_gmk = explicit_upper == "GMK" or (
            explicit_upper == "AUTO"
            and GMKEndpointResolver.is_gmk_endpoint(raw_bootstrap, raw_topic)
        )
        if is_gmk:
            boot, topic = GMKEndpointResolver.resolve_gmk_endpoints(
                raw_bootstrap, raw_topic
            )
            return boot, topic, "GMK"
        boot, topic = ExternalKafkaEndpointResolver.resolve_external_endpoints(
            raw_bootstrap, raw_topic
        )
        return boot, topic, "EXTERNAL"


class KafkaSparkConfigBuilder:
    """Routes Spark Kafka reader options to `GMKSparkConfigBuilder` or `ExternalKafkaSparkConfigBuilder`."""

    @staticmethod
    def build_reader_options(
        bootstrap_servers: str,
        topic: str,
        source_type: str,
        pam_creds: Dict[str, Any],
        starting_offsets: str = "earliest",
        consumer_group_id: str = DEFAULT_CONSUMER_GROUP,
        max_offsets_per_trigger: int = 500000,
        fail_on_data_loss: bool = True,
    ) -> Dict[str, str]:
        if source_type.upper() == "GMK":
            return GMKSparkConfigBuilder.build_gmk_reader_options(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                starting_offsets=starting_offsets,
                consumer_group_id=consumer_group_id,
                max_offsets_per_trigger=max_offsets_per_trigger,
                fail_on_data_loss=fail_on_data_loss,
            )
        return ExternalKafkaSparkConfigBuilder.build_external_reader_options(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            pam_creds=pam_creds,
            starting_offsets=starting_offsets,
            consumer_group_id=consumer_group_id,
            max_offsets_per_trigger=max_offsets_per_trigger,
            fail_on_data_loss=fail_on_data_loss,
        )


# ------------------------------------------------------------------------------
# 2. Standard Spark Structured Streaming DataFrame Transformations (4 Formats)
# ------------------------------------------------------------------------------
def parse_kafka_dataframe(
    raw_df: DataFrame,
    input_format: str,
    avro_schema_json: str,
    schema_mapping_spec: Dict[str, Any],
) -> DataFrame:
    """
    Parses a raw Spark Kafka DataFrame (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`)
    across any of the 4 supported formats (`JSON`, `AVRO_BINARY`, `AVRO_SCHEMA_REGISTRY`,
    `SCHEMA_MAPPED_STRUCT`) using native Spark SQL expressions (`from_json`, `from_avro`, `get_json_object`).
    """
    fmt = input_format.strip().upper()
    df = raw_df.withColumn("_value_str", F.col("value").cast("string")).withColumn(
        "_key_str", F.col("key").cast("string")
    )

    if fmt == "JSON":
        df = df.withColumn(
            "_parsed",
            F.from_json(
                F.col("_value_str"),
                JSON_WIRE_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
        for col_name in (
            "event_id",
            "equipment_id",
            "fab_site_code",
            "wafer_lot_id",
            "metric_name",
            "status_code",
            "event_timestamp",
            "source_file",
        ):
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.col(f"_parsed.payload.{col_name}"),
                    F.col(f"_parsed.{col_name}"),
                ),
            )
        raw_metric_col = F.coalesce(
            F.col("_parsed.payload.metric_value"), F.col("_parsed.metric_value")
        )
        df = df.withColumn("metric_value", raw_metric_col.cast("double"))
        df = df.withColumn(
            "payload_ext_json",
            F.coalesce(
                F.col("_parsed.payload.payload_ext_json"),
                F.col("_parsed.payload_ext_json"),
                F.to_json(
                    F.struct(
                        F.coalesce(
                            F.col("_parsed.payload.chamber_id"),
                            F.col("_parsed.chamber_id"),
                        ).alias("chamber_id"),
                        F.coalesce(
                            F.col("_parsed.payload.metric_unit"),
                            F.col("_parsed.metric_unit"),
                        ).alias("metric_unit"),
                    )
                ),
            ),
        )
        df = df.withColumn(
            "_error_message",
            F.when(F.col("value").isNull(), F.lit("Empty Kafka message payload"))
            .when(
                F.col("_parsed._corrupt_record").isNotNull() | F.col("_parsed").isNull(),
                F.concat(
                    F.lit("Malformed JSON syntax: "),
                    F.coalesce(F.col("_value_str"), F.lit("")),
                ),
            )
            .when(
                raw_metric_col.isNotNull() & F.col("metric_value").isNull(),
                F.concat(
                    F.lit("TypeMismatchError: Cannot cast metric_value '"),
                    raw_metric_col,
                    F.lit("' to FLOAT64"),
                ),
            )
            .when(
                F.col("event_id").isNull() & F.col("equipment_id").isNull(),
                F.lit("SchemaValidationError: Missing event_id and equipment_id"),
            )
            .otherwise(F.lit(None).cast("string")),
        )

    elif fmt in ("AVRO_BINARY", "AVRO_SCHEMA_REGISTRY"):
        if fmt == "AVRO_SCHEMA_REGISTRY":
            # Confluent / GMK Schema Registry Wire Format: [0x00 (1B)][Schema ID (4B)][Avro Payload]
            valid_wire_header = (F.length(F.col("value")) > 5) & (
                F.expr("substring(value, 1, 1) = X'00'")
            )
            avro_bytes_col = F.when(
                valid_wire_header,
                F.expr("substring(value, 6, length(value) - 5)"),
            ).otherwise(F.lit(None).cast("binary"))
        else:
            valid_wire_header = F.col("value").isNotNull() & (
                F.length(F.col("value")) > 0
            )
            avro_bytes_col = F.col("value")

        df = df.withColumn(
            "_parsed",
            from_avro(avro_bytes_col, avro_schema_json, {"mode": "PERMISSIVE"}),
        )
        for col_name in (
            "event_id",
            "equipment_id",
            "fab_site_code",
            "wafer_lot_id",
            "metric_name",
            "status_code",
            "event_timestamp",
            "source_file",
            "payload_ext_json",
        ):
            df = df.withColumn(col_name, F.col(f"_parsed.{col_name}"))
        df = df.withColumn(
            "metric_value", F.col("_parsed.metric_value").cast("double")
        )
        df = df.withColumn(
            "_error_message",
            F.when(
                ~valid_wire_header,
                F.lit(
                    "Invalid Confluent/GMK Wire Format header (expected magic byte 0x00 and >5 bytes)"
                ),
            )
            .when(
                F.col("_parsed").isNull()
                | (F.col("event_id").isNull() & F.col("equipment_id").isNull()),
                F.lit(f"AvroDecodeError: Malformed {fmt} payload bytes"),
            )
            .otherwise(F.lit(None).cast("string")),
        )

    elif fmt == "SCHEMA_MAPPED_STRUCT":
        field_mappings: Dict[str, str] = schema_mapping_spec.get("field_mappings") or {}
        defaults: Dict[str, Any] = schema_mapping_spec.get("defaults") or {}
        required_src: List[str] = (
            schema_mapping_spec.get("required_source_fields") or []
        )

        # Build reverse lookup: target Bronze column -> list of candidate JSON paths
        reverse_map: Dict[str, List[str]] = {}
        for src_field, target_col in field_mappings.items():
            reverse_map.setdefault(target_col, []).append(src_field)

        def _extract_mapped_col(target_col: str) -> Column:
            candidates = [target_col] + reverse_map.get(target_col, [])
            json_exprs: List[Column] = []
            for cand in candidates:
                json_exprs.append(F.get_json_object(F.col("_value_str"), f"$.{cand}"))
                json_exprs.append(
                    F.get_json_object(F.col("_value_str"), f"$.device.{cand}")
                )
                json_exprs.append(
                    F.get_json_object(F.col("_value_str"), f"$.telemetry.{cand}")
                )
            if target_col in defaults and defaults[target_col] is not None:
                json_exprs.append(F.lit(str(defaults[target_col])))
            return F.coalesce(*json_exprs)

        for col_name in (
            "event_id",
            "equipment_id",
            "fab_site_code",
            "wafer_lot_id",
            "metric_name",
            "status_code",
            "event_timestamp",
            "source_file",
        ):
            df = df.withColumn(col_name, _extract_mapped_col(col_name))

        raw_metric_col = _extract_mapped_col("metric_value")
        df = df.withColumn("metric_value", raw_metric_col.cast("double"))
        df = df.withColumn(
            "payload_ext_json",
            F.to_json(
                F.struct(
                    F.coalesce(
                        F.get_json_object(F.col("_value_str"), "$.chamber_id"),
                        F.get_json_object(F.col("_value_str"), "$.device.chamber_id"),
                    ).alias("chamber_id"),
                    F.coalesce(
                        F.get_json_object(F.col("_value_str"), "$.metric_unit"),
                        F.get_json_object(F.col("_value_str"), "$.telemetry.metric_unit"),
                    ).alias("metric_unit"),
                )
            ),
        )

        # Validate JSON syntax and any required_source_fields
        error_expr = F.when(
            F.col("_value_str").isNull()
            | F.get_json_object(F.col("_value_str"), "$").isNull(),
            F.lit("Malformed JSON in SCHEMA_MAPPED_STRUCT payload"),
        )
        for req_field in required_src:
            req_val = F.get_json_object(F.col("_value_str"), f"$.{req_field}")
            error_expr = error_expr.when(
                req_val.isNull() | (req_val == ""),
                F.lit(f"Missing required source field '{req_field}'"),
            )
        error_expr = error_expr.when(
            raw_metric_col.isNotNull() & F.col("metric_value").isNull(),
            F.lit("TypeMismatchError: Cannot cast mapped metric_value to FLOAT64"),
        ).when(
            F.col("event_id").isNull() & F.col("equipment_id").isNull(),
            F.lit("SchemaValidationError: Missing mapped event_id and equipment_id"),
        )
        df = df.withColumn(
            "_error_message", error_expr.otherwise(F.lit(None).cast("string"))
        )
    else:
        raise ValueError(f"Unsupported input_format: {fmt}")

    # Populate canonical Bronze Timestamp & Kafka lineage columns
    return (
        df.withColumn(
            "event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("event_timestamp")),
                F.col("timestamp"),
                F.current_timestamp(),
            ),
        )
        .withColumn("kafka_key", F.struct(F.col("_key_str").alias("key")))
        .withColumn("_key", F.col("key"))
        .withColumn(
            "kafka_metadata",
            F.struct(
                F.col("topic").alias("topic"),
                F.col("partition").cast("long").alias("partition"),
                F.col("offset").cast("long").alias("offset"),
                F.current_timestamp().alias("insertTime"),
            ),
        )
    )


def filter_excluded_dataframe(
    df: DataFrame,
    exclude_patterns: List[str],
    exclude_files: List[str],
) -> DataFrame:
    """
    Applies wildcard (`--exclude-patterns`) and exact-match (`--exclude-files`)
    exclusions in Spark SQL across `source_file`, `equipment_id`, `metric_name`, and `_key_str`.
    Only valid candidate records are checked so corrupt DLQ records are still quarantined.
    """
    if not exclude_patterns and not exclude_files:
        return df

    candidate_cols = [
        F.coalesce(F.lower(F.col("source_file")), F.lit("")),
        F.coalesce(
            F.lower(F.element_at(F.split(F.col("source_file"), "/"), -1)), F.lit("")
        ),
        F.coalesce(F.lower(F.col("equipment_id")), F.lit("")),
        F.coalesce(F.lower(F.col("metric_name")), F.lit("")),
        F.coalesce(F.lower(F.col("_key_str")), F.lit("")),
    ]

    is_excluded = F.lit(False)
    exact_lower = [e.strip().lower() for e in exclude_files if e and e.strip()]
    if exact_lower:
        for col_expr in candidate_cols:
            is_excluded = is_excluded | col_expr.isin(exact_lower)

    for pat in exclude_patterns:
        clean_pat = pat.strip().lower()
        if not clean_pat:
            continue
        sql_like = clean_pat.replace("*", "%").replace("?", "_")
        for col_expr in candidate_cols:
            is_excluded = is_excluded | col_expr.like(sql_like)

    # Keep DLQ records (where _error_message IS NOT NULL) and non-excluded valid records
    return df.filter(F.col("_error_message").isNotNull() | (~is_excluded))


def deduplicate_bronze_dataframe(bronze_df: DataFrame) -> DataFrame:
    """Deduplicates valid records within a micro-batch by `event_id` (retaining latest offset)."""
    dedup_key = F.coalesce(
        F.col("event_id"),
        F.concat_ws(
            ":",
            F.col("kafka_metadata.topic"),
            F.col("kafka_metadata.partition").cast("string"),
            F.col("kafka_metadata.offset").cast("string"),
        ),
    )
    window_spec = Window.partitionBy(dedup_key).orderBy(
        F.col("event_timestamp").desc_nulls_last(),
        F.col("kafka_metadata.offset").desc_nulls_last(),
    )
    return (
        bronze_df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_dlq_dataframe(invalid_df: DataFrame, input_format: str) -> DataFrame:
    """Shapes rejected records into the BigQuery Dead-Letter Queue (`fab_telemetry_events_dlq`) schema."""
    return invalid_df.select(
        F.current_timestamp().alias("timestamp"),
        F.col("_value_str").alias("payloadString"),
        F.col("value").alias("payloadBytes"),
        F.array(
            F.struct(F.lit("topic").alias("key"), F.col("topic").alias("value")),
            F.struct(
                F.lit("partition").alias("key"),
                F.col("partition").cast("string").alias("value"),
            ),
            F.struct(
                F.lit("offset").alias("key"),
                F.col("offset").cast("string").alias("value"),
            ),
            F.struct(
                F.lit("input_format").alias("key"),
                F.lit(input_format).alias("value"),
            ),
        ).alias("attributes"),
        F.col("_error_message").alias("errorMessage"),
        F.col("_error_message").alias("stacktrace"),
    )


# ------------------------------------------------------------------------------
# 3. Standard Spark Structured Streaming `foreachBatch` Writer & Runner
# ------------------------------------------------------------------------------
def run_spark_structured_streaming(
    spark: SparkSession,
    args: argparse.Namespace,
    reader_options: Dict[str, str],
    avro_schema_json: str,
    schema_mapping_spec: Dict[str, Any],
    exclude_patterns: List[str],
    exclude_files: List[str],
    checkpoint_uri: str,
) -> None:
    """
    Starts the Spark Structured Streaming query from Kafka and writes each micro-batch
    to the BigQuery Bronze Apache Iceberg table and BigQuery DLQ table.
    """
    bronze_table_fqn = f"{args.project_id}.{args.output_dataset}.{args.output_table}"
    dlq_table_fqn = f"{args.project_id}.{args.output_dataset}.{args.dlq_table}"
    bronze_cols = [field.name for field in BRONZE_TABLE_SCHEMA.fields]

    raw_stream_df = spark.readStream.format("kafka").options(**reader_options).load()

    def _write_micro_batch(batch_df: DataFrame, epoch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info("Micro-batch epoch_id=%d is empty; skipping.", epoch_id)
            return

        parsed_df = parse_kafka_dataframe(
            raw_df=batch_df,
            input_format=args.input_format,
            avro_schema_json=avro_schema_json,
            schema_mapping_spec=schema_mapping_spec,
        )
        filtered_df = filter_excluded_dataframe(
            df=parsed_df,
            exclude_patterns=exclude_patterns,
            exclude_files=exclude_files,
        ).cache()

        try:
            # 1. Valid records -> Deduplicate -> Write to BigQuery Bronze Apache Iceberg Table
            valid_df = filtered_df.filter(F.col("_error_message").isNull()).select(
                *bronze_cols
            )
            dedup_bronze_df = deduplicate_bronze_dataframe(valid_df).coalesce(4)
            if not dedup_bronze_df.isEmpty():
                (
                    dedup_bronze_df.write.format("bigquery")
                    .option("table", bronze_table_fqn)
                    .option("writeMethod", args.write_method)
                    .option("temporaryGcsBucket", args.output_bucket)
                    .mode("append")
                    .save()
                )
                logger.info(
                    "Epoch %d: Appended valid records to Bronze Iceberg table %s.",
                    epoch_id,
                    bronze_table_fqn,
                )

            # 2. Invalid/corrupt records -> Write to BigQuery Dead-Letter Queue (DLQ) Table
            invalid_df = filtered_df.filter(F.col("_error_message").isNotNull())
            if not invalid_df.isEmpty():
                dlq_df = build_dlq_dataframe(invalid_df, args.input_format)
                (
                    dlq_df.write.format("bigquery")
                    .option("table", dlq_table_fqn)
                    .option("writeMethod", args.write_method)
                    .option("temporaryGcsBucket", args.output_bucket)
                    .mode("append")
                    .save()
                )
                logger.info(
                    "Epoch %d: Quarantined invalid records to DLQ table %s.",
                    epoch_id,
                    dlq_table_fqn,
                )
        finally:
            filtered_df.unpersist()

    safe_topic_name = "".join(
        c if c.isalnum() else "_" for c in str(reader_options.get("subscribe", "kafka"))
    )
    query_name = (
        f"kafka_to_bq_{args.kafka_source_type.lower()}_{safe_topic_name}_{args.input_format.lower()}"
    )
    writer = (
        raw_stream_df.writeStream.queryName(query_name)
        .foreachBatch(_write_micro_batch)
        .option("checkpointLocation", checkpoint_uri)
    )

    if args.batch_mode == "available_now":
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(
            processingTime=f"{args.batch_interval_minutes} minutes"
        ).start()

    timeout_sec = max(15, int(args.await_termination_timeout_ms / 1000))
    query.awaitTermination(timeout=timeout_sec)
    if query.isActive:
        query.stop()
    logger.info("Spark Structured Streaming query completed cleanly.")


# ------------------------------------------------------------------------------
# 4. CLI Argument Parser & Main Entrypoint
# ------------------------------------------------------------------------------
def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parses CLI parameters for the Kafka to BigQuery Iceberg Ingestion Template."""
    parser = argparse.ArgumentParser(
        description="Apache Kafka to BigQuery Apache Iceberg PySpark Structured Streaming Template"
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument(
        "--kafka-bootstrap-servers",
        "--kafka-broker",
        dest="kafka_bootstrap_servers",
        required=True,
        help="Kafka bootstrap servers (GMK bootstrap FQDN/URI or External host:port)",
    )
    parser.add_argument(
        "--kafka-topic",
        "--topic",
        dest="kafka_topic",
        default="",
        help="Kafka source topic name",
    )
    parser.add_argument(
        "--kafka-source-type",
        default="AUTO",
        choices=["AUTO", "GMK", "EXTERNAL"],
        help="Kafka broker type: GMK, EXTERNAL, or AUTO",
    )
    parser.add_argument(
        "--secret-name",
        default=DEFAULT_PAM_SECRET,
        help="GCP Secret Manager PAM secret name for External Kafka authentication",
    )
    parser.add_argument(
        "--input-format",
        "--message-format",
        dest="input_format",
        default="JSON",
        choices=list(SUPPORTED_INPUT_FORMATS),
        help="Payload format: JSON, AVRO_BINARY, AVRO_SCHEMA_REGISTRY, or SCHEMA_MAPPED_STRUCT",
    )
    parser.add_argument(
        "--avro-schema-path",
        "--schema-file-gcs-uri",
        dest="avro_schema_path",
        default="",
        help="GCS URI or local path to .avsc schema file",
    )
    parser.add_argument(
        "--schema-mapping-uri",
        default="",
        help="GCS URI or local path to JSON field mapping specification for SCHEMA_MAPPED_STRUCT",
    )
    parser.add_argument(
        "--schema-registry-url",
        default="",
        help="Optional Schema Registry URL for AVRO_SCHEMA_REGISTRY",
    )
    parser.add_argument(
        "--exclude-patterns",
        default="",
        help="Comma-separated wildcard/glob patterns to exclude (e.g. '*.tmp,*.bak,~$*')",
    )
    parser.add_argument(
        "--exclude-files",
        default="",
        help="Comma-separated exact file, key, or equipment names to exclude",
    )
    parser.add_argument(
        "--batch-mode",
        "--trigger-mode",
        dest="batch_mode",
        default="available_now",
        choices=["available_now", "processing_time"],
        help="Spark trigger mode: 'available_now' or 'processing_time'",
    )
    parser.add_argument(
        "--batch-size",
        "--max-offsets-per-trigger",
        dest="batch_size",
        type=int,
        default=500000,
        help="Maximum Kafka offsets processed per micro-batch (maxOffsetsPerTrigger)",
    )
    parser.add_argument(
        "--batch-interval-minutes",
        type=int,
        default=5,
        help="Micro-batch trigger interval in minutes when --batch-mode=processing_time",
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="Kafka starting offset strategy: 'earliest' or 'latest'",
    )
    parser.add_argument(
        "--consumer-group-id",
        default=DEFAULT_CONSUMER_GROUP,
        help="Kafka consumer group ID",
    )
    parser.add_argument(
        "--fail-on-data-loss",
        default="true",
        choices=["true", "false"],
    )
    parser.add_argument(
        "--await-termination-timeout-ms",
        type=int,
        default=600000,
    )
    parser.add_argument(
        "--output-format",
        default="ICEBERG",
        choices=["ICEBERG", "NATIVE_BQ", "BIGQUERY_NATIVE"],
        help="Target table format on Bronze layer (default: ICEBERG)",
    )
    parser.add_argument("--output-dataset", default=DEFAULT_DATASET_ID)
    parser.add_argument("--output-table", default=DEFAULT_BRONZE_TABLE)
    parser.add_argument("--dlq-table", default=DEFAULT_DLQ_TABLE)
    parser.add_argument(
        "--output-bucket",
        "--gcs-bucket",
        dest="output_bucket",
        default=DEFAULT_BUCKET_NAME,
    )
    parser.add_argument("--checkpoint-location", default="")
    parser.add_argument(
        "--write-method",
        default="direct",
        choices=["direct", "indirect"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    """Main entrypoint for the Spark Structured Streaming Kafka-to-BigQuery Iceberg pipeline."""
    args = parse_arguments(argv)

    bootstrap_servers, resolved_topic, resolved_source_type = (
        KafkaEndpointResolver.resolve_endpoints(
            raw_bootstrap=args.kafka_bootstrap_servers,
            raw_topic=args.kafka_topic,
            explicit_source_type=args.kafka_source_type,
        )
    )
    exclude_patterns = [
        p.strip() for p in args.exclude_patterns.split(",") if p.strip()
    ]
    exclude_files = [f.strip() for f in args.exclude_files.split(",") if f.strip()]
    checkpoint_uri = (
        args.checkpoint_location
        or f"gs://{args.output_bucket}/checkpoints/kafka_to_bq/{resolved_source_type.lower()}_{resolved_topic}_{args.input_format.lower()}"
    )

    pam_creds = resolve_pam_credentials(
        project_id=args.project_id,
        secret_name=args.secret_name,
        kafka_source_type=resolved_source_type,
    )
    reader_options = KafkaSparkConfigBuilder.build_reader_options(
        bootstrap_servers=bootstrap_servers,
        topic=resolved_topic,
        source_type=resolved_source_type,
        pam_creds=pam_creds,
        starting_offsets=args.starting_offsets,
        consumer_group_id=args.consumer_group_id,
        max_offsets_per_trigger=args.batch_size,
        fail_on_data_loss=(args.fail_on_data_loss.lower() == "true"),
    )

    avro_schema_dict = load_json_config(
        args.avro_schema_path,
        default_config=DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
        project_id=args.project_id,
    )
    schema_mapping_spec = load_json_config(
        args.schema_mapping_uri,
        default_config=DEFAULT_SCHEMA_MAPPING_SPEC,
        project_id=args.project_id,
    )

    logger.info(
        "Starting Kafka-to-BigQuery Iceberg Stream: source=%s, broker=%s, topic=%s, format=%s, target=%s.%s.%s",
        resolved_source_type,
        bootstrap_servers,
        resolved_topic,
        args.input_format,
        args.project_id,
        args.output_dataset,
        args.output_table,
    )

    spark = (
        SparkSession.builder.appName(
            f"KafkaToBQIceberg_{resolved_source_type}_{resolved_topic}_{args.input_format}"
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        run_spark_structured_streaming(
            spark=spark,
            args=args,
            reader_options=reader_options,
            avro_schema_json=json.dumps(avro_schema_dict),
            schema_mapping_spec=schema_mapping_spec,
            exclude_patterns=exclude_patterns,
            exclude_files=exclude_files,
            checkpoint_uri=checkpoint_uri,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
