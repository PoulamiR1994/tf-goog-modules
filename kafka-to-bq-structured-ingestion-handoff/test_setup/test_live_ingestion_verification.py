#!/usr/bin/env python3
"""
End-to-End 4-Format & Dual-Source Verification Suite for the Apache Kafka to BigQuery
Apache Iceberg Bronze Ingestion Template.

Executes verification across every input payload format:
1. Input Format 1 - JSON (--input-format=JSON)
2. Input Format 2 - Plain Binary Avro (--input-format=AVRO_BINARY)
3. Input Format 3 - Confluent / GMK Schema Registry Wire Format (--input-format=AVRO_SCHEMA_REGISTRY)
4. Input Format 4 - Schema-Mapped Struct (--input-format=SCHEMA_MAPPED_STRUCT)
5. Hosting Boundary Verification (GMK vs. External Kafka)
"""

import logging
import os
import sys
from typing import Dict

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
_SRC_ROOT = os.path.join(_REPO_ROOT, "src")
for _p in (_REPO_ROOT, _SRC_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from jobs.kafka_bq_ingestion import (  # noqa: E402
    KafkaEndpointResolver,
    KafkaSparkConfigBuilder,
)
from test_setup.generate_enterprise_test_data import (  # noqa: E402
    generate_avro_binary_test_messages,
    generate_avro_schema_registry_test_messages,
    generate_json_test_messages,
    generate_schema_mapped_struct_test_messages,
)
from tests.integration.test_dlq_and_deduplication import _run_mapper_on_messages  # noqa: E402
from utils.schema_and_dlq_handler import BronzeRecordMapper, ExclusionFilter  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("test_live_ingestion_verification")


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
PROJECT_ID = _CFG.get("project_id", "example-gcp-project")
DATASET_ID = _CFG.get("output_dataset", "bq_bronze_fab_telemetry_poc")
BUCKET_NAME = _CFG.get("output_bucket", f"{PROJECT_ID}-kafka-landing-zone")


def run_verification() -> None:
    """Runs the complete 4-format + GMK/External verification suite."""
    logger.info("Starting End-to-End Verification (Project=%s, Dataset=%s)", PROJECT_ID, DATASET_ID)

    exclusion_filter = ExclusionFilter.from_strings(
        patterns_str="*.tmp,*.bak,~$*,*_debug_*",
        exact_str="desktop.ini,thumbs.db,TEST_TOOL_IGNORE",
    )

    # 1. JSON
    json_msgs = generate_json_test_messages(num_valid=20)
    mapper_json = BronzeRecordMapper(input_format="JSON", exclusion_filter=exclusion_filter)
    bronze_1, dlq_1, excl_1 = _run_mapper_on_messages(mapper_json, json_msgs)
    assert len(bronze_1) == 20
    assert len(dlq_1) == 2
    assert excl_1 == 2

    # 2. AVRO_BINARY
    avro_bin_msgs = generate_avro_binary_test_messages(num_valid=10)
    mapper_avro = BronzeRecordMapper(input_format="AVRO_BINARY", exclusion_filter=exclusion_filter)
    bronze_2, dlq_2, excl_2 = _run_mapper_on_messages(mapper_avro, avro_bin_msgs)
    assert len(bronze_2) == 10
    assert excl_2 == 1
    assert len(dlq_2) == 1

    # 3. AVRO_SCHEMA_REGISTRY
    avro_sr_msgs = generate_avro_schema_registry_test_messages(num_valid=10, schema_id=42)
    mapper_sr = BronzeRecordMapper(input_format="AVRO_SCHEMA_REGISTRY", exclusion_filter=exclusion_filter)
    bronze_3, dlq_3, _ = _run_mapper_on_messages(mapper_sr, avro_sr_msgs)
    assert len(bronze_3) == 10
    assert len(dlq_3) == 1

    # 4. SCHEMA_MAPPED_STRUCT
    struct_msgs = generate_schema_mapped_struct_test_messages(num_valid=10)
    mapper_struct = BronzeRecordMapper(input_format="SCHEMA_MAPPED_STRUCT", exclusion_filter=exclusion_filter)
    bronze_4, dlq_4, _ = _run_mapper_on_messages(mapper_struct, struct_msgs)
    assert len(bronze_4) == 10
    assert len(dlq_4) == 0

    # 5. GMK vs External Kafka Spark Reader Options
    boot_gmk, topic_gmk, type_gmk = KafkaEndpointResolver.resolve_endpoints(
        f"projects/{PROJECT_ID}/locations/us-central1/clusters/sample-kafka-target-cluster/topics/fab.equipment.telemetry.v1"
    )
    assert type_gmk == "GMK"
    assert boot_gmk == f"bootstrap.sample-kafka-target-cluster.us-central1.managedkafka.{PROJECT_ID}.cloud.goog:9092"
    assert topic_gmk == "fab.equipment.telemetry.v1"

    gmk_opts = KafkaSparkConfigBuilder.build_reader_options(
        bootstrap_servers=boot_gmk,
        topic=topic_gmk,
        source_type="GMK",
        pam_creds={},
    )
    assert gmk_opts["kafka.sasl.mechanism"] == "OAUTHBEARER"

    ext_opts = KafkaSparkConfigBuilder.build_reader_options(
        bootstrap_servers="10.20.0.50:9093",
        topic="fab.equipment.telemetry.v1",
        source_type="EXTERNAL",
        pam_creds={
            "auth_mode": "SASL_SCRAM_512",
            "security_protocol": "SASL_SSL",
            "username": "fab_reader",
            "password": "test_password_not_real",
            "truststore_gcs_uri": f"gs://{BUCKET_NAME}/certs/truststore.jks",
        },
    )
    assert ext_opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert "ScramLoginModule" in ext_opts["kafka.sasl.jaas.config"]
    logger.info("ALL 4 INPUT FORMATS & GMK/EXTERNAL CONFIGS PASSED 100%% SUCCESSFULLY!")


if __name__ == "__main__":
    run_verification()
