#!/usr/bin/env python3
"""
Interactive & Automated Live Demo Scenario Generator.

Simulates 5 realistic Intel Fab Telemetry streaming scenarios across GMK and External Kafka:
- Scenario 1: JSON Telemetry Stream with Extensible JSON Attributes (chamber_id, metric_unit)
- Scenario 2: Plain Binary Avro (.avsc) & Confluent/GMK Schema Registry 5-Byte Wire Format
- Scenario 3: Wildcard (*.tmp, *_debug_*) & Exact Match (desktop.ini, TEST_TOOL_IGNORE) Exclusions
- Scenario 4: Corrupt Binary/JSON Payload Quarantine into BigQuery DLQ (fab_telemetry_events_dlq)
- Scenario 5: Producer Retry Duplicates & Downstream QUALIFY ROW_NUMBER() Deduplication
"""

import argparse
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

from test_setup.generate_enterprise_test_data import (  # noqa: E402
    generate_avro_binary_test_messages,
    generate_avro_schema_registry_test_messages,
    generate_json_test_messages,
    generate_schema_mapped_struct_test_messages,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("prepare_demo_scenarios")


def prepare_all_demo_scenarios() -> Dict[str, int]:
    """Generates and summarizes the 4 format streams and 5 demo scenarios."""
    json_batch = generate_json_test_messages(num_valid=25)
    avro_bin_batch = generate_avro_binary_test_messages(num_valid=15)
    avro_sr_batch = generate_avro_schema_registry_test_messages(num_valid=15)
    struct_batch = generate_schema_mapped_struct_test_messages(num_valid=15)

    summary = {
        "JSON_TOTAL": len(json_batch),
        "AVRO_BINARY_TOTAL": len(avro_bin_batch),
        "AVRO_SCHEMA_REGISTRY_TOTAL": len(avro_sr_batch),
        "SCHEMA_MAPPED_STRUCT_TOTAL": len(struct_batch),
    }
    logger.info("Prepared Demo Scenarios Summary: %s", summary)
    return summary


def main() -> None:
    """CLI entrypoint for preparing live demo scenarios."""
    parser = argparse.ArgumentParser(
        description="Prepare Kafka to BigQuery Iceberg Live Demo Scenarios"
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID", ""),
        help="GCP Project ID",
    )
    parser.parse_args()
    prepare_all_demo_scenarios()


if __name__ == "__main__":
    main()
