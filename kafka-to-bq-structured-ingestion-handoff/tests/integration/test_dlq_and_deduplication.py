#!/usr/bin/env python3
"""
Integration Test Suite: Validates Multi-Format Decoding (JSON, AVRO_BINARY,
AVRO_SCHEMA_REGISTRY, SCHEMA_MAPPED_STRUCT), Dead-Letter Queue (DLQ) Quarantine,
and Exclusion Filtering via BronzeRecordMapper.
"""

import unittest

from src.utils.schema_and_dlq_handler import BronzeRecordMapper, ExclusionFilter
from test_setup.generate_enterprise_test_data import (
    generate_avro_binary_test_messages,
    generate_avro_schema_registry_test_messages,
    generate_json_test_messages,
    generate_schema_mapped_struct_test_messages,
)


def _run_mapper_on_messages(
    mapper: BronzeRecordMapper, messages: list
) -> tuple[list, list, list]:
    bronze_rows = []
    dlq_rows = []
    excluded_rows = []
    for msg in messages:
        raw_key = (
            bytes.fromhex(msg["key"])
            if msg.get("key_is_hex") and isinstance(msg.get("key"), str)
            else (
                msg["key"].encode("utf-8")
                if isinstance(msg.get("key"), str)
                else msg.get("key")
            )
        )
        raw_val = (
            bytes.fromhex(msg["value"])
            if msg.get("value_is_hex") and isinstance(msg.get("value"), str)
            else (
                msg["value"].encode("utf-8")
                if isinstance(msg.get("value"), str)
                else msg.get("value")
            )
        )
        status, bronze_row, dlq_row, _ = mapper.process_kafka_record(
            raw_key=raw_key,
            raw_value=raw_val,
            topic=str(msg.get("topic") or "test.topic"),
            partition=int(msg.get("partition") or 0),
            offset=int(msg.get("offset") or 0),
            headers=msg.get("headers") or {},
        )
        if status == "SUCCESS" and bronze_row:
            bronze_rows.append(bronze_row)
        elif status == "DLQ" and dlq_row:
            dlq_rows.append(dlq_row)
        elif status == "EXCLUDED":
            excluded_rows.append(msg)
    return bronze_rows, dlq_rows, excluded_rows


class TestDLQAndDeduplicationIntegration(unittest.TestCase):
    """Validates multi-format decoding, exclusion filtering, and DLQ routing across all 4 input formats."""

    def setUp(self) -> None:
        self.exclusion_filter = ExclusionFilter.from_strings(
            patterns_str="*.tmp,*.bak,~$*,*_debug_*",
            exact_str="desktop.ini,thumbs.db,TEST_TOOL_IGNORE",
        )

    def test_01_json_pipeline_execution(self) -> None:
        """Verify JSON decoding, exclusions, and DLQ isolation."""
        messages = generate_json_test_messages(num_valid=15)
        mapper = BronzeRecordMapper(
            input_format="JSON", exclusion_filter=self.exclusion_filter
        )
        bronze_rows, dlq_rows, excluded_rows = _run_mapper_on_messages(
            mapper, messages
        )
        self.assertEqual(len(bronze_rows), 16)
        self.assertEqual(len(dlq_rows), 2)
        self.assertEqual(len(excluded_rows), 2)

    def test_02_avro_binary_pipeline_execution(self) -> None:
        """Verify AVRO_BINARY (.avsc) binary decoding and binary DLQ."""
        messages = generate_avro_binary_test_messages(num_valid=12)
        mapper = BronzeRecordMapper(
            input_format="AVRO_BINARY", exclusion_filter=self.exclusion_filter
        )
        bronze_rows, dlq_rows, excluded_rows = _run_mapper_on_messages(
            mapper, messages
        )
        self.assertEqual(len(bronze_rows), 12)
        self.assertEqual(len(dlq_rows), 1)
        self.assertIsNone(dlq_rows[0]["payloadString"])
        self.assertGreater(len(dlq_rows[0]["payloadBytes"]), 0)
        self.assertEqual(len(excluded_rows), 1)

    def test_03_avro_schema_registry_wire_format_execution(self) -> None:
        """Verify AVRO_SCHEMA_REGISTRY (5-byte Confluent/GMK Wire Format)."""
        messages = generate_avro_schema_registry_test_messages(
            num_valid=10, schema_id=77
        )
        mapper = BronzeRecordMapper(
            input_format="AVRO_SCHEMA_REGISTRY", exclusion_filter=self.exclusion_filter
        )
        bronze_rows, dlq_rows, _ = _run_mapper_on_messages(mapper, messages)
        self.assertEqual(len(bronze_rows), 10)
        self.assertEqual(len(dlq_rows), 1)
        self.assertIn("magic byte", dlq_rows[0]["errorMessage"].lower())

    def test_04_schema_mapped_struct_execution_external_kafka(self) -> None:
        """Verify SCHEMA_MAPPED_STRUCT nested struct extraction."""
        messages = generate_schema_mapped_struct_test_messages(num_valid=8)
        mapper = BronzeRecordMapper(
            input_format="SCHEMA_MAPPED_STRUCT", exclusion_filter=self.exclusion_filter
        )
        bronze_rows, dlq_rows, _ = _run_mapper_on_messages(mapper, messages)
        self.assertEqual(len(bronze_rows), 8)
        self.assertEqual(len(dlq_rows), 0)
        self.assertIn("chamber_id", bronze_rows[0]["payload_ext_json"])


if __name__ == "__main__":
    unittest.main()
