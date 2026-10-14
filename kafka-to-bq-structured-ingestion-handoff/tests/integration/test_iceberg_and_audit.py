#!/usr/bin/env python3
"""
Integration Test Suite: Validates Multi-Format Batch Export Roundtrip across All 4 Formats.
"""

import json
import tempfile
import unittest

from src.utils.schema_and_dlq_handler import BronzeRecordMapper, ExclusionFilter
from test_setup.generate_enterprise_test_data import export_test_batches_to_dir
from tests.integration.test_dlq_and_deduplication import _run_mapper_on_messages


class TestIcebergAndAuditIntegration(unittest.TestCase):
    """Validates multi-format test batch generation and roundtrip decoding."""

    def test_01_exported_json_batches_all_four_formats_roundtrip(self) -> None:
        """Verify that export_test_batches_to_dir exports all 4 formats and hex-encoded Avro batches decode cleanly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            exported = export_test_batches_to_dir(tmp_dir)
            self.assertEqual(
                set(exported.keys()),
                {"JSON", "AVRO_BINARY", "AVRO_SCHEMA_REGISTRY", "SCHEMA_MAPPED_STRUCT"},
            )
            for fmt in ("AVRO_BINARY", "AVRO_SCHEMA_REGISTRY"):
                with open(exported[fmt], "r", encoding="utf-8") as fh:
                    loaded_msgs = json.load(fh)
                mapper = BronzeRecordMapper(
                    input_format=fmt,
                    exclusion_filter=ExclusionFilter.from_strings(
                        "*.tmp,*.bak", "desktop.ini"
                    ),
                )
                bronze_rows, dlq_rows, _ = _run_mapper_on_messages(mapper, loaded_msgs)
                self.assertEqual(len(bronze_rows), 10)
                self.assertEqual(len(dlq_rows), 1)


if __name__ == "__main__":
    unittest.main()
