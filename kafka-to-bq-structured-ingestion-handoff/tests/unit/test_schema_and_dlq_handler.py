"""
Unit tests for Avro binary (.avsc) encoding/decoding, Confluent / GMK 5-byte
Wire-Format header unpacking, ExclusionFilter pattern/exact matching, and DLQ record building.
"""

import json
import unittest

from src.utils.schema_and_dlq_handler import (
    DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
    AvroPayloadCodec,
    BronzeRecordMapper,
    DLQRecordBuilder,
    ExclusionFilter,
)


class TestSchemaAndDLQHandler(unittest.TestCase):
    """Tests for AvroPayloadCodec, ExclusionFilter, DLQRecordBuilder, and BronzeRecordMapper."""

    def setUp(self) -> None:
        self.codec = AvroPayloadCodec(DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA)
        self.sample_record = {
            "event_id": "EVT-UNIT-001",
            "equipment_id": "FAB34-LITH-01",
            "fab_site_code": "Leixlip",
            "wafer_lot_id": "LOT-999",
            "metric_name": "chamber_pressure",
            "metric_value": 101.325,
            "status_code": "NOMINAL",
            "event_timestamp": "2026-10-12T12:00:00Z",
            "source_file": "metrology/scan_001.avro",
            "payload_ext_json": json.dumps(
                {"chamber_id": "CH-1", "metric_unit": "kPa"}
            ),
        }

    def test_binary_avro_roundtrip(self) -> None:
        """Verify Plain Binary Avro (.avsc) serialization and deserialization roundtrip."""
        encoded = self.codec.encode_binary_avro(self.sample_record)
        self.assertIsInstance(encoded, bytes)
        self.assertGreater(len(encoded), 10)

        decoded = self.codec.decode_binary_avro(encoded)
        self.assertEqual(decoded["event_id"], "EVT-UNIT-001")
        self.assertEqual(decoded["equipment_id"], "FAB34-LITH-01")
        self.assertAlmostEqual(decoded["metric_value"], 101.325, places=3)

    def test_confluent_wire_format_roundtrip(self) -> None:
        """Verify 5-byte Confluent/GMK Schema Registry Wire Format (0x00 + 4-byte ID)."""
        wire_bytes = self.codec.encode_wire_format_avro(
            self.sample_record, schema_id=105
        )
        self.assertEqual(wire_bytes[0], 0)

        schema_id, decoded = self.codec.decode_wire_format_avro(wire_bytes)
        self.assertEqual(schema_id, 105)
        self.assertEqual(decoded["event_id"], "EVT-UNIT-001")
        self.assertEqual(decoded["fab_site_code"], "Leixlip")

    def test_confluent_wire_format_rejects_bad_magic_byte(self) -> None:
        """Verify that non-0x00 magic byte raises ValueError for DLQ routing."""
        bad_bytes = b"\x01\x00\x00\x00\x05payload"
        with self.assertRaises(ValueError):
            self.codec.unpack_confluent_wire_header(bad_bytes)

    def test_dlq_record_builder_binary_vs_utf8(self) -> None:
        """Verify DLQRecordBuilder sets payloadString=None for binary failures and preserves payloadBytes."""
        corrupt_binary = b"\x80\xfe\xff\x00\x01corrupt"
        dlq_rec = DLQRecordBuilder.build_dlq_record(
            raw_value_bytes=corrupt_binary,
            topic="fab.equipment.telemetry.avro.v1",
            partition=2,
            offset=9876,
            error_message="AvroDecodeError: Invalid union branch",
        )
        self.assertIsNone(dlq_rec["payloadString"])
        self.assertEqual(dlq_rec["payloadBytes"], corrupt_binary)
        attr_map = {a["key"]: a["value"] for a in dlq_rec["attributes"]}
        self.assertEqual(attr_map["topic"], "fab.equipment.telemetry.avro.v1")
        self.assertEqual(attr_map["partition"], "2")
        self.assertEqual(attr_map["offset"], "9876")
        self.assertEqual(attr_map["input_format"], "JSON")

    def test_exclusion_filter_patterns_and_exact(self) -> None:
        """Verify wildcard/glob patterns and exact matches filter accurately."""
        ef = ExclusionFilter.from_strings(
            patterns_str="*.tmp, *.bak, ~$*, *_debug_*",
            exact_str="desktop.ini, thumbs.db, TEST_TOOL_IGNORE",
        )
        self.assertTrue(ef.is_excluded("cleanroom/dump.tmp")[0])
        self.assertTrue(ef.is_excluded("cleanroom/~$lock.xlsx")[0])
        self.assertTrue(ef.is_excluded("cleanroom/desktop.ini")[0])
        self.assertTrue(ef.is_excluded("cleanroom/valid.json", "TEST_TOOL_IGNORE")[0])
        self.assertFalse(
            ef.is_excluded("cleanroom/telemetry_01.json", "EUV-LITH-01")[0]
        )

    def test_message_based_json_envelope_extraction(self) -> None:
        """Verify Kafka Connect Message-Based JSON ('schema' + 'payload') is unwrapped."""
        mapper = BronzeRecordMapper(input_format="JSON")
        embedded_msg = json.dumps(
            {
                "schema": {"type": "struct", "fields": []},
                "payload": {
                    "event_id": "EVT-MSG-SCHEMA-01",
                    "equipment_id": "TOOL-99",
                    "metric_value": 55.5,
                },
            }
        ).encode("utf-8")
        status, bronze_row, dlq_row, _ = mapper.process_kafka_record(
            raw_key=b"KEY-99",
            raw_value=embedded_msg,
            topic="fab.equipment.telemetry.v1",
            partition=0,
            offset=10,
        )
        self.assertEqual(status, "SUCCESS")
        self.assertIsNone(dlq_row)
        self.assertIsNotNone(bronze_row)
        self.assertEqual(bronze_row["event_id"], "EVT-MSG-SCHEMA-01")
        self.assertEqual(bronze_row["kafka_key"]["key"], "KEY-99")

    def test_declarative_schema_mapped_struct_mapping(self) -> None:
        """Verify SCHEMA_MAPPED_STRUCT applies declarative field_mappings, defaults, and required_source_fields."""
        custom_spec = {
            "required_source_fields": ["eventId", "toolId"],
            "field_mappings": {
                "eventId": "event_id",
                "toolId": "equipment_id",
                "fabLocation": "fab_site_code",
                "readingValue": "metric_value",
            },
            "defaults": {"status_code": "NOMINAL"},
            "type_casts": {"metric_value": "FLOAT64"},
        }
        mapper = BronzeRecordMapper(
            input_format="SCHEMA_MAPPED_STRUCT",
            schema_mapping_spec=custom_spec,
        )
        valid_payload = json.dumps(
            {
                "eventId": "EVT-MAP-100",
                "toolId": "EUV-SCANNER-07",
                "fabLocation": "D1X",
                "readingValue": "42.75",
                "chamber_id": "CH-B",
            }
        ).encode("utf-8")
        status, bronze_row, dlq_row, _ = mapper.process_kafka_record(
            raw_key=b"KEY-MAP-100",
            raw_value=valid_payload,
            topic="fab.cleanroom.metrology.struct.v1",
            partition=1,
            offset=55,
        )
        self.assertEqual(status, "SUCCESS")
        self.assertIsNone(dlq_row)
        self.assertEqual(bronze_row["event_id"], "EVT-MAP-100")
        self.assertEqual(bronze_row["equipment_id"], "EUV-SCANNER-07")
        self.assertEqual(bronze_row["fab_site_code"], "D1X")
        self.assertAlmostEqual(bronze_row["metric_value"], 42.75)

        # Missing required field 'toolId' must route to DLQ
        invalid_payload = json.dumps({"eventId": "EVT-MAP-101"}).encode("utf-8")
        bad_status, bad_bronze, bad_dlq, _ = mapper.process_kafka_record(
            raw_key=b"KEY-MAP-101",
            raw_value=invalid_payload,
            topic="fab.cleanroom.metrology.struct.v1",
            partition=1,
            offset=56,
        )
        self.assertEqual(bad_status, "DLQ")
        self.assertIsNone(bad_bronze)
        self.assertIsNotNone(bad_dlq)
        self.assertIn("toolId", bad_dlq["errorMessage"])


if __name__ == "__main__":
    unittest.main()
