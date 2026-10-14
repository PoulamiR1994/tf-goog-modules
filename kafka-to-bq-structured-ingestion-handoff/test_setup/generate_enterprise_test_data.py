#!/usr/bin/env python3
"""
Enterprise Kafka Test Data Generator across All 4 Input Payload Formats:
1. JSON (Plain JSON & Message-Based JSON with extensible attributes)
2. AVRO_BINARY (Plain Binary Avro encoded with fab_telemetry_event.avsc)
3. AVRO_SCHEMA_REGISTRY (Confluent / GMK 5-Byte Wire Format: 0x00 + 4-byte Schema ID + Avro)
4. SCHEMA_MAPPED_STRUCT (Nested equipment/telemetry struct payloads)

Also injects:
- Excluded records matching wildcard patterns (*.tmp, *_debug_*, *heartbeat*) and
  exact match rules (desktop.ini, TEST_TOOL_IGNORE)
- Producer retry duplicate records (same event_id across multiple offsets)
- Corrupted/malformed records (truncated Avro, bad magic byte 0xFF, malformed JSON,
  invalid FLOAT64 metric_value) to validate Dead-Letter Queue (DLQ) quarantine.
"""

import argparse
import base64
import datetime
import json
import logging
import os
import sys
from typing import Any, Dict, List

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
_SRC_ROOT = os.path.join(_REPO_ROOT, "src")
for _p in (_CURRENT_DIR, _REPO_ROOT, _SRC_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from utils.schema_and_dlq_handler import (  # noqa: E402
        DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
        AvroPayloadCodec,
    )
except ImportError:
    from schema_and_dlq_handler import (  # type: ignore[no-redef] # noqa: E402
        DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA,
        AvroPayloadCodec,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("generate_enterprise_test_data")

FAB_SITES = ("D1X", "Ocotillo", "Leixlip", "Fab34")
METRICS = ("chamber_pressure_mbar", "rf_power_watts", "wafer_temp_c", "laser_dose_mj")


def generate_json_test_messages(
    topic: str = "fab.equipment.telemetry.json.v1",
    num_valid: int = 20,
) -> List[Dict[str, Any]]:
    """Generates JSON Kafka test messages including valid, duplicate, excluded, and corrupt payloads."""
    messages: List[Dict[str, Any]] = []
    base_ts = datetime.datetime(2026, 10, 12, 10, 0, 0, tzinfo=datetime.timezone.utc)

    for i in range(1, num_valid + 1):
        event_ts = (base_ts + datetime.timedelta(seconds=i * 5)).isoformat()
        payload = {
            "event_id": f"EVT-JSON-{i:04d}",
            "equipment_id": f"EUV-LITH-{(i % 5) + 1:02d}",
            "fab_site_code": FAB_SITES[i % len(FAB_SITES)],
            "wafer_lot_id": f"LOT-2026-{(i % 10) + 100}",
            "metric_name": METRICS[i % len(METRICS)],
            "metric_value": round(100.5 + (i * 1.25), 3),
            "status_code": "NOMINAL",
            "event_timestamp": event_ts,
            "source_file": f"cleanroom/lot_{i:04d}/sensor_readout.json",
            "chamber_id": f"CH-{(i % 3) + 1}",
            "metric_unit": "mbar",
        }
        messages.append(
            {
                "key": f"KEY-JSON-{i:04d}",
                "value": json.dumps(payload),
                "topic": topic,
                "partition": i % 3,
                "offset": i,
                "timestamp": event_ts,
                "headers": {"source_file": payload["source_file"]},
            }
        )

    # Add 1 duplicate retry of EVT-JSON-0001 at a higher offset to test deduplication
    dup_ts = (base_ts + datetime.timedelta(minutes=10)).isoformat()
    dup_payload = {
        "event_id": "EVT-JSON-0001",
        "equipment_id": "EUV-LITH-02",
        "fab_site_code": "Ocotillo",
        "wafer_lot_id": "LOT-2026-101",
        "metric_name": "rf_power_watts",
        "metric_value": 999.99,
        "status_code": "RETRY_UPDATED",
        "event_timestamp": dup_ts,
        "source_file": "cleanroom/lot_0001/sensor_readout.json",
    }
    messages.append(
        {
            "key": "KEY-JSON-0001",
            "value": json.dumps(dup_payload),
            "topic": topic,
            "partition": 1,
            "offset": num_valid + 1,
            "timestamp": dup_ts,
            "headers": {"source_file": dup_payload["source_file"]},
        }
    )

    # Add 2 Excluded messages (1 wildcard *.tmp, 1 exact match desktop.ini)
    messages.append(
        {
            "key": "KEY-EXCL-TMP",
            "value": json.dumps(
                {
                    "event_id": "EVT-EXCL-01",
                    "equipment_id": "EUV-LITH-01",
                    "source_file": "scratch/calibration_dump.tmp",
                }
            ),
            "topic": topic,
            "partition": 0,
            "offset": num_valid + 2,
            "timestamp": dup_ts,
            "headers": {"source_file": "scratch/calibration_dump.tmp"},
        }
    )
    messages.append(
        {
            "key": "KEY-EXCL-EXACT",
            "value": json.dumps(
                {
                    "event_id": "EVT-EXCL-02",
                    "equipment_id": "TEST_TOOL_IGNORE",
                    "source_file": "desktop.ini",
                }
            ),
            "topic": topic,
            "partition": 0,
            "offset": num_valid + 3,
            "timestamp": dup_ts,
            "headers": {"source_file": "desktop.ini"},
        }
    )

    # Add 2 Corrupt/DLQ messages (1 malformed JSON syntax, 1 invalid FLOAT64 type mismatch)
    messages.append(
        {
            "key": "KEY-CORRUPT-JSON",
            "value": "{corrupt_json_missing_quotes: true, unclosed",
            "topic": topic,
            "partition": 2,
            "offset": num_valid + 4,
            "timestamp": dup_ts,
            "headers": {"source_file": "cleanroom/corrupt_packet.json"},
        }
    )
    messages.append(
        {
            "key": "KEY-TYPE-MISMATCH",
            "value": json.dumps(
                {
                    "event_id": "EVT-BAD-FLOAT",
                    "equipment_id": "EUV-LITH-03",
                    "metric_name": "rf_power_watts",
                    "metric_value": "NOT_A_NUMBER_FLOAT",
                }
            ),
            "topic": topic,
            "partition": 2,
            "offset": num_valid + 5,
            "timestamp": dup_ts,
            "headers": {},
        }
    )
    return messages


def generate_avro_binary_test_messages(
    topic: str = "fab.equipment.telemetry.avro.binary.v1",
    num_valid: int = 10,
) -> List[Dict[str, Any]]:
    """Generates Plain Binary Avro (.avsc) test messages plus corrupt binary DLQ payload."""
    codec = AvroPayloadCodec(DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA)
    messages: List[Dict[str, Any]] = []
    base_ts = datetime.datetime(2026, 10, 12, 11, 0, 0, tzinfo=datetime.timezone.utc)

    for i in range(1, num_valid + 1):
        event_ts = (base_ts + datetime.timedelta(seconds=i * 10)).isoformat()
        record = {
            "event_id": f"EVT-AVRO-BIN-{i:04d}",
            "equipment_id": f"CVD-CHAMBER-{(i % 4) + 1:02d}",
            "fab_site_code": FAB_SITES[i % len(FAB_SITES)],
            "wafer_lot_id": f"LOT-AVRO-{200 + i}",
            "metric_name": METRICS[i % len(METRICS)],
            "metric_value": float(250.0 + i),
            "status_code": "ACTIVE",
            "event_timestamp": event_ts,
            "source_file": f"metrology/avro_bin_{i:04d}.avro",
            "payload_ext_json": json.dumps({"chamber_id": "CH-B1", "metric_unit": "C"}),
        }
        avro_bytes = codec.encode_binary_avro(record)
        messages.append(
            {
                "key": f"KEY-AVRO-BIN-{i:04d}".encode("utf-8"),
                "value": avro_bytes,
                "topic": topic,
                "partition": i % 2,
                "offset": i,
                "timestamp": event_ts,
                "headers": {"source_file": record["source_file"]},
            }
        )

    # Add 1 Excluded record (*.bak pattern)
    excl_rec = {
        "event_id": "EVT-AVRO-EXCL",
        "equipment_id": "CVD-CHAMBER-01",
        "fab_site_code": "D1X",
        "wafer_lot_id": "LOT-000",
        "metric_name": "temp",
        "metric_value": 10.0,
        "status_code": "SKIP",
        "event_timestamp": base_ts.isoformat(),
        "source_file": "metrology/backup_trace.bak",
        "payload_ext_json": "{}",
    }
    messages.append(
        {
            "key": b"KEY-AVRO-EXCL",
            "value": codec.encode_binary_avro(excl_rec),
            "topic": topic,
            "partition": 0,
            "offset": num_valid + 1,
            "timestamp": base_ts.isoformat(),
            "headers": {"source_file": "metrology/backup_trace.bak"},
        }
    )

    # Add 1 Corrupted binary Avro payload (invalid union branch / truncated bytes) -> DLQ
    messages.append(
        {
            "key": b"KEY-AVRO-CORRUPT",
            "value": b"\x02\xff\xff\xff\xfa\x00\x01corrupt_avro_bytes",
            "topic": topic,
            "partition": 1,
            "offset": num_valid + 2,
            "timestamp": base_ts.isoformat(),
            "headers": {"source_file": "metrology/corrupt_stream.avro"},
        }
    )
    return messages


def generate_avro_schema_registry_test_messages(
    topic: str = "fab.equipment.telemetry.avro.sr.v1",
    schema_id: int = 42,
    num_valid: int = 10,
) -> List[Dict[str, Any]]:
    """Generates Confluent / GMK Schema Registry 5-Byte Wire Format Avro messages + DLQ test."""
    codec = AvroPayloadCodec(DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA)
    messages: List[Dict[str, Any]] = []
    base_ts = datetime.datetime(2026, 10, 12, 12, 0, 0, tzinfo=datetime.timezone.utc)

    for i in range(1, num_valid + 1):
        event_ts = (base_ts + datetime.timedelta(seconds=i * 10)).isoformat()
        record = {
            "event_id": f"EVT-AVRO-SR-{i:04d}",
            "equipment_id": f"ETCH-TOOL-{(i % 4) + 1:02d}",
            "fab_site_code": FAB_SITES[i % len(FAB_SITES)],
            "wafer_lot_id": f"LOT-SR-{300 + i}",
            "metric_name": METRICS[i % len(METRICS)],
            "metric_value": float(500.0 + i * 2.5),
            "status_code": "NOMINAL",
            "event_timestamp": event_ts,
            "source_file": f"sr_stream/batch_{i:04d}.avro",
            "payload_ext_json": json.dumps(
                {"chamber_id": "CH-SR1", "metric_unit": "W", "schema_id": schema_id}
            ),
        }
        wire_bytes = codec.encode_wire_format_avro(record, schema_id=schema_id)
        messages.append(
            {
                "key": f"KEY-AVRO-SR-{i:04d}".encode("utf-8"),
                "value": wire_bytes,
                "topic": topic,
                "partition": i % 2,
                "offset": i,
                "timestamp": event_ts,
                "headers": {"source_file": record["source_file"]},
            }
        )

    # Add 1 Corrupted Wire Format message (invalid Magic Byte 0x7F instead of 0x00) -> DLQ
    messages.append(
        {
            "key": b"KEY-SR-BAD-MAGIC",
            "value": b"\x7f\x00\x00\x00\x2a\x02\x08bad_magic_byte",
            "topic": topic,
            "partition": 1,
            "offset": num_valid + 1,
            "timestamp": base_ts.isoformat(),
            "headers": {"source_file": "sr_stream/bad_magic.avro"},
        }
    )
    return messages


def generate_schema_mapped_struct_test_messages(
    topic: str = "fab.cleanroom.metrology.struct.v1",
    num_valid: int = 10,
) -> List[Dict[str, Any]]:
    """Generates nested Schema-Mapped Struct messages for testing nested struct flattening."""
    messages: List[Dict[str, Any]] = []
    base_ts = datetime.datetime(2026, 10, 12, 13, 0, 0, tzinfo=datetime.timezone.utc)

    for i in range(1, num_valid + 1):
        event_ts = (base_ts + datetime.timedelta(seconds=i * 15)).isoformat()
        nested_payload = {
            "event_id": f"EVT-STRUCT-{i:04d}",
            "device": {
                "equipment_id": f"METRO-SEM-{(i % 3) + 1:02d}",
                "fab_site_code": FAB_SITES[i % len(FAB_SITES)],
                "chamber_id": f"SEM-CH-{i % 2}",
            },
            "telemetry": {
                "wafer_lot_id": f"LOT-STRUCT-{400 + i}",
                "metric_name": METRICS[i % len(METRICS)],
                "metric_value": round(42.0 + i * 0.75, 2),
                "status_code": "CALIBRATED",
                "event_timestamp": event_ts,
                "metric_unit": "nm",
            },
            "source_file": f"struct_feed/sem_scan_{i:04d}.json",
        }
        messages.append(
            {
                "key": f"KEY-STRUCT-{i:04d}",
                "value": json.dumps(nested_payload),
                "topic": topic,
                "partition": i % 2,
                "offset": i,
                "timestamp": event_ts,
                "headers": {"source_file": nested_payload["source_file"]},
            }
        )
    return messages


def _serialize_messages_for_json_export(
    messages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts any bytes key/value fields into hex-encoded strings with *_is_hex flags for JSON file staging."""
    serialized: List[Dict[str, Any]] = []
    for msg in messages:
        item = dict(msg)
        if isinstance(item.get("key"), (bytes, bytearray)):
            item["key"] = bytes(item["key"]).hex()
            item["key_is_hex"] = True
        if isinstance(item.get("value"), (bytes, bytearray)):
            item["value"] = bytes(item["value"]).hex()
            item["value_is_hex"] = True
        serialized.append(item)
    return serialized


def export_test_batches_to_dir(output_dir: str) -> Dict[str, str]:
    """Writes generated test batches for all 4 input formats to local JSON files for testing and GCS staging."""
    os.makedirs(output_dir, exist_ok=True)
    paths: Dict[str, str] = {}

    format_batches = {
        "JSON": ("batch_json.json", generate_json_test_messages()),
        "AVRO_BINARY": (
            "batch_avro_binary.json",
            generate_avro_binary_test_messages(),
        ),
        "AVRO_SCHEMA_REGISTRY": (
            "batch_avro_schema_registry.json",
            generate_avro_schema_registry_test_messages(),
        ),
        "SCHEMA_MAPPED_STRUCT": (
            "batch_schema_mapped_struct.json",
            generate_schema_mapped_struct_test_messages(),
        ),
    }

    for fmt_name, (filename, raw_msgs) in format_batches.items():
        out_path = os.path.join(output_dir, filename)
        serializable_msgs = _serialize_messages_for_json_export(raw_msgs)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(serializable_msgs, f, indent=2)
        paths[fmt_name] = out_path

    logger.info(
        "Exported sample test batches to %s: %s", output_dir, list(paths.keys())
    )
    return paths


def main() -> None:
    """CLI entrypoint for generating enterprise Kafka test data across all 4 formats."""
    parser = argparse.ArgumentParser(
        description="Generate Enterprise Kafka Test Data for All 4 Input Formats"
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(_CURRENT_DIR, "sample_batches"),
        help="Directory to write sample batch files",
    )
    args = parser.parse_args()
    export_test_batches_to_dir(args.output_dir)


if __name__ == "__main__":
    main()
