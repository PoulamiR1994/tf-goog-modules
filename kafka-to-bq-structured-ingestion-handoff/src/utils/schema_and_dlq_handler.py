#!/usr/bin/env python3
"""
Reusable Schema Definitions, Exclusion Filtering, and Dead-Letter Queue (DLQ) Utilities.

Supports all 4 Kafka payload formats:
1. JSON: Plain JSON and Message-Based JSON (with embedded schema/payload envelope).
2. AVRO_BINARY: Plain Binary Avro decoded against an explicit .avsc schema.
3. AVRO_SCHEMA_REGISTRY: Confluent / Google Cloud Managed Schema Registry Wire Format
   (5-byte prefix: 0x00 magic byte + 4-byte big-endian schema ID + binary Avro payload).
4. SCHEMA_MAPPED_STRUCT: Nested struct & declarative field-mapped JSON payloads.

Also provides:
- ExclusionFilter: Wildcard/glob pattern and exact-match filtering across message keys,
  headers, source_file metadata, equipment_id, and metric_name.
- DLQRecordBuilder: Dead-Letter Queue record builder for `fab_telemetry_events_dlq`.
"""

import datetime
import fnmatch
import io
import json
import logging
import struct
import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("schema_and_dlq_handler")

CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_HEADER_SIZE = 5
MAX_BQ_ROW_SIZE_BYTES = 20 * 1024 * 1024  # 20 MB BigQuery Storage Write API limit
GMK_DEFAULT_MAX_MESSAGE_BYTES = 10 * 1024 * 1024  # 10 MB GMK default cap

# Default Avro Schema (.avsc) for Intel Fab Telemetry Bronze Events
DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "FabTelemetryEvent",
    "namespace": "com.intel.manufacturing.fab.telemetry",
    "doc": "Structured Fab cleanroom equipment & silicon metrology telemetry event.",
    "fields": [
        {"name": "event_id", "type": ["null", "string"], "default": None},
        {"name": "equipment_id", "type": ["null", "string"], "default": None},
        {"name": "fab_site_code", "type": ["null", "string"], "default": None},
        {"name": "wafer_lot_id", "type": ["null", "string"], "default": None},
        {"name": "metric_name", "type": ["null", "string"], "default": None},
        {"name": "metric_value", "type": ["null", "double"], "default": None},
        {"name": "status_code", "type": ["null", "string"], "default": None},
        {"name": "event_timestamp", "type": ["null", "string"], "default": None},
        {"name": "source_file", "type": ["null", "string"], "default": None},
        {"name": "payload_ext_json", "type": ["null", "string"], "default": None},
    ],
}

# Default Declarative Field Mapping Specification for SCHEMA_MAPPED_STRUCT
DEFAULT_SCHEMA_MAPPING_SPEC: Dict[str, Any] = {
    "required_source_fields": [],
    "field_mappings": {
        "evtId": "event_id",
        "eqpId": "equipment_id",
        "siteCode": "fab_site_code",
        "fab_site": "fab_site_code",
        "lotId": "wafer_lot_id",
        "paramName": "metric_name",
        "paramValue": "metric_value",
        "statusCode": "status_code",
        "evtTs": "event_timestamp",
        "srcFile": "source_file",
    },
    "type_casts": {
        "metric_value": "FLOAT64",
    },
    "defaults": {},
}

CORE_BRONZE_FIELDS: Tuple[str, ...] = (
    "event_id",
    "equipment_id",
    "fab_site_code",
    "wafer_lot_id",
    "metric_name",
    "metric_value",
    "status_code",
    "event_timestamp",
    "payload_ext_json",
)


def parse_iso_timestamp(ts_str: Optional[Any]) -> Optional[datetime.datetime]:
    """Parses an ISO-8601 timestamp string or epoch value into a UTC datetime."""
    if ts_str is None:
        return None
    if isinstance(ts_str, datetime.datetime):
        if ts_str.tzinfo is None:
            return ts_str.replace(tzinfo=datetime.timezone.utc)
        return ts_str.astimezone(datetime.timezone.utc)
    if isinstance(ts_str, (int, float)):
        # Support epoch milliseconds or seconds
        epoch_sec = ts_str / 1000.0 if ts_str > 1e11 else float(ts_str)
        return datetime.datetime.fromtimestamp(epoch_sec, tz=datetime.timezone.utc)

    clean = str(ts_str).strip()
    if not clean:
        return None
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except ValueError as ex:
        logger.warning("Could not parse ISO timestamp '%s': %s", ts_str, ex)
        return None


class ExclusionFilter:
    """
    Evaluates Kafka messages and source file/record identifiers against exclusion rules:
    1. Exact match list (e.g., 'desktop.ini', 'thumbs.db', 'TEST_TOOL_IGNORE')
    2. Wildcard/Glob pattern list (e.g., '*.tmp', '*.bak', '~$*', '*_debug_*', '*heartbeat*')
    """

    def __init__(
        self,
        exclude_patterns: Optional[List[str]] = None,
        exclude_files: Optional[List[str]] = None,
    ) -> None:
        self.patterns: List[str] = [
            p.strip() for p in (exclude_patterns or []) if p and p.strip()
        ]
        self.exact_matches: Set[str] = {
            m.strip().lower() for m in (exclude_files or []) if m and m.strip()
        }

    @classmethod
    def from_strings(
        cls,
        patterns_str: Optional[str] = None,
        exact_str: Optional[str] = None,
    ) -> "ExclusionFilter":
        """Constructs an ExclusionFilter from comma-separated configuration strings."""
        patterns = (
            [p.strip() for p in patterns_str.split(",") if p.strip()]
            if patterns_str
            else []
        )
        exact = (
            [e.strip() for e in exact_str.split(",") if e.strip()] if exact_str else []
        )
        return cls(exclude_patterns=patterns, exclude_files=exact)

    def is_excluded(
        self,
        candidate_path_or_key: str,
        candidate_name: str = "",
        additional_candidates: Optional[List[str]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Checks whether a record or file path matches any configured exact or glob rule.

        Returns:
            Tuple[bool, Optional[str]]: (is_excluded, matched_rule_description)
        """
        if not self.patterns and not self.exact_matches:
            return False, None

        candidates = self._collect_candidates(
            candidate_path_or_key, candidate_name, additional_candidates
        )
        exact_hit = self._check_exact_matches(candidates)
        if exact_hit:
            return True, exact_hit

        pattern_hit = self._check_pattern_matches(candidates)
        if pattern_hit:
            return True, pattern_hit

        return False, None

    @staticmethod
    def _collect_candidates(
        primary_path: str,
        item_name: str,
        extra: Optional[List[str]],
    ) -> List[str]:
        raw_items = [primary_path, item_name] + (extra or [])
        collected: List[str] = []
        for item in raw_items:
            if not item:
                continue
            cleaned = str(item).strip()
            if not cleaned:
                continue
            collected.append(cleaned)
            for part in cleaned.split("/"):
                if part and part not in collected:
                    collected.append(part)
        return collected

    def _check_exact_matches(self, candidates: List[str]) -> Optional[str]:
        for cand in candidates:
            if cand.lower() in self.exact_matches:
                return f"EXACT_MATCH:{cand}"
        return None

    def _check_pattern_matches(self, candidates: List[str]) -> Optional[str]:
        for pat in self.patterns:
            pat_lower = pat.lower()
            for cand in candidates:
                if fnmatch.fnmatchcase(cand.lower(), pat_lower):
                    return f"PATTERN_MATCH:{pat}"
        return None


class AvroPayloadCodec:
    """
    Encodes and decodes Plain Binary Avro (.avsc) and Confluent / GMK Schema Registry
    Wire-Format Avro (5-byte header: 0x00 + 4-byte Schema ID + binary payload).

    Uses `fastavro` when available in the runtime environment and provides a deterministic
    spec-compliant binary Avro record encoder/decoder fallback for standard nullable string/double
    schemas so unit and integration tests can execute anywhere without C extensions.
    """

    def __init__(
        self,
        avro_schema: Optional[Dict[str, Any]] = None,
        schema_registry_Lookup: Optional[Callable[[int], Dict[str, Any]]] = None,
    ) -> None:
        self.default_schema = avro_schema or DEFAULT_FAB_TELEMETRY_AVRO_SCHEMA
        self.schema_registry_lookup = schema_registry_Lookup
        self._schema_cache: Dict[int, Dict[str, Any]] = {}

    def resolve_schema(self, schema_id: Optional[int] = None) -> Dict[str, Any]:
        """Resolves an Avro schema by Schema Registry ID, falling back to default_schema."""
        if schema_id is None:
            return self.default_schema
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        if self.schema_registry_lookup is not None:
            resolved = self.schema_registry_lookup(schema_id)
            if resolved:
                self._schema_cache[schema_id] = resolved
                return resolved
        return self.default_schema

    @staticmethod
    def unpack_confluent_wire_header(raw_bytes: bytes) -> Tuple[int, bytes]:
        """
        Validates and strips the 5-byte Confluent / GMK Schema Registry Wire Format header.

        Format:
          - Byte 0: Magic byte (must be 0x00)
          - Bytes 1..4: 4-byte big-endian unsigned integer Schema ID
          - Bytes 5..N: Avro binary payload
        """
        if not raw_bytes or len(raw_bytes) <= CONFLUENT_HEADER_SIZE:
            raise ValueError(
                f"Payload too short for Confluent Wire Format ({len(raw_bytes or b'')} bytes; minimum > 5 bytes)."
            )
        magic_byte, schema_id = struct.unpack(">bI", raw_bytes[:CONFLUENT_HEADER_SIZE])
        if magic_byte != CONFLUENT_MAGIC_BYTE:
            raise ValueError(
                f"Invalid Confluent Wire Format magic byte: expected 0x00, got 0x{magic_byte & 0xFF:02x}."
            )
        return schema_id, raw_bytes[CONFLUENT_HEADER_SIZE:]

    @staticmethod
    def pack_confluent_wire_header(schema_id: int, avro_bytes: bytes) -> bytes:
        """Prefixes binary Avro bytes with the 5-byte Confluent Wire Format header."""
        header = struct.pack(">bI", CONFLUENT_MAGIC_BYTE, schema_id)
        return header + avro_bytes

    def encode_binary_avro(
        self,
        record: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Serializes a dictionary into schemaless Plain Binary Avro bytes."""
        target_schema = schema or self.default_schema
        try:
            import fastavro  # type: ignore

            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, target_schema, record)
            return buf.getvalue()
        except ImportError:
            return self._encode_standard_nullable_record(record, target_schema)

    def decode_binary_avro(
        self,
        avro_bytes: bytes,
        schema: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Deserializes schemaless Plain Binary Avro bytes using the provided .avsc schema."""
        if not avro_bytes:
            raise ValueError("Empty binary Avro payload.")
        target_schema = schema or self.default_schema
        try:
            import fastavro  # type: ignore

            buf = io.BytesIO(avro_bytes)
            result = fastavro.schemaless_reader(buf, target_schema)
            if not isinstance(result, dict):
                raise ValueError("Decoded Avro payload is not a record dictionary.")
            return result
        except ImportError:
            return self._decode_standard_nullable_record(avro_bytes, target_schema)

    def encode_wire_format_avro(
        self,
        record: Dict[str, Any],
        schema_id: int = 1,
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Serializes a dictionary into Confluent / GMK Schema Registry Wire Format bytes."""
        avro_payload = self.encode_binary_avro(record, schema=schema)
        return self.pack_confluent_wire_header(schema_id, avro_payload)

    def decode_wire_format_avro(
        self,
        raw_bytes: bytes,
    ) -> Tuple[int, Dict[str, Any]]:
        """Unpacks the 5-byte Schema Registry header and decodes the binary Avro payload."""
        schema_id, avro_payload = self.unpack_confluent_wire_header(raw_bytes)
        schema = self.resolve_schema(schema_id)
        decoded = self.decode_binary_avro(avro_payload, schema=schema)
        return schema_id, decoded

    @staticmethod
    def _write_zigzag_varint(buf: io.BytesIO, value: int) -> None:
        n = (value << 1) ^ (value >> 63)
        while (n & ~0x7F) != 0:
            buf.write(bytes([(n & 0x7F) | 0x80]))
            n >>= 7
        buf.write(bytes([n & 0x7F]))

    @staticmethod
    def _read_zigzag_varint(buf: io.BytesIO) -> int:
        shift = 0
        result = 0
        while True:
            byte_val = buf.read(1)
            if not byte_val:
                raise EOFError("Unexpected EOF while reading Avro zigzag varint.")
            b = byte_val[0]
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
            if shift > 64:
                raise ValueError("Corrupt Avro varint exceeds 64 bits.")
        return (result >> 1) ^ -(result & 1)

    @classmethod
    def _encode_standard_nullable_record(
        cls,
        record: Dict[str, Any],
        schema: Dict[str, Any],
    ) -> bytes:
        buf = io.BytesIO()
        for field_spec in schema.get("fields", []):
            fname = field_spec["name"]
            ftype = field_spec["type"]
            val = record.get(fname)
            is_nullable = isinstance(ftype, list) and "null" in ftype
            non_null_type = (
                [t for t in ftype if t != "null"][0]
                if isinstance(ftype, list)
                else ftype
            )
            if val is None:
                if not is_nullable:
                    raise ValueError(f"Required Avro field '{fname}' is None.")
                cls._write_zigzag_varint(buf, 0)  # Union index 0 -> null
            else:
                if is_nullable:
                    cls._write_zigzag_varint(buf, 1)  # Union index 1 -> value
                if non_null_type == "string":
                    encoded_str = str(val).encode("utf-8")
                    cls._write_zigzag_varint(buf, len(encoded_str))
                    buf.write(encoded_str)
                elif non_null_type == "double":
                    buf.write(struct.pack("<d", float(val)))
                elif non_null_type in ("long", "int"):
                    cls._write_zigzag_varint(buf, int(val))
                else:
                    encoded_str = json.dumps(val).encode("utf-8")
                    cls._write_zigzag_varint(buf, len(encoded_str))
                    buf.write(encoded_str)
        return buf.getvalue()

    @classmethod
    def _decode_standard_nullable_record(
        cls,
        avro_bytes: bytes,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        buf = io.BytesIO(avro_bytes)
        decoded: Dict[str, Any] = {}
        for field_spec in schema.get("fields", []):
            fname = field_spec["name"]
            ftype = field_spec["type"]
            is_nullable = isinstance(ftype, list) and "null" in ftype
            non_null_type = (
                [t for t in ftype if t != "null"][0]
                if isinstance(ftype, list)
                else ftype
            )
            if is_nullable:
                union_idx = cls._read_zigzag_varint(buf)
                if union_idx == 0:
                    decoded[fname] = None
                    continue
                if union_idx != 1:
                    raise ValueError(
                        f"Invalid Avro union branch index {union_idx} for field '{fname}'."
                    )
            decoded[fname] = cls._read_primitive_value(buf, non_null_type, fname)

        trailing = buf.read()
        if trailing:
            raise ValueError(
                f"Trailing unconsumed bytes ({len(trailing)}) in binary Avro payload."
            )
        return decoded

    @classmethod
    def _read_primitive_value(
        cls,
        buf: io.BytesIO,
        non_null_type: Any,
        fname: str,
    ) -> Any:
        if non_null_type == "string":
            str_len = cls._read_zigzag_varint(buf)
            if str_len < 0 or str_len > MAX_BQ_ROW_SIZE_BYTES:
                raise ValueError(
                    f"Corrupt Avro string length {str_len} for field '{fname}'."
                )
            raw_str = buf.read(str_len)
            if len(raw_str) != str_len:
                raise EOFError(
                    f"Truncated Avro string for field '{fname}': expected {str_len} bytes, got {len(raw_str)}."
                )
            return raw_str.decode("utf-8")
        if non_null_type == "double":
            raw_dbl = buf.read(8)
            if len(raw_dbl) != 8:
                raise EOFError(f"Truncated Avro double for field '{fname}'.")
            return struct.unpack("<d", raw_dbl)[0]
        if non_null_type in ("long", "int"):
            return cls._read_zigzag_varint(buf)
        raise ValueError(f"Unsupported Avro primitive type '{non_null_type}'.")


class DLQRecordBuilder:
    """
    Constructs Dead-Letter Queue (DLQ) quarantine records for `fab_telemetry_events_dlq`:
      - timestamp (TIMESTAMP)
      - payloadString (STRING, NULLABLE for binary decoding failures)
      - payloadBytes (BYTES, exact uncorrupted raw Kafka message bytes for replay)
      - attributes (ARRAY<STRUCT<key STRING, value STRING>>)
      - errorMessage (STRING)
      - stacktrace (STRING)
    """

    @staticmethod
    def build_dlq_record(
        raw_value_bytes: Optional[bytes],
        topic: str,
        partition: int,
        offset: int,
        error_message: str,
        stacktrace_str: Optional[str] = None,
        kafka_key_bytes: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        failure_timestamp: Optional[datetime.datetime] = None,
        input_format: Optional[str] = "JSON",
    ) -> Dict[str, Any]:
        """Builds a BigQuery-compatible DLQ quarantine dictionary."""
        ts = failure_timestamp or datetime.datetime.now(datetime.timezone.utc)
        payload_bytes = raw_value_bytes if raw_value_bytes is not None else b""

        payload_string: Optional[str] = None
        if payload_bytes:
            try:
                payload_string = payload_bytes.decode("utf-8")
            except UnicodeDecodeError:
                payload_string = None

        attributes: List[Dict[str, str]] = [
            {"key": "topic", "value": str(topic)},
            {"key": "partition", "value": str(partition)},
            {"key": "offset", "value": str(offset)},
        ]
        if input_format:
            attributes.append({"key": "input_format", "value": str(input_format)})

        if kafka_key_bytes is not None:
            try:
                attributes.append(
                    {"key": "kafka_key", "value": kafka_key_bytes.decode("utf-8")}
                )
            except UnicodeDecodeError:
                attributes.append(
                    {"key": "kafka_key_hex", "value": kafka_key_bytes.hex()}
                )

        if headers:
            for h_key, h_val in headers.items():
                attributes.append({"key": str(h_key), "value": str(h_val)})

        return {
            "timestamp": ts,
            "payloadString": payload_string,
            "payloadBytes": payload_bytes,
            "attributes": attributes,
            "errorMessage": str(error_message),
            "stacktrace": str(stacktrace_str or error_message),
        }


class BronzeRecordMapper:
    """
    Decodes incoming Kafka messages across all supported input formats (JSON, AVRO_BINARY,
    AVRO_SCHEMA_REGISTRY, SCHEMA_MAPPED_STRUCT) and maps them into the target BigQuery
    Bronze Iceberg Table schema (`fab_telemetry_events_bronze`).
    """

    def __init__(
        self,
        input_format: str = "JSON",
        avro_codec: Optional[AvroPayloadCodec] = None,
        exclusion_filter: Optional[ExclusionFilter] = None,
        schema_mapping_spec: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.input_format = input_format.strip().upper()
        self.avro_codec = avro_codec or AvroPayloadCodec()
        self.exclusion_filter = exclusion_filter or ExclusionFilter()
        self.schema_mapping_spec = schema_mapping_spec or DEFAULT_SCHEMA_MAPPING_SPEC

    def process_kafka_record(
        self,
        raw_key: Optional[bytes],
        raw_value: Optional[bytes],
        topic: str,
        partition: int,
        offset: int,
        kafka_timestamp: Optional[datetime.datetime] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
        """
        Processes a single raw Kafka message envelope.

        Returns:
            Tuple of:
              - status: 'SUCCESS', 'EXCLUDED', or 'DLQ'
              - bronze_row: Populated Bronze table dict when status == 'SUCCESS'
              - dlq_row: Populated DLQ table dict when status == 'DLQ'
              - reason: Exclusion rule or DLQ error summary
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        key_str = self._decode_key_string(raw_key)

        # 1. Check row size guardrail (BigQuery Storage Write API 20 MB limit / GMK 10 MB default)
        if raw_value and len(raw_value) > MAX_BQ_ROW_SIZE_BYTES:
            err_msg = (
                f"Message size ({len(raw_value)} bytes) exceeds BigQuery Storage Write API "
                f"maximum row size limit ({MAX_BQ_ROW_SIZE_BYTES} bytes)."
            )
            dlq_row = DLQRecordBuilder.build_dlq_record(
                raw_value_bytes=raw_value,
                topic=topic,
                partition=partition,
                offset=offset,
                error_message=err_msg,
                kafka_key_bytes=raw_key,
                headers=headers,
                failure_timestamp=now_utc,
                input_format=self.input_format,
            )
            return "DLQ", None, dlq_row, err_msg

        # 2. Check pre-decode header/key exclusion rules
        header_file = (
            (headers or {}).get("source_file")
            or (headers or {}).get("file_name")
            or (headers or {}).get("filename")
            or ""
        )
        is_excl, excl_reason = self.exclusion_filter.is_excluded(
            candidate_path_or_key=header_file or key_str or "",
            candidate_name=key_str or "",
        )
        if is_excl:
            return "EXCLUDED", None, None, excl_reason

        # 3. Decode payload according to input_format
        try:
            decoded_dict = self._decode_by_format(raw_value)
        except Exception as dec_err:
            tb_str = traceback.format_exc()
            dlq_row = DLQRecordBuilder.build_dlq_record(
                raw_value_bytes=raw_value,
                topic=topic,
                partition=partition,
                offset=offset,
                error_message=f"{type(dec_err).__name__}: {dec_err}",
                stacktrace_str=tb_str,
                kafka_key_bytes=raw_key,
                headers=headers,
                failure_timestamp=now_utc,
                input_format=self.input_format,
            )
            return "DLQ", None, dlq_row, str(dec_err)

        # 4. Check post-decode payload field exclusion rules (source_file, equipment_id, metric_name)
        src_file = str(
            decoded_dict.get("source_file")
            or decoded_dict.get("file_name")
            or header_file
            or ""
        )
        equip_id = str(decoded_dict.get("equipment_id") or "")
        metric_nm = str(decoded_dict.get("metric_name") or "")
        is_excl_post, excl_reason_post = self.exclusion_filter.is_excluded(
            candidate_path_or_key=src_file,
            candidate_name=equip_id,
            additional_candidates=[metric_nm, key_str or ""],
        )
        if is_excl_post:
            return "EXCLUDED", None, None, excl_reason_post

        # 5. Validate & normalize into Bronze table schema
        try:
            bronze_row = self._map_to_bronze_schema(
                decoded=decoded_dict,
                raw_key=raw_key,
                key_str=key_str,
                topic=topic,
                partition=partition,
                offset=offset,
                insert_time=kafka_timestamp or now_utc,
            )
            return "SUCCESS", bronze_row, None, None
        except Exception as map_err:
            tb_str = traceback.format_exc()
            dlq_row = DLQRecordBuilder.build_dlq_record(
                raw_value_bytes=raw_value,
                topic=topic,
                partition=partition,
                offset=offset,
                error_message=f"SchemaValidationError: {map_err}",
                stacktrace_str=tb_str,
                kafka_key_bytes=raw_key,
                headers=headers,
                failure_timestamp=now_utc,
                input_format=self.input_format,
            )
            return "DLQ", None, dlq_row, str(map_err)

    def _decode_by_format(self, raw_value: Optional[bytes]) -> Dict[str, Any]:
        if not raw_value:
            raise ValueError("Kafka message value is empty or null.")

        if self.input_format == "JSON":
            return self._decode_json_payload(raw_value)
        if self.input_format in ("AVRO_BINARY", "AVRO_BINARY_ENCODING"):
            return self.avro_codec.decode_binary_avro(raw_value)
        if self.input_format in (
            "AVRO_SCHEMA_REGISTRY",
            "AVRO_CONFLUENT_WIRE_FORMAT",
        ):
            _, decoded = self.avro_codec.decode_wire_format_avro(raw_value)
            return decoded
        if self.input_format == "SCHEMA_MAPPED_STRUCT":
            return self._decode_schema_mapped_struct(raw_value)

        raise ValueError(f"Unsupported input_format: '{self.input_format}'")

    @staticmethod
    def _decode_json_payload(raw_value: bytes) -> Dict[str, Any]:
        text = raw_value.decode("utf-8").strip()
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("JSON payload root must be a JSON object/dictionary.")
        # Support Kafka Connect Message-Based Schema mode (JSON with embedded 'schema' and 'payload')
        if (
            "schema" in parsed
            and "payload" in parsed
            and isinstance(parsed["payload"], dict)
        ):
            return parsed["payload"]
        return parsed

    def _decode_schema_mapped_struct(self, raw_value: bytes) -> Dict[str, Any]:
        parsed = self._decode_json_payload(raw_value)
        spec = self.schema_mapping_spec or {}

        # Validate required_source_fields if configured in mapping spec
        required_src = spec.get("required_source_fields") or []
        for req_field in required_src:
            if parsed.get(req_field) in (None, ""):
                raise ValueError(
                    f"Missing required source field '{req_field}' in SCHEMA_MAPPED_STRUCT payload."
                )

        # Flatten nested 'telemetry', 'data', 'metrics', or 'device' struct envelopes if present
        flattened: Dict[str, Any] = {}
        for k, v in parsed.items():
            if k in ("telemetry", "data", "metrics", "device") and isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    flattened[sub_k] = sub_v
            else:
                flattened[k] = v

        # Apply declarative field_mappings (e.g., camelCase vendor keys -> canonical Bronze columns)
        field_mappings: Dict[str, str] = spec.get("field_mappings") or {}
        mapped: Dict[str, Any] = {}
        for src_k, val in flattened.items():
            target_k = field_mappings.get(src_k, src_k)
            mapped[target_k] = val

        # Apply default values for omitted fields
        defaults: Dict[str, Any] = spec.get("defaults") or {}
        for def_k, def_v in defaults.items():
            if mapped.get(def_k) is None:
                mapped[def_k] = def_v

        # Apply explicit type_casts if configured
        type_casts: Dict[str, str] = spec.get("type_casts") or {}
        for cast_field, target_type in type_casts.items():
            if mapped.get(cast_field) is not None and mapped.get(cast_field) != "":
                t_upper = str(target_type).upper()
                if t_upper in ("FLOAT", "FLOAT64", "DOUBLE"):
                    mapped[cast_field] = float(mapped[cast_field])
                elif t_upper in ("INT", "INT64", "LONG", "INTEGER"):
                    mapped[cast_field] = int(mapped[cast_field])
                elif t_upper in ("STRING", "STR"):
                    mapped[cast_field] = str(mapped[cast_field])

        return mapped

    @staticmethod
    def _decode_key_string(raw_key: Optional[bytes]) -> Optional[str]:
        if raw_key is None:
            return None
        try:
            return raw_key.decode("utf-8")
        except UnicodeDecodeError:
            return raw_key.hex()

    @staticmethod
    def _map_to_bronze_schema(
        decoded: Dict[str, Any],
        raw_key: Optional[bytes],
        key_str: Optional[str],
        topic: str,
        partition: int,
        offset: int,
        insert_time: datetime.datetime,
    ) -> Dict[str, Any]:
        # Validate metric_value is numeric if provided
        raw_metric_val = decoded.get("metric_value")
        metric_val: Optional[float] = None
        if raw_metric_val is not None and raw_metric_val != "":
            try:
                metric_val = float(raw_metric_val)
            except (ValueError, TypeError) as val_err:
                raise ValueError(
                    f"Cannot convert field 'metric_value' value '{raw_metric_val}' to FLOAT64."
                ) from val_err

        # Parse event_timestamp (or default to insert_time if omitted, since all Bronze cols are NULLABLE)
        raw_event_ts = decoded.get("event_timestamp")
        event_ts: Optional[datetime.datetime] = insert_time
        if raw_event_ts is not None and str(raw_event_ts).strip() != "":
            parsed_ts = parse_iso_timestamp(raw_event_ts)
            if parsed_ts is None:
                raise ValueError(
                    f"Invalid TIMESTAMP format for 'event_timestamp': '{raw_event_ts}'"
                )
            event_ts = parsed_ts

        # Collect extensible attributes into payload_ext_json
        explicit_ext = decoded.get("payload_ext_json")
        if isinstance(explicit_ext, dict):
            ext_dict = dict(explicit_ext)
        elif isinstance(explicit_ext, str) and explicit_ext.strip():
            try:
                ext_dict = json.loads(explicit_ext)
            except ValueError:
                ext_dict = {"raw_ext": explicit_ext}
        else:
            ext_dict = {}

        for k, v in decoded.items():
            if k not in CORE_BRONZE_FIELDS and v is not None:
                ext_dict[k] = v

        payload_ext_str = json.dumps(ext_dict, sort_keys=True) if ext_dict else "{}"

        return {
            "event_id": (
                str(decoded["event_id"])
                if decoded.get("event_id") is not None
                else None
            ),
            "equipment_id": (
                str(decoded["equipment_id"])
                if decoded.get("equipment_id") is not None
                else None
            ),
            "fab_site_code": (
                str(decoded["fab_site_code"])
                if decoded.get("fab_site_code") is not None
                else None
            ),
            "wafer_lot_id": (
                str(decoded["wafer_lot_id"])
                if decoded.get("wafer_lot_id") is not None
                else None
            ),
            "metric_name": (
                str(decoded["metric_name"])
                if decoded.get("metric_name") is not None
                else None
            ),
            "metric_value": metric_val,
            "status_code": (
                str(decoded["status_code"])
                if decoded.get("status_code") is not None
                else None
            ),
            "event_timestamp": event_ts,
            "payload_ext_json": payload_ext_str,
            "kafka_key": {"key": key_str},
            "_key": raw_key,
            "kafka_metadata": {
                "topic": str(topic),
                "partition": int(partition),
                "offset": int(offset),
                "insertTime": insert_time,
            },
        }
