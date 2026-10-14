"""
Unit tests for Kafka endpoint normalization (GMK vs. External), Spark Structured Streaming
JAAS authentication config generation, Bronze & DLQ Spark schema conformance, and DDL generation.
"""

import unittest

from setup.create_iceberg_tables import (
    build_bronze_iceberg_ddl,
    build_bronze_native_fallback_ddl,
    build_curated_deduplication_view_ddl,
    build_dlq_table_ddl,
    build_dlq_triage_query_sql,
)
from src.connectors.external_kafka_connector import (
    ExternalKafkaEndpointResolver,
    ExternalKafkaSparkConfigBuilder,
)
from src.connectors.gmk_connector import (
    GMKEndpointResolver,
    GMKSparkConfigBuilder,
)
from src.jobs.kafka_bq_ingestion import (
    BRONZE_TABLE_SCHEMA,
    DLQ_TABLE_SCHEMA,
    KafkaEndpointResolver,
    KafkaSparkConfigBuilder,
    parse_arguments,
)


class TestKafkaTransformsAndConfig(unittest.TestCase):
    """Tests for endpoint resolution, Spark Kafka auth options, schemas, and Iceberg DDL."""

    def test_gmk_topic_uri_normalization(self) -> None:
        """Verify full GMK topic resource URI resolves to GMK bootstrap FQDN, topic, and GMK source type."""
        uri = (
            "projects/example-gcp-project/locations/us-central1/"
            "clusters/gmk-fab-telemetry-poc/topics/fab.equipment.telemetry.v1"
        )
        boot, topic, source_type = KafkaEndpointResolver.resolve_endpoints(uri)
        self.assertEqual(
            boot,
            "bootstrap.gmk-fab-telemetry-poc.us-central1.managedkafka.example-gcp-project.cloud.goog:9092",
        )
        self.assertEqual(topic, "fab.equipment.telemetry.v1")
        self.assertEqual(source_type, "GMK")

    def test_external_kafka_semicolon_normalization(self) -> None:
        """Verify External Fab Kafka semicolon-delimited bootstrap;topic specification."""
        spec = "fab-kafka-01.corp.intel.com:9093,fab-kafka-02.corp.intel.com:9093;fab.equipment.telemetry.v1"
        boot, topic, source_type = KafkaEndpointResolver.resolve_endpoints(spec)
        self.assertEqual(
            boot,
            "fab-kafka-01.corp.intel.com:9093,fab-kafka-02.corp.intel.com:9093",
        )
        self.assertEqual(topic, "fab.equipment.telemetry.v1")
        self.assertEqual(source_type, "EXTERNAL")

    def test_gmk_spark_reader_options(self) -> None:
        """Verify GMK source generates keyless IAM OAUTHBEARER + GcpLoginCallbackHandler options."""
        opts = KafkaSparkConfigBuilder.build_reader_options(
            bootstrap_servers="bootstrap.gmk-fab-telemetry-poc.us-central1.managedkafka.example-gcp-project.cloud.goog:9092",
            topic="fab.equipment.telemetry.v1",
            source_type="GMK",
            pam_creds={},
            max_offsets_per_trigger=250000,
        )
        self.assertEqual(opts["kafka.security.protocol"], "SASL_SSL")
        self.assertEqual(opts["kafka.sasl.mechanism"], "OAUTHBEARER")
        self.assertEqual(
            opts["kafka.sasl.login.callback.handler.class"],
            "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
        )
        self.assertEqual(opts["maxOffsetsPerTrigger"], "250000")

    def test_external_kafka_spark_reader_options(self) -> None:
        """Verify External Kafka source generates SASL_SCRAM_512 + TrustStore options from PAM secret."""
        pam_creds = {
            "auth_mode": "SASL_SCRAM_512",
            "security_protocol": "SASL_SSL",
            "username": "svc_fab_reader",
            "password": "ephemeral_pam_value",
            "truststore_location": "gs://example-gcp-project-kafka-landing-zone/certs/fab-ca.jks",
            "truststore_password": "ephemeral_ts_value",
        }
        opts = KafkaSparkConfigBuilder.build_reader_options(
            bootstrap_servers="fab-kafka-01.corp.intel.com:9093",
            topic="fab.equipment.telemetry.v1",
            source_type="EXTERNAL",
            pam_creds=pam_creds,
        )
        self.assertEqual(opts["kafka.security.protocol"], "SASL_SSL")
        self.assertEqual(opts["kafka.sasl.mechanism"], "SCRAM-SHA-512")
        self.assertIn("ScramLoginModule", opts["kafka.sasl.jaas.config"])
        self.assertEqual(
            opts["kafka.ssl.truststore.location"],
            "gs://example-gcp-project-kafka-landing-zone/certs/fab-ca.jks",
        )

    def test_spark_dataframe_schemas_conformance(self) -> None:
        """Verify Bronze and DLQ Spark schemas match target BigQuery table definitions."""
        bronze_fields = {f.name for f in BRONZE_TABLE_SCHEMA.fields}
        expected_bronze = {
            "event_id",
            "equipment_id",
            "fab_site_code",
            "wafer_lot_id",
            "metric_name",
            "metric_value",
            "status_code",
            "event_timestamp",
            "payload_ext_json",
            "kafka_key",
            "_key",
            "kafka_metadata",
        }
        self.assertEqual(bronze_fields, expected_bronze)
        self.assertTrue(all(f.nullable for f in BRONZE_TABLE_SCHEMA.fields))

        dlq_fields = {f.name for f in DLQ_TABLE_SCHEMA.fields}
        expected_dlq = {
            "timestamp",
            "payloadString",
            "payloadBytes",
            "attributes",
            "errorMessage",
            "stacktrace",
        }
        self.assertEqual(dlq_fields, expected_dlq)

    def test_iceberg_and_dlq_ddl_generation(self) -> None:
        """Verify BigQuery-Managed Apache Iceberg Bronze DDL, DLQ DDL, Curated View DDL, and Triage SQL."""
        ice_ddl = build_bronze_iceberg_ddl(
            project_id="example-gcp-project",
            region="us-central1",
            dataset_id="bq_bronze_fab_telemetry_poc",
            table_name="fab_telemetry_events_bronze",
            bucket="example-gcp-project-kafka-landing-zone",
            connection_id="gcp_lakehouse_connection",
        )
        self.assertIn("table_format = 'ICEBERG'", ice_ddl)
        self.assertIn("file_format = 'PARQUET'", ice_ddl)
        self.assertIn(
            "WITH CONNECTION `example-gcp-project.us-central1.gcp_lakehouse_connection`",
            ice_ddl,
        )

        native_ddl = build_bronze_native_fallback_ddl(
            project_id="example-gcp-project",
            dataset_id="bq_bronze_fab_telemetry_poc",
            table_name="fab_telemetry_events_bronze",
        )
        self.assertIn("require_partition_filter = true", native_ddl)

        dlq_ddl = build_dlq_table_ddl(
            project_id="example-gcp-project",
            dataset_id="bq_bronze_fab_telemetry_poc",
        )
        self.assertIn("fab_telemetry_events_dlq", dlq_ddl)
        self.assertIn("PARTITION BY DATE(timestamp)", dlq_ddl)
        self.assertIn("require_partition_filter = true", dlq_ddl)

        view_ddl = build_curated_deduplication_view_ddl(
            project_id="example-gcp-project",
            dataset_id="bq_bronze_fab_telemetry_poc",
        )
        self.assertIn("QUALIFY", view_ddl)
        self.assertIn("ROW_NUMBER() OVER", view_ddl)

        triage_sql = build_dlq_triage_query_sql(
            project_id="example-gcp-project",
            dataset_id="bq_bronze_fab_telemetry_poc",
        )
        self.assertIn("UNNEST(attributes)", triage_sql)
        self.assertIn("input_format", triage_sql)

    def test_cli_flag_aliases(self) -> None:
        """Verify --schema-file-gcs-uri and --schema-mapping-uri parse cleanly."""
        parsed = parse_arguments(
            [
                "--kafka-bootstrap-servers=bootstrap.gmk:9092",
                "--kafka-topic=fab.equipment.telemetry.v1",
                "--input-format=SCHEMA_MAPPED_STRUCT",
                "--schema-file-gcs-uri=gs://bkt/schemas/test.avsc",
                "--schema-mapping-uri=gs://bkt/schemas/map.json",
            ]
        )
        self.assertEqual(parsed.avro_schema_path, "gs://bkt/schemas/test.avsc")
        self.assertEqual(parsed.schema_mapping_uri, "gs://bkt/schemas/map.json")

    def test_separated_gmk_and_external_connector_modules(self) -> None:
        """Verify dedicated gmk_connector.py and external_kafka_connector.py operate independently."""
        gmk_boot, gmk_topic = GMKEndpointResolver.resolve_endpoints(
            "projects/example-gcp-project/locations/us-central1/clusters/gmk-fab-telemetry-poc",
            "fab.equipment.telemetry.v1",
        )
        self.assertEqual(
            gmk_boot,
            "bootstrap.gmk-fab-telemetry-poc.us-central1.managedkafka.example-gcp-project.cloud.goog:9092",
        )
        gmk_opts = GMKSparkConfigBuilder.build_reader_options(
            bootstrap_servers=gmk_boot,
            topic=gmk_topic,
        )
        self.assertEqual(gmk_opts["kafka.sasl.mechanism"], "OAUTHBEARER")

        ext_boot, ext_topic = ExternalKafkaEndpointResolver.resolve_endpoints(
            "10.20.0.50:9093;fab.cleanroom.metrology.struct.v1"
        )
        self.assertEqual(ext_boot, "10.20.0.50:9093")
        self.assertEqual(ext_topic, "fab.cleanroom.metrology.struct.v1")
        ext_opts = ExternalKafkaSparkConfigBuilder.build_reader_options(
            bootstrap_servers=ext_boot,
            topic=ext_topic,
            pam_creds={
                "username": "svc_intel_fab_kafka_reader",
                "password": "secret-password",
                "sasl_mechanism": "SCRAM-SHA-512",
                "security_protocol": "SASL_SSL",
            },
        )
        self.assertEqual(ext_opts["kafka.sasl.mechanism"], "SCRAM-SHA-512")
        self.assertIn("ScramLoginModule", ext_opts["kafka.sasl.jaas.config"])


if __name__ == "__main__":
    unittest.main()
