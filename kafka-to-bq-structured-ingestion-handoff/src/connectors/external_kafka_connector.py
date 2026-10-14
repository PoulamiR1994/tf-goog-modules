#!/usr/bin/env python3
"""
External / On-Premises / GCE VM Apache Kafka Connector Module (via PAM Secret Manager)
======================================================================================
Isolates External Kafka endpoint resolution, PAM credential retrieval from GCP
Secret Manager, and `SASL_SSL` / `SASL_PLAINTEXT` (`SCRAM-SHA-512` / `SCRAM-SHA-256` /
`PLAIN` + optional JKS TrustStore/KeyStore) Spark Structured Streaming configuration.

Key Characteristics of the External Kafka Path:
  - Authentication : PAM-rotated credentials retrieved at runtime from GCP Secret Manager
                     (`kafka-pam-credentials`)
  - SASL Mechanism : `SCRAM-SHA-512` (default), `SCRAM-SHA-256`, or `PLAIN`
  - Login Module   : `org.apache.kafka.common.security.scram.ScramLoginModule`
  - Transport      : GCE VM (`10.20.0.50:9093`) or On-Premises Brokers
"""

import json
import logging
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class ExternalKafkaEndpointResolver:
    """
    Resolves External / On-Premises / GCE VM Kafka bootstrap server lists and topic names,
    including semicolon-delimited `host1:port,host2:port;topic_name` specifications.
    """

    @staticmethod
    def resolve_external_endpoints(
        raw_bootstrap: str,
        raw_topic: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Normalizes External Kafka bootstrap server `host:port` list and topic name."""
        clean_boot = (raw_bootstrap or "").strip()
        clean_topic = (raw_topic or "").strip()

        if ";" in clean_boot and not clean_boot.startswith("projects/"):
            parts = clean_boot.split(";", 1)
            clean_boot = parts[0].strip()
            if not clean_topic:
                clean_topic = parts[1].strip()

        if not clean_topic:
            raise ValueError(
                "External Kafka topic must be provided via --kafka-topic or embedded in --kafka-bootstrap-servers."
            )

        return clean_boot, clean_topic

    resolve_endpoints = resolve_external_endpoints


class ExternalKafkaPAMResolver:
    """
    Retrieves and normalizes PAM-managed Kafka and Schema Registry credentials from
    Google Cloud Secret Manager for the External Kafka ingestion path.
    """

    @staticmethod
    def resolve_pam_credentials(
        project_id: str,
        secret_fetcher: Callable[[str, str], str],
        secret_name: Optional[str] = "kafka-pam-credentials",
        sasl_username_secret: str = "kafka-sasl-username",
        sasl_password_secret: str = "kafka-sasl-password",
        truststore_password_secret: str = "kafka-truststore-password",
    ) -> Dict[str, Any]:
        """
        Resolves External Kafka PAM credentials from a consolidated JSON secret
        (`kafka-pam-credentials`) or discrete fallback secrets in GCP Secret Manager.
        """
        creds: Dict[str, Any] = {
            "auth_mode": "SASL_SCRAM_512",
            "sasl_mechanism": "SCRAM-SHA-512",
            "security_protocol": "SASL_SSL",
            "username": "",
            "password": "",
            "truststore_location": "",
            "truststore_password": "",
            "keystore_location": "",
            "keystore_password": "",
            "schema_registry_url": "",
            "schema_registry_auth_mode": "GCP",
            "schema_registry_api_key": "",
            "schema_registry_api_secret": "",
        }

        if secret_name:
            try:
                raw_payload = secret_fetcher(project_id, secret_name)
                parsed = json.loads(raw_payload)
                if isinstance(parsed, dict):
                    creds.update({k: v for k, v in parsed.items() if v is not None})
                    if "sasl_username" in parsed and not creds.get("username"):
                        creds["username"] = parsed["sasl_username"]
                    if "sasl_password" in parsed and not creds.get("password"):
                        creds["password"] = parsed["sasl_password"]
                    logger.info(
                        "Successfully resolved External Kafka PAM configuration from secret '%s' (auth_mode=%s).",
                        secret_name,
                        creds.get("auth_mode"),
                    )
                    return creds
            except Exception as ex:
                logger.warning(
                    "Could not load consolidated PAM JSON secret '%s' (%s).",
                    secret_name,
                    ex,
                )

        try:
            creds["username"] = secret_fetcher(project_id, sasl_username_secret)
            creds["password"] = secret_fetcher(project_id, sasl_password_secret)
            try:
                creds["truststore_password"] = secret_fetcher(
                    project_id, truststore_password_secret
                )
            except Exception:
                creds["truststore_password"] = ""
        except Exception as fallback_err:
            logger.warning(
                "Fallback individual Kafka secrets unavailable (%s); continuing with current config.",
                fallback_err,
            )

        return creds


class ExternalKafkaSparkConfigBuilder:
    """
    Builds Spark Structured Streaming `.format("kafka")` reader options exclusively
    for External / On-Premises / GCE VM Kafka clusters using PAM SASL/SCRAM or PLAIN
    credentials and optional JKS TrustStore/KeyStore certificates.
    """

    @staticmethod
    def build_external_reader_options(
        bootstrap_servers: str,
        topic: str,
        pam_creds: Dict[str, Any],
        starting_offsets: str = "earliest",
        consumer_group_id: str = "cg-spark-kafka-bq-bronze",
        max_offsets_per_trigger: int = 500000,
        fail_on_data_loss: bool = True,
    ) -> Dict[str, str]:
        """
        Generates Spark Kafka reader options for External Kafka (`SASL_SSL` / `SASL_PLAINTEXT`
        with `SCRAM-SHA-512` / `SCRAM-SHA-256` / `PLAIN` and optional custom JKS TrustStore/KeyStore).
        """
        options: Dict[str, str] = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "failOnDataLoss": str(fail_on_data_loss).lower(),
            "maxOffsetsPerTrigger": str(max_offsets_per_trigger),
            "groupIdPrefix": consumer_group_id,
        }

        auth_mode = str(
            pam_creds.get("auth_mode")
            or pam_creds.get("sasl_mechanism")
            or "SASL_SCRAM_512"
        ).upper()
        sec_protocol = str(pam_creds.get("security_protocol", "SASL_SSL")).upper()
        username = str(pam_creds.get("username", ""))
        password = str(pam_creds.get("password", ""))

        if auth_mode == "PLAINTEXT" or sec_protocol == "PLAINTEXT":
            options["kafka.security.protocol"] = "PLAINTEXT"
            return options

        options["kafka.security.protocol"] = sec_protocol
        if "SCRAM" in auth_mode:
            mechanism = "SCRAM-SHA-256" if "256" in auth_mode else "SCRAM-SHA-512"
            options["kafka.sasl.mechanism"] = mechanism
            options["kafka.sasl.jaas.config"] = (
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                f'username="{username}" password="{password}";'
            )
        elif "PLAIN" in auth_mode:
            options["kafka.sasl.mechanism"] = "PLAIN"
            options["kafka.sasl.jaas.config"] = (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{username}" password="{password}";'
            )

        truststore_loc = str(
            pam_creds.get("truststore_location")
            or pam_creds.get("truststore_gcs_uri")
            or ""
        )
        truststore_pass = str(pam_creds.get("truststore_password") or "")
        if truststore_loc:
            options["kafka.ssl.truststore.location"] = truststore_loc
        if truststore_pass:
            options["kafka.ssl.truststore.password"] = truststore_pass

        keystore_loc = str(
            pam_creds.get("keystore_location")
            or pam_creds.get("keystore_gcs_uri")
            or ""
        )
        keystore_pass = str(pam_creds.get("keystore_password") or "")
        if keystore_loc:
            options["kafka.ssl.keystore.location"] = keystore_loc
        if keystore_pass:
            options["kafka.ssl.keystore.password"] = keystore_pass

        return options

    build_reader_options = build_external_reader_options
