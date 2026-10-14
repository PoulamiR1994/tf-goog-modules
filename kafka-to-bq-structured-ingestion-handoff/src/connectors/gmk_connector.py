#!/usr/bin/env python3
"""
Google Cloud Managed Service for Apache Kafka (GMK) Connector Module
====================================================================
Isolates GMK endpoint resolution and keyless GCP IAM `OAUTHBEARER`
(`GcpLoginCallbackHandler`) Spark Structured Streaming configuration.

Key Characteristics of the GMK Path:
  - Authentication : Keyless GCP Workload/Service Account IAM (`OAUTHBEARER`)
  - Callback Class : `com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler`
  - Transport      : Private Service Connect (PSC) endpoints on `tcp:9092` (`SASL_SSL`)
"""

import re
from typing import Dict, Optional, Tuple


class GMKEndpointResolver:
    """
    Resolves Google Cloud Managed Service for Apache Kafka (GMK) topic resource URIs
    and bootstrap server FQDNs.
    """

    GMK_URI_REGEX = re.compile(
        r"^projects/(?P<project>[^/]+)/locations/(?P<region>[^/]+)/clusters/(?P<cluster>[^/]+)"
        r"(?:/topics/(?P<topic>[^/]+))?$"
    )

    @classmethod
    def is_gmk_endpoint(cls, raw_bootstrap: str, raw_topic: Optional[str] = None) -> bool:
        """Returns True if either the topic or bootstrap string matches a GMK pattern."""
        clean_bootstrap = (raw_bootstrap or "").strip()
        clean_topic = (raw_topic or "").strip()
        if cls.GMK_URI_REGEX.match(clean_bootstrap) or cls.GMK_URI_REGEX.match(
            clean_topic
        ):
            return True
        return ".managedkafka." in clean_bootstrap.lower()

    @classmethod
    def resolve_gmk_endpoints(
        cls,
        raw_bootstrap: str,
        raw_topic: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Normalizes GMK bootstrap servers and topic name from either a full GMK Topic URI
        (`projects/<project>/locations/<region>/clusters/<cluster>/topics/<topic>`)
        or explicit GMK bootstrap FQDN + topic.
        """
        clean_boot = (raw_bootstrap or "").strip()
        clean_topic = (raw_topic or "").strip()

        if ";" in clean_boot and not clean_boot.startswith("projects/"):
            parts = clean_boot.split(";", 1)
            clean_boot = parts[0].strip()
            if not clean_topic:
                clean_topic = parts[1].strip()

        match = cls.GMK_URI_REGEX.match(clean_boot) or cls.GMK_URI_REGEX.match(
            clean_topic
        )
        if match:
            gmk_proj = match.group("project")
            gmk_reg = match.group("region")
            gmk_cluster = match.group("cluster")
            uri_topic = match.group("topic") or ""
            synthesized_boot = (
                f"bootstrap.{gmk_cluster}.{gmk_reg}.managedkafka.{gmk_proj}.cloud.goog:9092"
            )
            if cls.GMK_URI_REGEX.match(clean_boot) or not clean_boot:
                clean_boot = synthesized_boot
            if (not clean_topic or cls.GMK_URI_REGEX.match(clean_topic)) and uri_topic:
                clean_topic = uri_topic

        if not clean_topic:
            raise ValueError(
                "GMK topic must be provided via --kafka-topic or embedded in --kafka-bootstrap-servers."
            )

        return clean_boot, clean_topic

    resolve_endpoints = resolve_gmk_endpoints


class GMKSparkConfigBuilder:
    """
    Builds Spark Structured Streaming `.format("kafka")` reader options exclusively
    for Google Cloud Managed Service for Apache Kafka (GMK) using keyless IAM `OAUTHBEARER`.
    """

    GMK_CALLBACK_HANDLER = "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"
    GMK_OAUTH_JAAS = (
        'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;'
    )

    @classmethod
    def build_gmk_reader_options(
        cls,
        bootstrap_servers: str,
        topic: str,
        starting_offsets: str = "earliest",
        consumer_group_id: str = "cg-spark-kafka-bq-bronze",
        max_offsets_per_trigger: int = 500000,
        fail_on_data_loss: bool = True,
    ) -> Dict[str, str]:
        """
        Generates Spark Kafka reader options for GMK (`SASL_SSL` + `OAUTHBEARER`).
        """
        return {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": starting_offsets,
            "failOnDataLoss": str(fail_on_data_loss).lower(),
            "maxOffsetsPerTrigger": str(max_offsets_per_trigger),
            "groupIdPrefix": consumer_group_id,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.login.callback.handler.class": cls.GMK_CALLBACK_HANDLER,
            "kafka.sasl.jaas.config": cls.GMK_OAUTH_JAAS,
        }

    build_reader_options = build_gmk_reader_options
