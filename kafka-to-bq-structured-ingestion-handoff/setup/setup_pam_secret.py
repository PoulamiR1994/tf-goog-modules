#!/usr/bin/env python3
"""
PAM (Privileged Access Management) Secret Provisioner for GCP Secret Manager.

Separates credential asset creation from the runtime Kafka-to-BigQuery Spark job.
Consolidates Kafka authentication & Schema Registry credentials into a single JSON
secret in GCP Secret Manager (default: 'kafka-pam-credentials').

Supports both:
- Option A (GMK): auth_mode = 'GMK_OAUTHBEARER' (keyless IAM + Managed Schema Registry URL)
- Option B (External / On-Premises Fab Kafka): auth_mode = 'SASL_SCRAM_512' / 'SASL_PLAIN' / 'MTLS'
  with username, password, truststore_gcs_uri, truststore_password, and Schema Registry API keys.
"""

import argparse
import json
import logging
import os
from typing import Any, Dict, Optional

try:
    from google.api_core import exceptions
    from google.cloud import secretmanager
except ImportError:
    exceptions = None  # type: ignore[assignment]
    secretmanager = None  # type: ignore[assignment]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("setup_pam_secret")


def _load_job_config_defaults() -> Dict[str, str]:
    """Loads environment defaults from config/job_config.yaml if present."""
    defaults: Dict[str, str] = {}
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    cfg_path = os.environ.get(
        "JOB_CONFIG_PATH", os.path.join(repo_root, "config", "job_config.yaml")
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
DEFAULT_PROJECT_ID = (
    os.environ.get("GCP_PROJECT_ID")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or _CFG.get("project_id", "")
)
DEFAULT_SECRET_NAME = _CFG.get("secret_name", "kafka-pam-credentials")


def get_existing_secret(
    client: Any,
    project_id: str,
    secret_id: str,
) -> str:
    """Fetches the latest version of an existing secret if present."""
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()


def upsert_pam_json_secret(
    project_id: str,
    pam_secret_name: str,
    auth_mode: str = "GMK_OAUTHBEARER",
    security_protocol: str = "SASL_SSL",
    username: str = "",
    password: str = "",
    truststore_gcs_uri: str = "",
    truststore_password: str = "",
    schema_registry_url: str = "",
    schema_registry_auth_mode: str = "GCP",
    schema_registry_api_key: str = "",
    schema_registry_api_secret: str = "",
) -> str:
    """Creates or updates the consolidated Kafka PAM JSON secret in GCP Secret Manager."""
    payload_dict = {
        "auth_mode": auth_mode,
        "security_protocol": security_protocol,
        "username": username,
        "password": password,
        "truststore_gcs_uri": truststore_gcs_uri,
        "truststore_password": truststore_password,
        "schema_registry_url": schema_registry_url,
        "schema_registry_auth_mode": schema_registry_auth_mode,
        "schema_registry_api_key": schema_registry_api_key,
        "schema_registry_api_secret": schema_registry_api_secret,
    }
    payload_bytes = json.dumps(payload_dict, indent=2).encode("UTF-8")

    if secretmanager is None:
        import subprocess

        parent_secret = f"projects/{project_id}/secrets/{pam_secret_name}"
        desc_res = subprocess.run(
            ["gcloud", "secrets", "describe", pam_secret_name, f"--project={project_id}"],
            capture_output=True,
            text=True,
        )
        if desc_res.returncode != 0:
            subprocess.run(
                [
                    "gcloud",
                    "secrets",
                    "create",
                    pam_secret_name,
                    "--replication-policy=automatic",
                    f"--project={project_id}",
                ],
                check=True,
            )
        ver_res = subprocess.run(
            [
                "gcloud",
                "secrets",
                "versions",
                "add",
                pam_secret_name,
                "--data-file=-",
                f"--project={project_id}",
            ],
            input=payload_bytes,
            capture_output=True,
            check=True,
        )
        logger.info(
            "Successfully stored Kafka PAM JSON secret via gcloud CLI for %s (%s)",
            parent_secret,
            ver_res.stderr.strip() or "latest",
        )
        return f"{parent_secret}/versions/latest"

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"
    secret_path = f"{parent}/secrets/{pam_secret_name}"

    try:
        client.get_secret(request={"name": secret_path})
        logger.info(
            "PAM secret '%s' already exists in Secret Manager.", pam_secret_name
        )
    except exceptions.NotFound:
        logger.info(
            "Creating new PAM secret '%s' in project '%s'...",
            pam_secret_name,
            project_id,
        )
        client.create_secret(
            request={
                "parent": parent,
                "secret_id": pam_secret_name,
                "secret": {"replication": {"automatic": {}}},
            }
        )

    version = client.add_secret_version(
        request={
            "parent": secret_path,
            "payload": {"data": payload_bytes},
        }
    )
    logger.info("Successfully stored Kafka PAM JSON secret version: %s", version.name)
    return version.name


def main() -> None:
    """CLI entrypoint for provisioning the Kafka PAM JSON secret in GCP Secret Manager."""
    parser = argparse.ArgumentParser(
        description="Provision consolidated Kafka PAM JSON Secret in GCP Secret Manager"
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help="GCP Project ID",
    )
    parser.add_argument(
        "--secret-name",
        default=DEFAULT_SECRET_NAME,
        help="Target PAM Secret Name (default: kafka-pam-credentials)",
    )
    parser.add_argument(
        "--auth-mode",
        default="GMK_OAUTHBEARER",
        choices=[
            "GMK_OAUTHBEARER",
            "SASL_SCRAM_512",
            "SASL_SCRAM_256",
            "SASL_PLAIN",
            "MTLS",
            "SCRAM-SHA-512",
            "SCRAM-SHA-256",
            "PLAIN",
            "OAUTHBEARER",
        ],
        help="Kafka authentication mode (GMK_OAUTHBEARER for GMK; SASL_SCRAM_512 / SASL_PLAIN / MTLS for External Kafka)",
    )
    parser.add_argument(
        "--sasl-mechanism",
        default="",
        help="Optional explicit SASL mechanism alias (e.g., OAUTHBEARER, SCRAM-SHA-512, SCRAM-SHA-256, PLAIN)",
    )
    parser.add_argument(
        "--security-protocol",
        default="SASL_SSL",
        help="Kafka security.protocol (default: SASL_SSL)",
    )
    parser.add_argument(
        "--username",
        default="",
        help="SASL Username for External/On-Premises Kafka",
    )
    parser.add_argument(
        "--password",
        default="",
        help="SASL Password for External/On-Premises Kafka",
    )
    parser.add_argument(
        "--truststore-gcs-uri",
        default="",
        help="Optional GCS URI to CA TrustStore .jks file for External Kafka TLS",
    )
    parser.add_argument(
        "--truststore-password",
        default="",
        help="Optional CA TrustStore password",
    )
    parser.add_argument(
        "--schema-registry-url",
        default=_CFG.get(
            "schema_registry_url",
            f"https://managedkafka.googleapis.com/v1/projects/{DEFAULT_PROJECT_ID}/locations/us-central1/schemaRegistries/mfg-telemetry-sr-poc",
        ),
        help="Google Cloud Managed Schema Registry or Confluent Schema Registry URL",
    )
    parser.add_argument(
        "--schema-registry-auth-mode",
        default="GCP",
        choices=["GCP", "BASIC_AUTH"],
        help="Schema Registry authentication mode: GCP (ADC) or BASIC_AUTH",
    )
    parser.add_argument(
        "--schema-registry-api-key",
        default="",
        help="Optional Schema Registry API Key when --schema-registry-auth-mode=BASIC_AUTH",
    )
    parser.add_argument(
        "--schema-registry-api-secret",
        default="",
        help="Optional Schema Registry API Secret when --schema-registry-auth-mode=BASIC_AUTH",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and log the consolidated PAM JSON payload structure without writing to Secret Manager",
    )
    args = parser.parse_args()

    effective_auth_mode = args.auth_mode
    if args.sasl_mechanism:
        mech_upper = args.sasl_mechanism.upper()
        if mech_upper == "SCRAM-SHA-512":
            effective_auth_mode = "SASL_SCRAM_512"
        elif mech_upper == "SCRAM-SHA-256":
            effective_auth_mode = "SASL_SCRAM_256"
        elif mech_upper == "PLAIN":
            effective_auth_mode = "SASL_PLAIN"
        elif mech_upper == "OAUTHBEARER":
            effective_auth_mode = "GMK_OAUTHBEARER"

    if args.dry_run:
        redacted_preview = {
            "auth_mode": effective_auth_mode,
            "security_protocol": args.security_protocol,
            "username": args.username or "<empty>",
            "password": "***" if args.password else "<empty>",
            "truststore_gcs_uri": args.truststore_gcs_uri or "<empty>",
            "truststore_password": "***" if args.truststore_password else "<empty>",
            "schema_registry_url": args.schema_registry_url,
            "schema_registry_auth_mode": args.schema_registry_auth_mode,
        }
        logger.info(
            "[DRY-RUN] Validated PAM JSON secret structure for projects/%s/secrets/%s:\n%s",
            args.project_id,
            args.secret_name,
            json.dumps(redacted_preview, indent=2),
        )
        return

    upsert_pam_json_secret(
        project_id=args.project_id,
        pam_secret_name=args.secret_name,
        auth_mode=effective_auth_mode,
        security_protocol=args.security_protocol,
        username=args.username,
        password=args.password,
        truststore_gcs_uri=args.truststore_gcs_uri,
        truststore_password=args.truststore_password,
        schema_registry_url=args.schema_registry_url,
        schema_registry_auth_mode=args.schema_registry_auth_mode,
        schema_registry_api_key=args.schema_registry_api_key,
        schema_registry_api_secret=args.schema_registry_api_secret,
    )


if __name__ == "__main__":
    main()
