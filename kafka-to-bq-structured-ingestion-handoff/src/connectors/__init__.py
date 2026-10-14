"""
Pluggable Kafka Broker Connectors for PySpark Structured Streaming:
- `gmk_connector.py`: Google Cloud Managed Service for Apache Kafka (GMK) via keyless IAM `OAUTHBEARER`.
- `external_kafka_connector.py`: External / GCE VM / On-Premises Kafka via PAM Secret Manager (`SCRAM-SHA-512` / `PLAIN`).
"""
