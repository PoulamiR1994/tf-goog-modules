variable "project_id" {
  type        = string
  description = "The GCP project ID"
}

variable "region" {
  type        = string
  description = "The primary GCP region for Dataproc Serverless, GMK, and BigQuery"
  default     = "us-central1"
}

variable "landing_bucket_name" {
  type        = string
  description = "The GCS bucket name for Apache Iceberg Bronze storage, Spark checkpoints, and schemas"
}

variable "service_account_id" {
  type        = string
  description = "The ID of the custom Dataproc Serverless Spark runtime service account"
  default     = "sa-spark-kafka-bq"
}

variable "network_name" {
  type        = string
  description = "VPC network name for Dataproc Serverless and GMK PSC endpoints"
  default     = "kafka-bq-vpc"
}

variable "subnet_name" {
  type        = string
  description = "Dataproc Serverless compute subnet name with Private Google Access enabled"
  default     = "snet-dataproc-usc1"
}

variable "subnet_cidr" {
  type        = string
  description = "CIDR block for the Dataproc subnet (minimum /24 with 251 usable IPs per )"
  default     = "10.20.0.0/24"
}

variable "gmk_subnet_name" {
  type        = string
  description = "Subnet name for Google Cloud Managed Service for Apache Kafka (GMK) PSC endpoints"
  default     = "snet-gmk-usc1"
}

variable "gmk_subnet_cidr" {
  type        = string
  description = "CIDR block for the GMK PSC connected subnet"
  default     = "10.20.1.0/24"
}

variable "onprem_kafka_cidr" {
  type        = string
  description = "CIDR block for External / On-Premises Fab Kafka brokers over Cloud Interconnect / VPN"
  default     = "10.100.0.0/16"
}

variable "bronze_dataset_id" {
  type        = string
  description = "Target BigQuery Bronze dataset following <storage_prefix>_<medallion_layer>_<domain>_<env>"
  default     = "bq_bronze_fab_telemetry_poc"
}

variable "governance_dataset_id" {
  type        = string
  description = "Target BigQuery Governance dataset for operational Iceberg audit tables"
  default     = "governance"
}

variable "biglake_connection_id" {
  type        = string
  description = "BigQuery Cloud Resource Connection ID for BigQuery-Managed Apache Iceberg tables"
  default     = "gcp_lakehouse_connection"
}

