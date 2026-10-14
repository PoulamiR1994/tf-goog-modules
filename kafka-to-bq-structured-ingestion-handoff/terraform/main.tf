terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.12.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 6.12.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# 1. Google Cloud Storage Landing, Checkpoint & Iceberg Warehouse Bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "landing_zone" {
  name                        = var.landing_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED"
    }
  }

  labels = {
    workload    = "kafka-to-bq-iceberg"
    environment = "poc"
    pattern     = "track3-dataproc-serverless"
  }
}

# ------------------------------------------------------------------------------
# 2. Custom Spark Runtime IAM Service Account & Least-Privilege Roles
# ------------------------------------------------------------------------------
resource "google_service_account" "spark_sa" {
  account_id   = var.service_account_id
  display_name = "Dataproc Serverless Spark Kafka-to-BQ SA"
  description  = "Zero-trust customer-managed runtime SA for Kafka to BigQuery Iceberg ingestion"
}

resource "google_storage_bucket_iam_member" "sa_storage_admin" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_gmk_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_gmk_sr_viewer" {
  project = var.project_id
  role    = "roles/managedkafka.schemaRegistryViewer"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_biglake_admin" {
  project = var.project_id
  role    = "roles/biglake.admin"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_project_iam_member" "sa_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.spark_sa.email}"
}

# ------------------------------------------------------------------------------
# 3. Zero-Public-Internet VPC Network, /24 Subnets with PGA, DNS & Firewalls
# ------------------------------------------------------------------------------
resource "google_compute_network" "kafka_bq_vpc" {
  name                    = var.network_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "dataproc_subnet" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.kafka_bq_vpc.id
  private_ip_google_access = true
}

resource "google_compute_subnetwork" "gmk_subnet" {
  name                     = var.gmk_subnet_name
  ip_cidr_range            = var.gmk_subnet_cidr
  region                   = var.region
  network                  = google_compute_network.kafka_bq_vpc.id
  private_ip_google_access = true
}

# Static Route for Restricted Google Access (199.36.153.4/30 -> default-internet-gateway)
resource "google_compute_route" "restricted_googleapis_route" {
  name             = "rt-restricted-googleapis"
  network          = google_compute_network.kafka_bq_vpc.name
  dest_range       = "199.36.153.4/30"
  next_hop_gateway = "default-internet-gateway"
  priority         = 1000
}

# PrivateZone 1: googleapis.com -> restricted.googleapis.com (199.36.153.4/30)
resource "google_dns_managed_zone" "restricted_googleapis_zone" {
  name        = "dz-restricted-googleapis"
  dns_name    = "googleapis.com."
  description = "Restricted Google Access DNS zone for VPC-SC compliance"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.kafka_bq_vpc.id
    }
  }
}

resource "google_dns_record_set" "restricted_a_record" {
  name         = "restricted.googleapis.com."
  managed_zone = google_dns_managed_zone.restricted_googleapis_zone.name
  type         = "A"
  ttl          = 300
  rrdatas      = ["199.36.153.4", "199.36.153.5", "199.36.153.6", "199.36.153.7"]
}

resource "google_dns_record_set" "googleapis_cname" {
  name         = "*.googleapis.com."
  managed_zone = google_dns_managed_zone.restricted_googleapis_zone.name
  type         = "CNAME"
  ttl          = 300
  rrdatas      = ["restricted.googleapis.com."]
}

# PrivateZone 2: pkg.dev & gcr.io -> restricted.googleapis.com
resource "google_dns_managed_zone" "restricted_pkg_dev_zone" {
  name        = "dz-restricted-pkg-dev"
  dns_name    = "pkg.dev."
  description = "Restricted Google Access DNS zone for Artifact Registry (pkg.dev) under VPC-SC"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.kafka_bq_vpc.id
    }
  }
}

resource "google_dns_record_set" "pkg_dev_cname" {
  name         = "*.pkg.dev."
  managed_zone = google_dns_managed_zone.restricted_pkg_dev_zone.name
  type         = "CNAME"
  ttl          = 300
  rrdatas      = ["restricted.googleapis.com."]
}

resource "google_dns_managed_zone" "restricted_gcr_io_zone" {
  name        = "dz-restricted-gcr-io"
  dns_name    = "gcr.io."
  description = "Restricted Google Access DNS zone for Container Registry (gcr.io) under VPC-SC"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.kafka_bq_vpc.id
    }
  }
}

resource "google_dns_record_set" "gcr_io_cname" {
  name         = "*.gcr.io."
  managed_zone = google_dns_managed_zone.restricted_gcr_io_zone.name
  type         = "CNAME"
  ttl          = 300
  rrdatas      = ["restricted.googleapis.com."]
}

# Firewall Rule 1: fw-allow-google-apis-egress
resource "google_compute_firewall" "fw_allow_google_apis_egress" {
  name               = "fw-allow-google-apis-egress"
  network            = google_compute_network.kafka_bq_vpc.name
  direction          = "EGRESS"
  destination_ranges = ["199.36.153.4/30"]

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }
}

# Firewall Rule 2: fw-allow-gmk-psc-brokers
resource "google_compute_firewall" "fw_allow_gmk_psc_brokers" {
  name               = "fw-allow-gmk-psc-brokers"
  network            = google_compute_network.kafka_bq_vpc.name
  direction          = "EGRESS"
  destination_ranges = [var.gmk_subnet_cidr]

  allow {
    protocol = "tcp"
    ports    = ["9092", "9192"]
  }
}

# Firewall Rule 3: fw-allow-onprem-fab-kafka-egress
resource "google_compute_firewall" "fw_allow_onprem_fab_kafka_egress" {
  name               = "fw-allow-onprem-fab-kafka-egress"
  network            = google_compute_network.kafka_bq_vpc.name
  direction          = "EGRESS"
  destination_ranges = [var.onprem_kafka_cidr]

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093", "8081"]
  }
}

# Firewall Rule 4: fw-allow-dataproc-intra-subnet (Ingress & Egress - )
resource "google_compute_firewall" "fw_allow_dataproc_intra_subnet" {
  name          = "fw-allow-dataproc-intra-subnet"
  network       = google_compute_network.kafka_bq_vpc.name
  direction     = "INGRESS"
  source_ranges = [var.subnet_cidr]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }
}

resource "google_compute_firewall" "fw_allow_dataproc_intra_subnet_egress" {
  name               = "fw-allow-dataproc-intra-subnet-egress"
  network            = google_compute_network.kafka_bq_vpc.name
  direction          = "EGRESS"
  destination_ranges = [var.subnet_cidr]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }
}


# ------------------------------------------------------------------------------
# 4. BigQuery Bronze & Governance Datasets, BigLake Connection & Scoped IAM
# ------------------------------------------------------------------------------
resource "google_bigquery_connection" "biglake_conn" {
  connection_id = var.biglake_connection_id
  location      = var.region
  friendly_name = "BigLake Apache Iceberg Lakehouse Connection"
  cloud_resource {}
}

resource "google_bigquery_connection_iam_member" "sa_bq_conn_user" {
  project       = var.project_id
  location      = google_bigquery_connection.biglake_conn.location
  connection_id = google_bigquery_connection.biglake_conn.connection_id
  role          = "roles/bigquery.connectionUser"
  member        = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_storage_bucket_iam_member" "biglake_conn_storage_admin" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_bigquery_connection.biglake_conn.cloud_resource[0].service_account_id}"
}

resource "google_storage_bucket_iam_member" "biglake_conn_bucket_reader" {
  bucket = google_storage_bucket.landing_zone.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_bigquery_connection.biglake_conn.cloud_resource[0].service_account_id}"
}

resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id                 = var.bronze_dataset_id
  location                   = var.region
  description                = "Bronze schema-mapped Apache Iceberg dataset for Kafka Fab telemetry"
  storage_billing_model      = "PHYSICAL"
  delete_contents_on_destroy = false

  labels = {
    env     = "poc"
    layer   = "bronze"
    domain  = "fab-telemetry"
    pattern = "kafka-to-bq"
  }
}

resource "google_bigquery_dataset" "governance_dataset" {
  dataset_id                 = var.governance_dataset_id
  location                   = var.region
  description                = "Operational Apache Iceberg governance and audit dataset"
  delete_contents_on_destroy = false

  labels = {
    env     = "poc"
    layer   = "governance"
    domain  = "fab-telemetry"
    pattern = "kafka-to-bq"
  }
}

resource "google_bigquery_dataset_iam_member" "sa_bq_bronze_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bronze_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "sa_bq_governance_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.governance_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.spark_sa.email}"
}

# ------------------------------------------------------------------------------
# 5. Secret Manager Placeholders & Secret-Scoped IAM for PAM Credentials
# ------------------------------------------------------------------------------
resource "google_secret_manager_secret" "kafka_pam_credentials" {
  secret_id = "kafka-pam-credentials"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kafka_sasl_username" {
  secret_id = "kafka-sasl-username"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kafka_sasl_password" {
  secret_id = "kafka-sasl-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kafka_truststore_password" {
  secret_id = "kafka-truststore-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "sa_secret_accessor" {
  for_each = {
    pam_credentials     = google_secret_manager_secret.kafka_pam_credentials.secret_id
    sasl_username       = google_secret_manager_secret.kafka_sasl_username.secret_id
    sasl_password       = google_secret_manager_secret.kafka_sasl_password.secret_id
    truststore_password = google_secret_manager_secret.kafka_truststore_password.secret_id
  }
  project   = var.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.spark_sa.email}"
}

