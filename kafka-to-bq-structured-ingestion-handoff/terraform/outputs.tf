output "gcs_landing_bucket" {
  description = "The GCS bucket URI for Iceberg warehouse and Spark checkpoints"
  value       = "gs://${google_storage_bucket.landing_zone.name}"
}

output "service_account_email" {
  description = "The custom Dataproc Serverless Spark runtime service account email"
  value       = google_service_account.spark_sa.email
}

output "dataproc_subnet_id" {
  description = "The fully qualified subnet path for Dataproc Serverless batch submission"
  value       = google_compute_subnetwork.dataproc_subnet.id
}

output "gmk_subnet_id" {
  description = "The fully qualified subnet path for GMK PSC broker endpoints"
  value       = google_compute_subnetwork.gmk_subnet.id
}

output "bronze_dataset_id" {
  description = "The BigQuery Bronze dataset ID"
  value       = google_bigquery_dataset.bronze_dataset.dataset_id
}

output "biglake_connection_service_account" {
  description = "The Cloud Resource Connection service account ID for BigQuery-Managed Iceberg tables"
  value       = google_bigquery_connection.biglake_conn.cloud_resource[0].service_account_id
}
