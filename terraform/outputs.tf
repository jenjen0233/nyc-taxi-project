output "bucket_name" {
  description = "Name of the GCS bucket for taxi data"
  value       = google_storage_bucket.nyc_taxi_bucket.name
}

output "bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.nyc_taxi_bucket.url
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID for raw taxi data"
  value       = google_bigquery_dataset.nyc_taxi_dataset.dataset_id
}

output "bigquery_dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.nyc_taxi_dataset.location
}

output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP Region"
  value       = var.region
}