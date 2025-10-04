variable "project_id" {
  description = "The ID of the project in which to create the resources."
  type        = string
}

variable "bucket_name" {
  description = "The name of the Google Cloud Storage bucket."
  type        = string
}

variable "dataset_name" {
  description = "The ID of the BigQuery dataset."
  type        = string
  default     = "nyc_taxi_dataset"
}
variable "region" {
  description = "The region in which to create the resources."
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The location for the BigQuery dataset."
  type        = string
  default     = "US"
}
