terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.2.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "nyc_taxi_bucket" {
  name     = var.bucket_name
  location = var.location

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "nyc_taxi_dataset" {
  dataset_id = var.dataset_name
  project    = var.project_id
  location   = var.location
}
