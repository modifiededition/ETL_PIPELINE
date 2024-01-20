terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.9.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.location
}


resource "google_storage_bucket" "demo-bucket-terra" {
  name          = "${var.project}_${var.gcs_bucket_name}"
  location      = var.location
  force_destroy = true

  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
}

resource "google_dataproc_cluster" "cluster" {
  name   = "${var.project}-${var.gcs_dataproc_class}"
  region = var.location

  cluster_config {

    master_config {
      num_instances = var.gcs_dataproc_number_of_master_nodes
      machine_type  = var.gcs_dataproc_machine_type

    }
    worker_config {
      num_instances = var.gcs_dataproc_cluster_number_of_worker_nodes
      machine_type  = var.gcs_dataproc_machine_type

    }
    lifecycle_config {
      idle_delete_ttl = var.gcs_dataproc_lifecycle
    }
  }
}