variable "project" {
  description = "Project"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "asia-south2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "taxi_rides_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "etl_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_dataproc_class" {
  description = "Cluster Manager for GCS"
  default     = "spark-cluster"
}

variable "gcs_dataproc_cluster_network" {
  description = "Cluster Manager Network for GCS"
  default     = "spark_network"
}


variable "gcs_dataproc_number_of_master_nodes" {
  description = "Number of master nodes to create in your cluster"
  default     = "1"
}

variable "gcs_dataproc_cluster_number_of_worker_nodes" {
  description = "Number of worker nodes to create in your cluster"
}

variable "gcs_dataproc_machine_type" {
  description = "Cluster manager and worker machine type"
  default     = "e2-standard-2"
}


variable "gcs_dataproc_lifecycle" {
  description = "The duration to keep the cluster alive while idling."
}
