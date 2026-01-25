variable "credentials" {
  description = "My Credentials"
  default     = "./.secrets/gcp-sa.json"

}

variable "project" {
  description = "Project"
  default     = "dtc-de-course-485207"

}

variable "region" {
  description = "Region"
  default     = "us-centrall"

}

variable "location" {
  description = "Project Location"
  default     = "US"

}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"

}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dtc-de-course-485207-terra-bucket"

}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
