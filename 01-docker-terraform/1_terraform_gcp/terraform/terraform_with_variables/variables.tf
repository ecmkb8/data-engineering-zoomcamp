variable "credentials" {
  description = "My Credentials"
<<<<<<< HEAD:week_1_basics_n_setup/1_terraform_gcp/terraform/terraform_with_variables/variables.tf
  default     = "~/gcloud_creds/dtc-de-course-406816-6d128f9b1cca.json"
=======
  default     = "/Users/evanmoore/gcloud_creds/dtc-de-course-406816-6d128f9b1cca.json"
>>>>>>> main:01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables/variables.tf
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "dtc-de-course-406816"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-west1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "taxi_bigquery"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
<<<<<<< HEAD:week_1_basics_n_setup/1_terraform_gcp/terraform/terraform_with_variables/variables.tf
  default     = "dtc-de-evan"
=======
  default     = "dtc_de_2024_bucket"
>>>>>>> main:01-docker-terraform/1_terraform_gcp/terraform/terraform_with_variables/variables.tf
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}