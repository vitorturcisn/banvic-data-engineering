resource "random_password" "postgres_admin" {
  length  = 32
  special = false
}

resource "random_password" "postgres_ingest" {
  length  = 32
  special = false
}

resource "random_password" "airflow_metadata" {
  length  = 32
  special = false
}

resource "random_password" "airflow_admin" {
  length  = 32
  special = false
}

resource "random_password" "airflow_api_secret" {
  length  = 64
  special = false
}

resource "random_password" "airflow_jwt_secret" {
  length  = 64
  special = false
}

locals {
  postgres_admin_password = coalesce(
    var.postgres_admin_password,
    random_password.postgres_admin.result
  )

  postgres_ingest_password = coalesce(
    var.postgres_ingest_password,
    random_password.postgres_ingest.result
  )

  airflow_metadata_password = coalesce(
    var.airflow_metadata_password,
    random_password.airflow_metadata.result
  )

  airflow_admin_password = coalesce(
    var.airflow_admin_password,
    random_password.airflow_admin.result
  )

  postgres_host = "banvic-postgres.${var.namespace}.svc.cluster.local"
}

resource "random_id" "airflow_fernet" {
  byte_length = 32
}
