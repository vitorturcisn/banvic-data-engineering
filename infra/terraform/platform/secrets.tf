resource "kubernetes_secret_v1" "postgres_admin" {
  metadata {
    name      = "banvic-postgres-admin"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    POSTGRES_USER     = "banvic"
    POSTGRES_PASSWORD = local.postgres_admin_password
    POSTGRES_DB       = "banvic"
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "postgres_ingest" {
  metadata {
    name      = "banvic-postgres-ingest"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    POSTGRES_USER     = "banvic_ingest"
    POSTGRES_PASSWORD = local.postgres_ingest_password
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_metadata" {
  metadata {
    name      = "airflow-metadata"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    connection = "postgresql+psycopg://airflow:${local.airflow_metadata_password}@${local.postgres_host}:5432/airflow"
    password   = local.airflow_metadata_password
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_ingest_connection" {
  metadata {
    name      = "banvic-airflow-ingest"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    DB_HOST                      = local.postgres_host
    DB_PORT                      = "5432"
    DB_NAME                      = "banvic"
    DB_USER                      = "banvic_ingest"
    DB_PASSWORD                  = local.postgres_ingest_password
    AIRFLOW_CONN_BANVIC_POSTGRES = "postgresql+psycopg://banvic_ingest:${local.postgres_ingest_password}@${local.postgres_host}:5432/banvic"
  }

  depends_on = [kubernetes_secret_v1.postgres_ingest]
}

resource "kubernetes_secret_v1" "airflow_api" {
  metadata {
    name      = "airflow-api-static-secret"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    api-secret-key = random_password.airflow_api_secret.result
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_jwt" {
  metadata {
    name      = "airflow-jwt-secret"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    jwt-secret = random_password.airflow_jwt_secret.result
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_admin" {
  metadata {
    name      = "airflow-admin-secret"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    username = "admin"
    password = local.airflow_admin_password
    email    = "admin@banvic.local"
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_fernet" {
  metadata {
    name      = "airflow-fernet"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    "fernet-key" = random_id.airflow_fernet.b64_url
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_secret_v1" "airflow_filesystem" {
  metadata {
    name      = "airflow-filesystem"
    namespace = var.namespace
  }

  type = "Opaque"

  data = {
    connection = jsonencode({
      conn_type = "fs"
      extra = {
        path = "/opt/airflow/data/raw"
      }
    })
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}
