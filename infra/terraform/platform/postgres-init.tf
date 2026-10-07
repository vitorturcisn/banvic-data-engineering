resource "kubernetes_config_map_v1" "postgres_init" {
  metadata {
    name      = "banvic-postgres-init"
    namespace = var.namespace
  }

  data = {
    "init.sh" = <<-EOT
      #!/bin/sh
      set -eu

      export PGPASSWORD="$POSTGRES_ADMIN_PASSWORD"

      echo "Aguardando PostgreSQL..."
      until pg_isready \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_ADMIN_USER" \
        -d postgres
      do
        sleep 2
      done

      echo "PostgreSQL disponível."

      ADMIN_ARGS="-h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_ADMIN_USER"

      echo "Criando/atualizando role airflow..."

      AIRFLOW_EXISTS=$(
        psql $ADMIN_ARGS -d postgres -tAc \
          "SELECT 1 FROM pg_roles WHERE rolname='airflow';"
      )

      if [ "$AIRFLOW_EXISTS" = "1" ]; then
        psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
          -c "ALTER ROLE airflow LOGIN PASSWORD '$AIRFLOW_METADATA_PASSWORD';"
      else
        psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
          -c "CREATE ROLE airflow LOGIN PASSWORD '$AIRFLOW_METADATA_PASSWORD';"
      fi

      echo "Criando/atualizando role banvic_ingest..."

      INGEST_EXISTS=$(
        psql $ADMIN_ARGS -d postgres -tAc \
          "SELECT 1 FROM pg_roles WHERE rolname='banvic_ingest';"
      )

      if [ "$INGEST_EXISTS" = "1" ]; then
        psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
          -c "ALTER ROLE banvic_ingest LOGIN PASSWORD '$POSTGRES_INGEST_PASSWORD';"
      else
        psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
          -c "CREATE ROLE banvic_ingest LOGIN PASSWORD '$POSTGRES_INGEST_PASSWORD';"
      fi

      echo "Criando database airflow..."

      AIRFLOW_DB_EXISTS=$(
        psql $ADMIN_ARGS -d postgres -tAc \
          "SELECT 1 FROM pg_database WHERE datname='airflow';"
      )

      if [ "$AIRFLOW_DB_EXISTS" != "1" ]; then
        psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
          -c "CREATE DATABASE airflow OWNER airflow;"
      fi

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "ALTER DATABASE airflow OWNER TO airflow;"

      echo "Restringindo acesso ao database banvic..."

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "REVOKE ALL ON DATABASE banvic FROM PUBLIC;"

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "GRANT CONNECT ON DATABASE banvic TO banvic_ingest;"

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "GRANT TEMPORARY ON DATABASE banvic TO banvic_ingest;"

      echo "Restringindo acesso ao database airflow..."

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "REVOKE ALL ON DATABASE airflow FROM PUBLIC;"

      psql $ADMIN_ARGS -d postgres -v ON_ERROR_STOP=1 \
        -c "GRANT CONNECT ON DATABASE airflow TO airflow;"

      echo "Configurando schema raw..."

      psql $ADMIN_ARGS -d banvic -v ON_ERROR_STOP=1 \
        -c "CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION banvic_ingest;"

      psql $ADMIN_ARGS -d banvic -v ON_ERROR_STOP=1 \
        -c "ALTER SCHEMA raw OWNER TO banvic_ingest;"

      psql $ADMIN_ARGS -d banvic -v ON_ERROR_STOP=1 \
        -c "GRANT USAGE, CREATE ON SCHEMA raw TO banvic_ingest;"

      psql $ADMIN_ARGS -d banvic -v ON_ERROR_STOP=1 \
        -c "REVOKE ALL ON SCHEMA public FROM PUBLIC;"

      psql $ADMIN_ARGS -d banvic -v ON_ERROR_STOP=1 \
        -c "ALTER DEFAULT PRIVILEGES FOR ROLE banvic_ingest IN SCHEMA raw GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO banvic_ingest;"

      echo "Provisionamento do PostgreSQL concluído."
    EOT
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_job_v1" "postgres_init" {
  metadata {
    name      = "banvic-postgres-init"
    namespace = var.namespace
  }

  spec {
    backoff_limit = 10

    template {
      metadata {
        annotations = {
          "banvic/config-hash" = sha256(
            kubernetes_config_map_v1.postgres_init.data["init.sh"]
          )
        }
      }

      spec {
        restart_policy = "OnFailure"

        container {
          name  = "postgres-init"
          image = "postgres:16.15-alpine3.24@sha256:075f7ba66bc9b3ce7d6b8b635208ff61cd7cf1a67d71ec530eec5d7ae0cbe571"

          command = [
            "/bin/sh",
            "/scripts/init.sh"
          ]

          env {
            name = "POSTGRES_HOST"

            value = kubernetes_service_v1.postgres.metadata[0].name
          }

          env {
            name  = "POSTGRES_PORT"
            value = "5432"
          }

          env {
            name = "POSTGRES_ADMIN_USER"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres_admin.metadata[0].name
                key  = "POSTGRES_USER"
              }
            }
          }

          env {
            name = "POSTGRES_ADMIN_PASSWORD"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres_admin.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }

          env {
            name = "POSTGRES_INGEST_PASSWORD"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres_ingest.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }

          env {
            name = "AIRFLOW_METADATA_PASSWORD"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.airflow_metadata.metadata[0].name
                key  = "password"
              }
            }
          }

          volume_mount {
            name       = "scripts"
            mount_path = "/scripts"
            read_only  = true
          }
        }

        volume {
          name = "scripts"

          config_map {
            name = kubernetes_config_map_v1.postgres_init.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_deployment_v1.postgres,
    kubernetes_service_v1.postgres,
    kubernetes_secret_v1.postgres_admin,
    kubernetes_secret_v1.postgres_ingest,
    kubernetes_secret_v1.airflow_metadata,
    kubernetes_config_map_v1.postgres_init
  ]

  lifecycle {
    replace_triggered_by = [
      kubernetes_config_map_v1.postgres_init
    ]
  }
}