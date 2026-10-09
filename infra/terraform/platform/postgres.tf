
resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "banvic-postgres"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "banvic-postgres"
    }

    port {
      name        = "postgres"
      port        = 5432
      target_port = 5432
      protocol    = "TCP"
    }

    type = "ClusterIP"
  }

  depends_on = [kubernetes_namespace_v1.banvic]
}

resource "kubernetes_deployment_v1" "postgres" {
  metadata {
    name      = "banvic-postgres"
    namespace = var.namespace
  }

  spec {
    replicas = 1

    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = {
        app = "banvic-postgres"
      }
    }

    template {
      metadata {
        labels = {
          app = "banvic-postgres"
        }
      }

      spec {
        container {
          name  = "postgres"
          image = "postgres:16.15-alpine3.24@sha256:075f7ba66bc9b3ce7d6b8b635208ff61cd7cf1a67d71ec530eec5d7ae0cbe571"

          port {
            container_port = 5432
          }

          env_from {
            secret_ref {
              name = kubernetes_secret_v1.postgres_admin.metadata[0].name
            }
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }

            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          volume_mount {
            name       = "postgres-data"
            mount_path = "/var/lib/postgresql/data"
          }

          startup_probe {
            exec {
              command = [
                "/bin/sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""
              ]
            }

            period_seconds    = 10
            timeout_seconds   = 5
            failure_threshold = 60
          }

          readiness_probe {
            exec {
              command = [
                "/bin/sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""
              ]
            }

            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          liveness_probe {
            exec {
              command = [
                "/bin/sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""
              ]
            }

            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 6
          }
        }

        volume {
          name = "postgres-data"

          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.postgres_data.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_secret_v1.postgres_admin,
    kubernetes_persistent_volume_claim_v1.postgres_data
  ]
}
