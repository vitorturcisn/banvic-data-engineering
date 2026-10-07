resource "terraform_data" "airflow_image" {
  triggers_replace = [
    var.airflow_image,
    filesha256("${path.root}/../../airflow/Dockerfile"),
    filesha256("${path.root}/../../airflow/requirements.txt"),
    filesha256("${path.root}/../../../dags/banvic_elt.py"),
    filesha256("${path.root}/../../../meltano/meltano.yml"),
    filesha256("${path.root}/../../../meltano/config/csv_files_definition.json"),
  ]

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-NoProfile", "-Command"]

    command = <<-EOT
      $ErrorActionPreference = "Stop"

      Write-Host "Construindo imagem ${var.airflow_image}..."

      docker build `
        -t "${var.airflow_image}" `
        -f "${path.root}/../../airflow/Dockerfile" `
        "${path.root}/../../.."

      if ($LASTEXITCODE -ne 0) {
        throw "Falha no docker build."
      }

      Write-Host "Carregando imagem no Kind..."

      kind load docker-image `
        "${var.airflow_image}" `
        --name "${var.cluster_name}"

      if ($LASTEXITCODE -ne 0) {
        throw "Falha ao carregar a imagem no Kind."
      }

      Write-Host "Imagem do Airflow pronta."
    EOT
  }
}

resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  namespace  = var.namespace
  version    = var.airflow_chart_version

  values = [
    file("${path.root}/../../airflow/values.yaml")
  ]

  wait          = true
  wait_for_jobs = true
  atomic        = true
  timeout       = 900

  depends_on = [
    terraform_data.airflow_image,
    kubernetes_job_v1.postgres_init,
    kubernetes_persistent_volume_claim_v1.raw_data,
    kubernetes_persistent_volume_claim_v1.airflow_logs
  ]
}

resource "kubernetes_job_v1" "airflow_admin" {
  metadata {
    name      = "airflow-admin-bootstrap"
    namespace = var.namespace
  }

  spec {
    backoff_limit = 10

    template {
      metadata {
        labels = {
          app = "airflow-admin-bootstrap"
        }
      }

      spec {
        # O Job não precisa acessar a API do Kubernetes.
        automount_service_account_token = false
        restart_policy                  = "OnFailure"

        container {
          name  = "airflow-admin"
          image = var.airflow_image

          command = [
            "/bin/bash",
            "-c"
          ]

          args = [
            <<-EOT
              set -euo pipefail

              echo "Aguardando o banco de metadados do Airflow..."

              until airflow db check; do
                sleep 5
              done

              echo "Banco de metadados disponível."

              if airflow users list | grep -q "admin@banvic.local"; then
                echo "Usuário admin já existe."
                exit 0
              fi

              echo "Criando usuário admin..."

              airflow users create \
                --username admin \
                --firstname Admin \
                --lastname Banvic \
                --role Admin \
                --email admin@banvic.local \
                --password "$AIRFLOW_ADMIN_PASSWORD"

              echo "Usuário admin criado."
            EOT
          ]

          env {
            name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.airflow_metadata.metadata[0].name
                key  = "connection"
              }
            }
          }

          env {
            name = "AIRFLOW_ADMIN_PASSWORD"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.airflow_admin.metadata[0].name
                key  = "password"
              }
            }
          }

          env {
            name  = "AIRFLOW__CORE__AUTH_MANAGER"
            value = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "128Mi"
            }

            limits = {
              cpu    = "250m"
              memory = "256Mi"
            }
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "5m"
  }

  depends_on = [
    helm_release.airflow,
    kubernetes_secret_v1.airflow_admin,
    kubernetes_secret_v1.airflow_metadata
  ]

  lifecycle {
    replace_triggered_by = [
      kubernetes_secret_v1.airflow_admin
    ]
  }
}