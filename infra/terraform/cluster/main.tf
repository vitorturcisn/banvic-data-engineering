resource "kind_cluster" "banvic" {
  name            = var.cluster_name
  node_image      = var.node_image
  wait_for_ready  = true
  kubeconfig_path = pathexpand("~/.kube/config")

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"

      extra_mounts {
        host_path      = abspath("${path.module}/../../../data/raw")
        container_path = "/var/local/banvic-data/raw"
        read_only      = true
      }

      extra_mounts {
        host_path      = abspath("${path.module}/../../../runtime/airflow-logs")
        container_path = "/var/local/airflow-logs"
        read_only      = false
      }

      extra_mounts {
        host_path      = abspath("${path.module}/../../../runtime/postgres-data")
        container_path = "/var/local/banvic-postgres-data"
        read_only      = false
      }
    }
  }
}
