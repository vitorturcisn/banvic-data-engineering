resource "kubernetes_persistent_volume_v1" "postgres_data" {
  metadata {
    name = "banvic-postgres-pv"
  }

  spec {
    capacity = {
      storage = "2Gi"
    }

    access_modes                     = ["ReadWriteOnce"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = "banvic-hostpath"

    persistent_volume_source {
      host_path {
        path = "/var/local/banvic-postgres-data"
        type = "Directory"
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "postgres_data" {
  metadata {
    name      = "banvic-postgres-pvc"
    namespace = var.namespace
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "banvic-hostpath"
    volume_name        = kubernetes_persistent_volume_v1.postgres_data.metadata[0].name

    resources {
      requests = {
        storage = "2Gi"
      }
    }
  }

  depends_on = [
    kubernetes_namespace_v1.banvic,
    kubernetes_persistent_volume_v1.postgres_data
  ]
}

resource "kubernetes_persistent_volume_v1" "airflow_logs" {
  metadata {
    name = "airflow-logs-pv"
  }

  spec {
    capacity = {
      storage = "2Gi"
    }

    access_modes                     = ["ReadWriteOnce"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = "banvic-airflow-logs"

    persistent_volume_source {
      host_path {
        path = "/var/local/airflow-logs"
        type = "Directory"
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "airflow_logs" {
  metadata {
    name      = "airflow-logs-pvc"
    namespace = var.namespace
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "banvic-airflow-logs"
    volume_name        = kubernetes_persistent_volume_v1.airflow_logs.metadata[0].name

    resources {
      requests = {
        storage = "2Gi"
      }
    }
  }

  depends_on = [
    kubernetes_namespace_v1.banvic,
    kubernetes_persistent_volume_v1.airflow_logs
  ]
}

resource "kubernetes_persistent_volume_v1" "raw_data" {
  metadata {
    name = "banvic-raw-data-pv"
  }

  spec {
    capacity = {
      storage = "10Mi"
    }

    access_modes                     = ["ReadWriteOnce"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = "banvic-raw-data"

    persistent_volume_source {
      host_path {
        path = "/var/local/banvic-data/raw"
        type = "Directory"
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "raw_data" {
  metadata {
    name      = "banvic-raw-data-pvc"
    namespace = var.namespace
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "banvic-raw-data"
    volume_name        = kubernetes_persistent_volume_v1.raw_data.metadata[0].name

    resources {
      requests = {
        storage = "10Mi"
      }
    }
  }

  depends_on = [
    kubernetes_namespace_v1.banvic,
    kubernetes_persistent_volume_v1.raw_data
  ]
}
