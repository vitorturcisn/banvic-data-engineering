provider "kubernetes" {
  config_path    = pathexpand("~/.kube/config")
  config_context = "kind-${var.cluster_name}"
}

provider "helm" {
  kubernetes = {
    config_path    = pathexpand("~/.kube/config")
    config_context = "kind-${var.cluster_name}"
  }
}

provider "random" {}
