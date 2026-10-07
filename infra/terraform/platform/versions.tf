terraform {
  required_version = ">= 1.6.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "3.2.1"
    }

    helm = {
      source  = "hashicorp/helm"
      version = "3.3.0"
    }

    random = {
      source  = "hashicorp/random"
      version = "3.9.0"
    }
  }
}
