output "cluster_name" {
  description = "Nome do cluster Kind."
  value       = kind_cluster.banvic.name
}

output "kubeconfig_path" {
  description = "Caminho do kubeconfig utilizado pelo cluster."
  value       = kind_cluster.banvic.kubeconfig_path
}

output "cluster_endpoint" {
  description = "Endpoint do Kubernetes."
  value       = kind_cluster.banvic.endpoint
}
