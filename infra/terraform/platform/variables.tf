variable "cluster_name" {
  description = "Nome do cluster Kind."
  type        = string
  default     = "banvic"
}

variable "namespace" {
  description = "Namespace da POC BanVic."
  type        = string
  default     = "banvic"
}

variable "airflow_image" {
  description = "Imagem customizada do Airflow."
  type        = string
  default     = "banvic-airflow:2.1.0"
}

variable "airflow_chart_version" {
  description = "Versão do Apache Airflow Helm Chart."
  type        = string
  default     = "1.22.0"
}

variable "postgres_admin_password" {
  description = "Senha do usuário administrativo do PostgreSQL."
  type        = string
  sensitive   = true
  nullable    = true
  default     = null
}

variable "postgres_ingest_password" {
  description = "Senha do usuário de ingestão do PostgreSQL."
  type        = string
  sensitive   = true
  nullable    = true
  default     = null
}

variable "airflow_metadata_password" {
  description = "Senha do usuário do banco de metadados do Airflow."
  type        = string
  sensitive   = true
  nullable    = true
  default     = null
}

variable "airflow_admin_password" {
  description = "Senha do usuário administrador do Airflow."
  type        = string
  sensitive   = true
  nullable    = true
  default     = null
}
