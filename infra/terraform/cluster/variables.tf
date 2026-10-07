variable "cluster_name" {
  description = "Nome do cluster Kind."
  type        = string
  default     = "banvic"
}

variable "node_image" {
  description = "Imagem do node Kind com digest fixado."
  type        = string

  default = "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
}
