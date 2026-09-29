variable "project" {
  type    = string
  default = "genai"
}

variable "postgres_password" {
  type      = string
  sensitive = true
  default   = "localdev-only"
}

variable "docker_host" {
  type        = string
  default     = "unix:///var/run/docker.sock"
  description = "Docker socket. Colima: unix:///Users/<you>/.colima/default/docker.sock"
}

variable "s3_access_key" {
  type    = string
  default = "local"
}

variable "s3_secret_key" {
  type      = string
  sensitive = true
  default   = "localdev-only"
}
