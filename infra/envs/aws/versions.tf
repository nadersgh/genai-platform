terraform {
  required_version = ">= 1.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Uncomment after bootstrapping a state bucket (see docs/ROADMAP.md, week 1).
  # backend "s3" {
  #   bucket       = "CHANGE-ME-tfstate"
  #   key          = "genai-platform/aws.tfstate"
  #   region       = "ca-central-1"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = var.project, managed_by = "opentofu" }
  }
}
