# ---------------- Terraform + Providers ----------------
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws     = { source = "hashicorp/aws",     version = ">= 5.40.0" }
    archive = { source = "hashicorp/archive", version = ">= 2.4.0" }
  }
}

provider "aws" {
  region = var.region
}

# ---------------- Vars ----------------
variable "region" {
  type    = string
  default = "us-east-1"
}

# S3 canary artifact source (already exists)
variable "artifact_bucket" {
  type    = string
  default = "canary-output-rainthos-009"
}

# Prefix up to the canary name (no date). Example: canary/us-east-1/clone2rainthos
variable "artifact_prefix" {
  type    = string
  default = "canary/us-east-1/clone2rainthos"
}

# Browser folder to use (must match S3 folder name exactly)
variable "only_browser" {
  type    = string
  default = "CHROME"
}

# S3 destination for generated report (already exists)
variable "reports_bucket" {
  type    = string
  default = "lambda-output-report-000000987123"
}

variable "reports_prefix" {
  type    = string
  default = "uptime"
}

# Report labeling + rules
variable "company_name" {
  type    = string
  default = "LogicEase Solutions Inc."
}

variable "service_name" {
  type    = string
  default = "CDP"
}

variable "client_name" {
  type    = string
  default = "client"
}

variable "slo_target" {
  type    = number
  default = 99.9
}

variable "fail_streak" {
  type    = number
  default = 3
}

variable "treat_missing" {
  type    = bool
  default = false
}

variable "brand_strapline" {
  type    = string
  default = "Service Level Report"
}

variable "name_prefix" {
  type    = string
  default = "uptime-artifact"
}

# Lambda architecture
variable "lambda_architecture" {
  type    = string
  default = "x86_64"
  validation {
    condition     = contains(["x86_64", "arm64"], var.lambda_architecture)
    error_message = "lambda_architecture must be x86_64 or arm64."
  }
}
