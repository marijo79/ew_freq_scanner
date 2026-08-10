variable "aws_region" {
  description = "AWS region to provision the MSK cluster and its networking in."
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Prefix applied to all resource names/tags."
  type        = string
  default     = "freqscan"
}

variable "allowed_cidr_blocks" {
  description = <<-EOT
    CIDR block(s) allowed to reach the MSK brokers on the SASL/IAM port (9098).
    Defaults to a documentation-only reserved range (RFC 5737 TEST-NET-3) that
    matches no real host, so the cluster is unreachable until this is
    deliberately overridden with the producer's actual public IP/CIDR.
  EOT
  type        = list(string)
  default     = ["203.0.113.0/24"]
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "kafka_version" {
  description = "MSK-supported Kafka version."
  type        = string
  default     = "3.6.0"
}

variable "broker_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.t3.small"
}

variable "broker_count" {
  description = "Total number of broker nodes. Must equal (a multiple of) the number of AZs used, which is fixed at 2 in this design."
  type        = number
  default     = 2
}

variable "broker_ebs_volume_size" {
  description = "Per-broker EBS volume size in GiB."
  type        = number
  default     = 100
}
