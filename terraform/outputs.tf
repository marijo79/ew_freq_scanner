output "cluster_arn" {
  description = "ARN of the MSK cluster."
  value       = aws_msk_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap broker connection string for SASL/IAM clients."
  value       = aws_msk_cluster.this.bootstrap_brokers_public_sasl_iam
}

output "kms_key_arn" {
  description = "ARN of the customer-managed KMS key used for encryption at rest."
  value       = aws_kms_key.msk.arn
}

output "producer_access_key_id" {
  description = "Access key ID for the producer IAM user."
  value       = aws_iam_access_key.producer.id
}

output "producer_secret_access_key" {
  description = "Secret access key for the producer IAM user. Sensitive — also stored in plaintext in local Terraform state; treat state like a credentials file."
  value       = aws_iam_access_key.producer.secret
  sensitive   = true
}
