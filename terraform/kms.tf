resource "aws_kms_key" "msk" {
  description             = "Customer-managed key for ${var.project_name} MSK encryption at rest"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-msk-kms"
  })
}

resource "aws_kms_alias" "msk" {
  name          = "alias/${var.project_name}-msk"
  target_key_id = aws_kms_key.msk.key_id
}