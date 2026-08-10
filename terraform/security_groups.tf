resource "aws_security_group" "msk_brokers" {
  name        = "${var.project_name}-msk-brokers"
  description = "MSK broker access for ${var.project_name}"
  vpc_id      = aws_vpc.this.id

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-msk-brokers"
  })
}

# SASL/IAM broker port, reachable only from the explicitly allow-listed CIDR(s).
resource "aws_security_group_rule" "sasl_iam_ingress" {
  type              = "ingress"
  from_port         = 9098
  to_port           = 9098
  protocol          = "tcp"
  cidr_blocks       = var.allowed_cidr_blocks
  security_group_id = aws_security_group.msk_brokers.id
  description       = "SASL/IAM broker access"
}

resource "aws_security_group_rule" "egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.msk_brokers.id
  description       = "Unrestricted egress"
}