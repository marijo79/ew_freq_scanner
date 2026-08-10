# IAM identity the freqscan producer authenticates as via SASL/IAM.
resource "aws_iam_user" "producer" {
  name = "${var.project_name}-producer"

  tags = local.common_tags
}

resource "aws_iam_access_key" "producer" {
  user = aws_iam_user.producer.name
}

locals {
  # MSK topic/group ARNs share the cluster ARN's <name>/<uuid> suffix, just
  # under a different resource type segment than "cluster/".
  msk_topic_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":topic/")
  msk_group_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":group/")
}

data "aws_iam_policy_document" "producer" {
  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_cluster.this.arn]
  }

  statement {
    sid = "ProduceToAnyTopic"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
    ]
    resources = ["${local.msk_topic_arn_prefix}/*"]
  }

  statement {
    sid       = "ConsumerGroup"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = ["${local.msk_group_arn_prefix}/*"]
  }
}

resource "aws_iam_user_policy" "producer" {
  name   = "${var.project_name}-producer-msk"
  user   = aws_iam_user.producer.name
  policy = data.aws_iam_policy_document.producer.json
}
