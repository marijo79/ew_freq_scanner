resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.project_name}-cluster"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = aws_subnet.public[*].id
    security_groups = [aws_security_group.msk_brokers.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_volume_size
      }
    }

    connectivity_info {
      public_access {
        type = "SERVICE_PROVIDED_EIPS"
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn

    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-cluster"
  })
}
