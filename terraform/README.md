# Kafka infrastructure (AWS MSK)

Terraform for the MSK cluster that the freqscan producer streams spectrum data to. This
is infra only — Kafka topic creation and the producer code itself are handled elsewhere.

Before running any of this, complete the one-time local machine + AWS account setup in
[SETUP.md](SETUP.md).

## Design

- **Dedicated VPC** (`vpc.tf`) — created from scratch, not reusing existing AWS resources.
  Two public subnets across two AZs (IGW + route table), because MSK's public broker
  access requires subnets that route to an internet gateway.
- **Public broker access** — the producer runs off-AWS (this or another
  internet-connected machine), so brokers need public endpoints rather than VPC-only
  access. Enabled via `connectivity_info.public_access` on the broker node group.
- **Auth: SASL/IAM**, not TLS mutual auth. MSK's native mTLS needs a private CA (AWS
  Private CA / ACM PCA) that costs ~$400/month in general-purpose mode — far more than
  the cluster itself. SASL/IAM needs no CA; transport is still TLS-encrypted underneath
  regardless of auth mechanism. Broker port for SASL/IAM is 9098.
- **Encryption at rest**: customer-managed KMS key (`kms.tf`), not an AWS-managed key.
- **Network access** (`security_groups.tf`): a security group open on 9098 only to
  `var.allowed_cidr_blocks`, which defaults to `203.0.113.0/24` — an RFC 5737
  documentation-only range that matches no real host. This must be overridden with the
  producer's actual public IP/CIDR before the cluster is reachable; see
  `terraform.tfvars.example`.
- **Cluster size** (`msk.tf`): small/dev tier — 2 brokers, `kafka.t3.small`, 100GB EBS
  each. Sized for the ~3.5 MB/s combined unfiltered sweep throughput measured during the
  producer design work, not production load.
- **Producer IAM identity** (`iam.tf`): a dedicated IAM user + access key, scoped to
  `kafka-cluster:Connect`/`DescribeCluster` on the cluster and `WriteData`/`DescribeTopic`
  on any topic under it (topic-level scoping deferred until the topic/schema design is
  finalized).
- **State**: local file, not S3+DynamoDB remote state — acceptable for solo prototyping.
  The state file contains the producer IAM user's **plaintext secret access key** — it's
  gitignored (`terraform/*.tfstate*`) and must be treated like a credentials file, never
  committed or shared.
- **Region**: `eu-central-1` (Frankfurt).

## Usage

```bash
cd terraform
terraform init
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars — at minimum set allowed_cidr_blocks to your real IP/CIDR
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

`terraform plan`/`apply` create real, billed AWS infrastructure — run them yourself
rather than expecting them to run unattended.

Retrieve the producer credentials after apply:

```bash
terraform output producer_access_key_id
terraform output -raw producer_secret_access_key
terraform output bootstrap_brokers_sasl_iam
```

## Out of scope for this pass

- Kafka topic creation (partitions/replication depend on the signal-detection/schema
  design, still unresolved).
- The producer code itself.
- Tightening the Terraform IAM user's `AdministratorAccess` (see SETUP.md) down to a
  scoped policy.
