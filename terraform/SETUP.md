# Terraform/AWS setup (prerequisite for the freqscan Kafka infra)

One-time local machine + AWS account setup needed before any Terraform for the MSK/Kafka
cluster can be applied. This is manual, partly done in the AWS Console — Terraform can't
bootstrap its own first credentials.

## 1. Install Terraform (Ubuntu/DragonOS)

```bash
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform
terraform -version
```

## 2. Install AWS CLI v2

Ubuntu dropped the `awscli` apt package (confirmed gone even with `universe` enabled on
this DragonOS/24.04 system) — use the official installer instead:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install -y unzip && unzip awscliv2.zip && sudo ./aws/install
aws --version
rm -rf awscliv2.zip aws/
```

## 3. Create an IAM user for Terraform (AWS Console, manual)

No AWS IAM Identity Center (SSO) available on this account, so this uses a long-lived IAM
user access key instead of `aws configure sso`.

1. AWS Console → IAM → Users → **Create user** (e.g. `marius-terraform`).
2. Attach **`AdministratorAccess`** for now — the planned infra touches EC2/VPC, KMS, MSK,
   and IAM, enough surface that hand-scoping a policy upfront isn't worth it yet. Tighten
   later once the actual resources are stable.
3. On that user → **Security credentials** tab → **Create access key** → use case
   **"Command Line Interface (CLI)"** → copy both the **Access Key ID** and **Secret Access
   Key** immediately — the secret is shown only once, no way to retrieve it again later.
4. Enable **MFA** on that IAM user in the console.

## 4. Configure the CLI locally

```bash
aws configure
```
Access Key ID / Secret Access Key from step 3, region `eu-central-1`, output format `json`.

Verify it worked:
```bash
aws sts get-caller-identity
```

## Done

Once this succeeds, Terraform's AWS provider will pick up the same credentials
automatically (via the default profile / `AWS_PROFILE` env var) and `terraform plan`/
`apply` will be able to authenticate — for the actual cluster `.tf` files (MSK, KMS, VPC,
IAM policy for the producer), see the Kafka infrastructure plan (not yet written to this
repo — ask to resume it when ready).