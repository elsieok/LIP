#!/usr/bin/env bash
set -euo pipefail
# LOCAL MACHINE — requires `aws configure` already done

INSTANCE_TYPE="${1:-t4g.medium}"   # 4 GB RAM; see sizing note below
KEY_NAME="llm-platform-key"
SG_NAME="llm-platform-sg"
KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"
DISK_GB=30

mkdir -p deploy

# 1. SSH key pair (reuse if both the AWS key and the local .pem exist)
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
  [ -f "$KEY_FILE" ] || { echo "Key $KEY_NAME exists in AWS but $KEY_FILE is missing. Delete the AWS key pair and rerun."; exit 1; }
else
  aws ec2 create-key-pair --key-name "$KEY_NAME" --query 'KeyMaterial' --output text > "$KEY_FILE"
  chmod 400 "$KEY_FILE"
fi

# 2. Security group: SSH (your IPv4 only) + HTTP (open, it's a demo)
# 2. Security group: SSH + HTTP, both restricted to your IPv4 only
MY_IP=$(curl -4 -s https://checkip.amazonaws.com | tr -d '\n')
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$SG_NAME" Name=vpc-id,Values="$VPC_ID" \
  --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG_ID" = "None" ]; then
  SG_ID=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "llm platform demo" \
    --vpc-id "$VPC_ID" --query 'GroupId' --output text)
fi
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 --cidr "${MY_IP}/32" 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 80 --cidr "${MY_IP}/32" 2>/dev/null || true

# 3. Latest Ubuntu 22.04 arm64 AMI for your region
AMI_ID=$(aws ec2 describe-images --owners 099720109477 \
  --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*" "Name=state,Values=available" \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' --output text)

# 4. Launch
INSTANCE_ID=$(aws ec2 run-instances --image-id "$AMI_ID" --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" --security-group-ids "$SG_ID" \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${DISK_GB},VolumeType=gp3}" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=llm-platform-demo}]' \
  --query 'Instances[0].InstanceId' --output text)

echo "Waiting for instance $INSTANCE_ID to be running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"
PUBLIC_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Instance up at ${PUBLIC_IP}"
echo "$INSTANCE_ID" > deploy/.instance_id
echo "$PUBLIC_IP" > deploy/.instance_ip