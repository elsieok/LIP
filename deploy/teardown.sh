#!/usr/bin/env bash
set -euo pipefail
aws ec2 terminate-instances --instance-ids $(cat deploy/.instance_id)
aws ec2 wait instance-terminated --instance-ids $(cat deploy/.instance_id)
echo "Terminated. Check the console for any orphaned EBS volumes or Elastic IPs."