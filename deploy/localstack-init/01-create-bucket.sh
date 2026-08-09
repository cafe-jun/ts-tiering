#!/bin/bash
# LocalStack 이 준비되면 실행된다 (/etc/localstack/init/ready.d).
# 로컬에 aws CLI 가 없어도 되도록, 버킷 생성은 컨테이너 안의 awslocal 로 처리한다.
set -euo pipefail

BUCKET="${COLD_BUCKET:-ts-tiering-cold}"
REGION="${AWS_DEFAULT_REGION:-ap-northeast-2}"

# us-east-1 이 아니면 LocationConstraint 가 필요하다.
awslocal s3api create-bucket \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --create-bucket-configuration "LocationConstraint=$REGION"

echo "created bucket s3://$BUCKET ($REGION)"
