#!/usr/bin/env bash
# Floci init hook (runs inside the floci container when services are
# ready; the LocalStack-compatible /etc/localstack/init/ready.d path is
# honored): pre-create the bucket used by the test suites.
# Requires the -compat image variant (ships the AWS CLI).
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 mb s3://constellation-ci || true
