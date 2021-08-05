#!/bin/bash

set -e

source ./common.sh

# can be anything you like
cluster_name=$(get_cluster_name)

# https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:
key_pair_file_name=$(get_key_pair_file_name)

# default subnet ID
subnet_id=$(get_subnet_id)

# S3 location to store your EMR logs in
log_uri=$(get_log_uri)

docker run \
  --rm -it \
  -v ~/.aws:/root/.aws \
  amazon/aws-cli \
  emr create-cluster \
  --name "${cluster_name}" \
  --use-default-roles \
  --release-label emr-6.3.0 \
  --instance-count 3 \
  --applications Name=Spark \
  --ec2-attributes KeyName="${key_pair_file_name}",SubnetId="${subnet_id}" \
  --instance-type m5.xlarge \
  --log-uri "${log_uri}"

