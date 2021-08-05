#!/bin/bash

set -e

# can be anything you like
cluster_name="sample-emr-cluster"

# EC2 key pair
key_pair_file_name="sample-ec2-emr-key-pair"

# default subnet ID
subnet_id="sample-subnet-id"

# S3 location to store your EMR logs in
log_uri="s3://sample-emr-cluster-log/"

# result from `create_emr_cluster.sh` under `ClusterId` field
cluster_id="sample-cluster-id"

pem_file_path=""

master_public_dns=""

get_cluster_name() {
  echo "${cluster_name}"
}

get_key_pair_file_name() {
  echo "${key_pair_file_name}"
}

get_subnet_id() {
  echo "${subnet_id}"
}

get_log_uri() {
  echo "${log_uri}"
}

get_cluster_id() {
  echo "${cluster_id}"
}

get_pem_file_path() {
  echo "${pem_file_path}"
}

get_master_public_dns() {
  echo "${master_public_dns}"
}
