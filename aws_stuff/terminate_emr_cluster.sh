#!/bin/bash

set -e

source ./common.sh

emr_cluster_id=$(get_cluster_id)

docker run \
  --rm -it \
  -v ~/.aws:/root/.aws \
  amazon/aws-cli emr terminate-clusters \
  --cluster-ids "${emr_cluster_id}"
