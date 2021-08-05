#!/bin/bash

set -e

source ./common.sh

pem_file_path=$(get_pem_file_path)
master_public_dns=$(get_master_public_dns)

scp -i "${pem_file_path}" \
  ../etl.py \
  ../dl.cfg \
  hadoop@"${master_public_dns}":/home/hadoop
