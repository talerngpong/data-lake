# ETL on Cloud Data Lake for Song Play Analysis

This project aims to load raw song and user data, process and save data as star schema for later analysis by Elastic Map-Reduce (EMR) service. This is also used to satisfied with `Data Lake` project under [Data Engineer Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Prerequisite
- Python3
- Python virtual environment (aka `venv`)
- AWS credentials/config files under `~/.aws` directories.

## Steps
1. Bootstrap virtual environment with dependencies
   ```bash
   $ python3 -m venv ./venv
   $ source ./venv/bin/activate
   $ pip install -r requirements.txt
   ```
2. Copy config template `template.dl.cfg` to `dl.cfg` and `aws_stuff/template.common.sh` to `aws_stuff/common.sh`.
   ```bash
   $ cp ./template.dl.cfg ./dl.cfg
   $ cp ./aws_stuff/template.common.sh ./aws_stuff/common.sh
   ```
3. Fill `dl.cfg` on `ETL_PROCESSED_DATA_SET` section. It refers to target S3 bucket to store processed data set. Here are possible values.
   ```cfg
   [ETL_PROCESSED_DATA_SET]
   BUCKET_NAME=sample-data-lake-bucket
   USER_DATA_PREFIX=data-lake/user
   ARTIST_DATA_PREFIX=data-lake/artist
   TIME_DATA_PREFIX=data-lake/time
   SONG_DATA_PREFIX=data-lake/song
   SONGPLAY_DATA_PREFIX=data-lake/songplay
   ```
4. Fill `aws_stuff/common.sh` on `cluster_name`, `key_pair_file_name`, `subnet_id`, `log_uri` and `pem_file_path`. Here are possible values.
   ```bash
   # can be anything as your choice
   cluster_name="tony-emr-cluster"
   
   # S3 location to store EMR logs in as your choice
   log_uri="s3://sample-emr-cluster-log/"
   
   # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html
   key_pair_file_name="sample-ec2-emr-key-pair"
   pem_file_path="${HOME}/.aws/sample-ec2-emr-key-pair.pem"
   
   # default subnet ID
   # https://docs.aws.amazon.com/vpc/latest/userguide/default-vpc.html#create-default-vpc
   subnet_id="sample-subnet-id"
   ```
5. Spin up EMR cluster.
   ```bash
   $ cd ./aws_stuff
   $ ./create_emr_cluster.sh
   ```
6. Look for cluster ID from previous result. Then, put it to `aws_stuff/common.sh`. Here is a possible value.
   ```bash
   cluster_id="sample-cluster-id"
   ```
7. Retrieve public domain name of master node from EMR AWS console. Then, put it to `aws_stuff/common.sh`. Here is a possible value.
   ```bash
   master_public_dns="sample-master-node.compute.amazonaws.com"
   ```
8. Upload `etl.py` and `dl.cfg` to master node.
   ```bash
   $ cd ./aws_stuff
   $ ./upload_etl_stuff.sh
   ```
9. SSH to master node and submit `etl.py` script via `spark-submit` command.
   ```bash
   $ spark-submit --master yarn etl.py
   ```
10. Terminate EMR cluster after used.
   ```bash
   $ cd ./aws_stuff
   $ ./terminate_emr_cluster.sh
   ```
