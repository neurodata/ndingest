#/bin/bash

# install fake_s3
sudo gem install fakes3

# install fake_sqs
sudo gem install fake_sqs

# install dynamo
cd /data/scratch/
wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz .
