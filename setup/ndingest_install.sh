#/bin/bash

# install fake_s3
sudo gem install fakes3
sudo apt-get install default-jre -y

# install fake_sqs
sudo gem install fake_sqs

# install dynamo
cd /data/scratch/
wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
tar -xvzf dynamodb_local_latest.tar.gz
