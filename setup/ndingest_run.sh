#!/bin/bash

# running fake_s3 in the background
s3cmd="fakes3 -r /data/scratch/ -p 4567"
nohup $s3cmd > /var/log/neurodata/fakes3.log 2>&1&
echo $! > /var/log/neurodata/fakes3_id

# running fake_sqs in the background
sqscmd="fake_sqs"
nohup $sqscmd > /var/log/neurodata/fakesqs.log 2>&1&
echo $! > /var/log/neurodata/fakesqs_id

# running local dynamodb in the background
dynamocmd="java -Djava.library.path=/data/scratch/DynamoDBLocal_lib -jar /data/scratch/DynamoDBLocal.jar -sharedDb"
nohup $dynamocmd > /var/log/neurodata/dynamo.log 2>&1&
echo $! > /var/log/neurodata/dynamo_id
