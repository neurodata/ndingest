# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function
import boto3
from ndingest.settings.settings import Settings
settings = Settings.load()
import hashlib
import json
from .util import Util
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.ndqueue.uploadqueue import UploadQueue
import random

INGEST_POLICY_NAME = '{}-client-policy-{}'

# Test policies introduce a random number to avoid collisions between tests.
TEST_INGEST_POLICY_NAME = '{}-test-{}-client-policy-{}'

class BossUtil(Util):
    # Static variable to hold random number added to test queue names.
    test_policy_id = -1

    @staticmethod
    def generateCuboidKey(project_name, channel_name, resolution, morton_index, time_index=0):
        """Generate the key for the supercube.  Not used by the Boss.
        """
        return NotImplemented

    @staticmethod
    def decode_chunk_key(key):
        """A method to decode the chunk key

        The tile key is the key used for each individual tile file.

        This should match chunk key encoding/decoding done by the ingest client.

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        result = {}
        parts = key.split('&')
        result["num_tiles"] = int(parts[1])
        result["collection"] = int(parts[2])
        result["experiment"] = int(parts[3])
        result["channel_layer"] = int(parts[4])
        result["resolution"] = int(parts[5])
        result["x_index"] = int(parts[6])
        result["y_index"] = int(parts[7])
        result["z_index"] = int(parts[8])
        result["t_index"] = int(parts[9])

        return result

    @staticmethod
    def generate_ingest_policy(
        job_id, upload_queue, tile_bucket, 
        region_name=settings.REGION_NAME, endpoint_url=None, description=''):
        """Generate the combined IAM policy.
       
        Policy allows receiving messages from the queue and writing to the tile bucket.

        Args:
            job_id (int): Id of ingest job.
            upload_queue (UploadQueue):
            tile_bucket (TileBucket):
            region_name (optional[string]): AWS region.
            endpoint_url (string|None): Alternative URL boto3 should use for testing instead of connecting to AWS.

        Returns:
            (iam.Policy)
        """
        iam = boto3.resource(
            'iam',
            region_name=region_name, endpoint_url=endpoint_url, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

        if not settings.TEST_MODE:
            policy_name = INGEST_POLICY_NAME.format(settings.DOMAIN, job_id)
        else:
            if BossUtil.test_policy_id == -1:
                BossUtil.test_policy_id = random.randint(0, 999)
            policy_name = TEST_INGEST_POLICY_NAME.format(
                settings.DOMAIN, BossUtil.test_policy_id, job_id)

        policy = {
            "Version": "2012-10-17",
            "Id": policy_name,
            "Statement": [
                {
                    "Sid": "ClientQueuePolicy",
                    "Effect": "Allow",
                    "Action": ["sqs:ReceiveMessage"],
                    "Resource": upload_queue.arn
                },
                {
                    "Sid": "ClientTileBucketPolicy",
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": TileBucket.buildArn(tile_bucket.bucket.name)
                }
                ]
            }

        return iam.create_policy(
            PolicyName=policy['Id'],
            PolicyDocument=json.dumps(policy),
            Path=settings.IAM_POLICY_PATH,
            Description=description)

    @staticmethod
    def delete_ingest_policy(
        job_id, region_name=settings.REGION_NAME, endpoint_url=None):
        """Delete the IAM policy associated with the given job id.

        Args:
            job_id (int): Id of ingest job.
            region_name (optional[string]): AWS region.
            endpoint_url (string|None): Alternative URL boto3 should use for testing instead of connecting to AWS.
            
        Returns:
            (bool): False if policy not found.
        """

        if not settings.TEST_MODE:
            name = INGEST_POLICY_NAME.format(settings.DOMAIN, job_id)
        else:
            name = TEST_INGEST_POLICY_NAME.format(
                settings.DOMAIN, BossUtil.test_policy_id, job_id)

        iam = boto3.resource(
            'iam',
            region_name=region_name, endpoint_url=endpoint_url, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

        for policy in iam.policies.filter(Scope='Local', PathPrefix=settings.IAM_POLICY_PATH):
            if policy.policy_name == name:
                policy.delete()
                return True

        return False
