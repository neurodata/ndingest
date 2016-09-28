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
import sys
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
import json
from ndqueue.ndqueue import NDQueue
from ndqueue.uploadqueue import UploadQueue
from ndqueue.serializer import Serializer
serializer = Serializer.load()
from ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
from random import randint
import unittest
import boto3
import botocore
import time
import warnings

class TestDeadletterQueue(unittest.TestCase):
    def setUp(self):
        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        if 'SQS_ENDPOINT' in dir(settings):
          self.endpoint_url = settings.SQS_ENDPOINT
        else:
          self.endpoint_url = None

        self.upload_queue = None
        self.nd_proj = self.generate_proj()

    def tearDown(self):
        if self.upload_queue is not None:
            self.upload_queue.queue.delete()

    def generate_proj(self):
        """Add some randomness to the project.
       
        Queue names must be different between tests because a deleted queue
        cannot be recreated with the same name until 60s has elapsed.
        """

        num = randint(100, 999)

        if settings.PROJECT_NAME == 'Boss':
            job_id = num
            nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, job_id)
        else:
            channel = 'image{}'.format(num)
            nd_proj = ProjClass('kasthuri11', channel, '0')

        return nd_proj


    def test_create_queue_with_default_name(self):
        # Create upload queue.
        UploadQueue.createQueue(self.nd_proj, endpoint_url=self.endpoint_url)
        self.upload_queue = UploadQueue(self.nd_proj, endpoint_url=self.endpoint_url)

        # Create dead letter queue with default name.
        exp_max_receives = 4
        dl_queue = self.upload_queue.addDeadLetterQueue(exp_max_receives)

        exp_name = self.upload_queue.queue_name + '_dead_letter'
        exp_arn = dl_queue.attributes['QueueArn']

        try:
            policy = json.loads(
                self.upload_queue.queue.attributes['RedrivePolicy'])
            self.assertEqual(exp_max_receives, policy['maxReceiveCount'])
            self.assertEqual(exp_arn, policy['deadLetterTargetArn'])
            # Confirm dead letter queue named correctly by looking at the end 
            # of its ARN.
            self.assertTrue(dl_queue.attributes['QueueArn'].endswith(exp_name))
        finally:
            dl_queue.delete()


    def test_add_existing_queue_as_dead_letter_queue(self):
        # Create existing queue for dead letter queue.
        sqs = boto3.resource(
            'sqs',
            region_name=settings.REGION_NAME, endpoint_url=self.endpoint_url, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        queue_name = 'deadletter_test_{}'.format(randint(100, 999))
        existing_queue = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'DelaySeconds': '0',
                'MaximumMessageSize': '262144'
            }
        )

        exp_arn = existing_queue.attributes['QueueArn']

        try:
            # Create upload queue.
            UploadQueue.createQueue(self.nd_proj, endpoint_url=self.endpoint_url)
            self.upload_queue = UploadQueue(self.nd_proj, endpoint_url=self.endpoint_url)

            # Attach the dead letter queue to it.
            exp_max_receives = 5
            dl_queue = self.upload_queue.addDeadLetterQueue(
                exp_max_receives, exp_arn)

            # Confirm policy settings.
            policy = json.loads(
                self.upload_queue.queue.attributes['RedrivePolicy'])
            self.assertEqual(exp_max_receives, policy['maxReceiveCount'])
            self.assertEqual(exp_arn, policy['deadLetterTargetArn'])

            # Confirm dead letter queue is the one created at the beginning 
            # of test.
            self.assertEqual(existing_queue.url, dl_queue.url)
        finally:
            existing_queue.delete()


    def test_delete_dead_letter_queue(self):
        # Create existing queue for dead letter queue.
        sqs = boto3.resource(
            'sqs',
            region_name=settings.REGION_NAME, endpoint_url=self.endpoint_url, 
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        queue_name = 'deadletter_test_{}'.format(randint(100, 999))
        existing_queue = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'DelaySeconds': '0',
                'MaximumMessageSize': '262144'
            }
        )

        # Create upload queue. 
        arn = existing_queue.attributes['QueueArn']
        UploadQueue.createQueue(self.nd_proj, endpoint_url=self.endpoint_url)
        self.upload_queue = UploadQueue(self.nd_proj, endpoint_url=self.endpoint_url)

        # Attach the dead letter queue to it.
        dl_queue = self.upload_queue.addDeadLetterQueue(2, arn)

        # Invoke the delete method.
        NDQueue.deleteDeadLetterQueue(sqs, self.upload_queue.queue)

        # Confirm deletion.
        with self.assertRaises(botocore.exceptions.ClientError):
            sqs.get_queue_by_name(QueueName=queue_name)


if __name__ == '__main__':
    unittest.main()
