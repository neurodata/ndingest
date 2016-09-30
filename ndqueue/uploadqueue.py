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
from ..settings.settings import Settings
settings = Settings.load()
import json
import boto3
import botocore
from ..ndqueue.ndqueue import NDQueue
import random

class UploadQueue(NDQueue):

  # Static variable to hold random number added to test queue names.
  test_queue_id = -1

  def __init__(self, nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resources for the queue"""
    
    self.queue_name = UploadQueue.generateQueueName(nd_proj)
    return super(UploadQueue, self).__init__(self.queue_name, region_name=region_name, endpoint_url=endpoint_url)

  @staticmethod 
  def generateNeurodataQueueName(nd_proj):
    return '-'.join(nd_proj.generateProjectInfo()+['UPLOAD']).replace('&', '-')
    
  @staticmethod 
  def generateBossQueueName(nd_proj):
    if not settings.TEST_MODE:
        return '{}-upload-{}'.format(settings.DOMAIN, nd_proj.job_id)

    if UploadQueue.test_queue_id == -1:
        UploadQueue.test_queue_id = random.randint(0, 999)

    return 'test{}-{}-upload-{}'.format(UploadQueue.test_queue_id, settings.DOMAIN, nd_proj.job_id)

  @staticmethod
  def createQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the upload queue"""
    
    # creating the resource
    queue_name = UploadQueue.generateQueueName(nd_proj)
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      # creating the queue, if the queue already exists catch exception
      response = sqs.create_queue(
        QueueName = queue_name,
        Attributes = {
          'DelaySeconds' : '0',
          'MaximumMessageSize' : '262144'
        }
      )
      return queue_name
    except Exception as e:
      print (e)
      raise


  @staticmethod  
  def deleteQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None, delete_deadletter_queue=False):
    """Delete the upload queue.
    
    Also delete the dead letter queue if delete_deadletter_queue is true.

    Args:
        nd_proj (IngestProj): Project settings used to generate queue's name.
        region_name (optional[string]): AWS region queue lives in.  Extracted from settings.ini if not provided.
        endpoint_url (optional[string]): Provide if using a mock or fake Boto3 service.
        delete_deadletter_queue (optional[bool]): Also delete the dead letter queue.  Defaults to False.
    """

    # creating the resource
    queue_name = UploadQueue.generateQueueName(nd_proj)
    NDQueue.deleteQueueByName(queue_name, region_name, endpoint_url, delete_deadletter_queue)


  @staticmethod
  def generateQueueName(nd_proj):
    """Generate the queue name based on project information"""
    # TODO come up with new naming scheme.  Limited to 80 chars.
    return UploadQueue.getNameGenerator()(nd_proj)

  def sendMessage(self, tile_info):
    """Send a message to upload queue"""
    return super(UploadQueue, self).sendMessage(tile_info)

  def receiveMessage(self, number_of_messages=1):
    """Receive a message from the upload queue"""
    message_list = super(UploadQueue, self).receiveMessage(number_of_messages=number_of_messages)
    if message_list is None:
      raise StopIteration
    else:
      for message in message_list:
        yield message.message_id, message.receipt_handle, json.loads(message.body)

  def deleteMessage(self, message_id, receipt_handle, number_of_messages=1):
    """Delete a message from the upload queue"""
    return super(UploadQueue, self).deleteMessage(message_id, receipt_handle, number_of_messages=number_of_messages)
