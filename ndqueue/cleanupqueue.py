# Copyright 2014 NeuroData (http://neurodata.io)
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
from settings.settings import Settings
settings = Settings.load()
import json
import boto3
import botocore
from ndqueue.ndqueue import NDQueue

class CleanupQueue(NDQueue):

  
  def __init__(self, nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resources for the queue"""
    
    self.queue_name = CleanupQueue.generateQueueName(nd_proj)
    return super(CleanupQueue, self).__init__(self.queue_name, region_name, endpoint_url)


  @staticmethod
  def createQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the upload queue"""
    
    # creating the resource
    queue_name = CleanupQueue.generateQueueName(nd_proj)
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
  def deleteQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the upload queue"""

    # creating the resource
    queue_name = CleanupQueue.generateQueueName(nd_proj)
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      # try fetching queue first
      queue = sqs.get_queue_by_name(
          QueueName = queue_name
      )
      # deleting the queue
      response = queue.delete()
      return response
    except Exception as e:
      print (e)
      raise


  @staticmethod
  def generateQueueName(nd_proj):
    """Generate the queue name based on project information"""
    return '&'.join(nd_proj.generateProjectInfo()+['CLEANUP'])


  def sendMessage(self, tile_info):
    """Send a message to upload queue"""
    return super(CleanupQueue, self).sendMessage(json.dumps(tile_info))


  def receiveMessage(self, number_of_messages=1):
    """Receive a message from the upload queue"""
    message_list = super(CleanupQueue, self).receiveMessage(number_of_messages=number_of_messages)
    if message_list is None:
      raise StopIteration
    else:
      for message in message_list:
        yield message.message_id, message.receipt_handle, json.loads(message.body)


  def deleteMessage(self, message_id, receipt_handle, number_of_messages=1):
    """Delete a message from the upload queue"""
    return super(CleanupQueue, self).deleteMessage(message_id, receipt_handle, number_of_messages=number_of_messages)
