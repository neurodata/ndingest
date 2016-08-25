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

import json
import boto3
import botocore

from ndqueue import NDQueue

class UploadQueue(NDQueue):

  
  def __init__(self, proj_info, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create resources for the queue"""
    
    queue_name = UploadQueue.generateQueueName(proj_info)
    NDQueue.__init__(self, queue_name)


  @staticmethod
  def createQueue(proj_info, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create the upload queue"""
    
    queue_name = UploadQueue.generateQueueName(proj_info)
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      # creating the queue, if the queue already exists catch exception
      queue = sqs.create_queue(
        QueueName = queue_name,
        Attributes = {
          'DelaySeconds' : '0',
          'MaximumMessageSize' : '262144'
        }
      )
      return queue_name
    except Exception as e:
      print e
      raise


  @staticmethod  
  def deleteQueue(proj_info, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Delete the upload queue"""

    queue_name = UploadQueue.generateQueueName(proj_info)
    
    # creating the resource
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      # try fetching queue first
      queue = sqs.get_queue_by_name(
          QueueName = queue_name
      )
      # deleting the queue
      response = queue.delete()
    except Exception as e:
      print e
      raise

  @staticmethod
  def generateQueueName(proj_info):
    """Generate the queue name based on project information"""
    return '&'.join(proj_info+['UPLOAD'])

  def sendMessage(self, tile_info):
    """Send a message to upload queue"""
    NDQueue.sendMessage(self, json.dumps(tile_info))

  def receiveMessage(self, number_of_messages=1):
    """Receive a message from the upload queue"""
    message_list = NDQueue.receiveMessage(self, number_of_messages=number_of_messages)
    for message in message_list:
      yield message.message_id, message.receipt_handle, json.loads(message.body)

  def deleteMessage(self, message_id, receipt_handle):
    """Delete a message from the upload queue"""
    return NDQueue.deleteMessage(self, message_id, receipt_handle)
