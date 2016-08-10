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

import boto3
import botocore

# TODO KL Load the queue name here
upload_queue = 'upload_queue'

class UploadQueue:

  def __init__(self, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create resource for the upload queue"""

    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.queue = sqs.get_queue_by_name(
          QueueName = upload_queue
      )
    except botocore.exceptions.ClientError as e:
      # UploadQueue.createQueue()
      print e
      raise
  
  @staticmethod
  def createQueue(region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create the upload queue"""
    
    try:
      # creating the queue, if the queue already exists catch exception
      sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
      queue = sqs.create_queue(
        QueueName = upload_queue,
        Attributes = {
          'DelaySeconds' : '0',
          'MaximumMessageSize' : '262144'
        }
      )
    except Exception as e:
      print e
      raise
  
  @staticmethod  
  def deleteQueue(region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Delete the upload queue"""

    # creating the resource
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      # try fetching queue first
      queue = sqs.get_queue_by_name(
          QueueName = upload_queue
      )
      # deleting the queue
      response = queue.delete()
    except Exception as e:
      print e
      raise

  def sendMessage(self, file_name):
    """Send message to upload queue"""
    
    try:
      response = self.queue.send_message(
          MessageBody = file_name,
          DelaySeconds = 0
      )
    except Exception as e:
      print e
      raise
  
  
  def receiveMessage(self):
    """Receive a message from the upload queue and return it"""
    
    try:
      message_list = self.queue.receive_messages(
          MaxNumberOfMessages=1
      )
      return message_list[0]
    except Exception as e:
      print e
      raise
    

  def deleteMessage(self, message):
    """Delete message from upload queue"""
    
    try:
      response = self.queue.delete_messages(
          Entries = [
            {
              'Id' : message.message_id,
              'ReceiptHandle' : message.receipt_handle
              },
          ]
      )
      # TODO KL Better handling for 400 aka when delete fails
      if 'Failed' in response:
        print response['Failed']['Message']
        raise
      else:
        return response
    except Exception as e:
      print e
      raise
