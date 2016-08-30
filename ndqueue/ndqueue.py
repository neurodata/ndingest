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


class NDQueue:

  def __init__(self, queue_name, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create resource for the queue"""
    
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.queue = sqs.get_queue_by_name(
          QueueName = queue_name
      )
    except botocore.exceptions.ClientError as e:
      print e
      raise

  
  def sendMessage(self, message_body, delay_seconds=0):
    """Send message to the queue"""
    try:
      response = self.queue.send_message(
          MessageBody = message_body,
          DelaySeconds = delay_seconds
      )
      return response
    except Exception as e:
      print e
      raise
  
  
  def receiveMessage(self, number_of_messages=1):
    """Receive a message from the queue"""
    
    try:
      message_list = self.queue.receive_messages(
          MaxNumberOfMessages = number_of_messages
      )   
      # checking for empty responses
      return None if not message_list else message_list
    except Exception as e:
      print e
      raise
    
  
  def deleteMessage(self, message_id, receipt_handle):
    """Delete message from queue"""
    
    try:
      response = self.queue.delete_messages(
          Entries = [
            {
              'Id' : message_id,
              'ReceiptHandle' : receipt_handle
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