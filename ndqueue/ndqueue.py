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
import boto3
import botocore

class NDQueue(object):

  def __init__(self, queue_name, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the queue"""
    
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      self.queue = sqs.get_queue_by_name(
          QueueName = queue_name
      )
    except botocore.exceptions.ClientError as e:
<<<<<<< HEAD
      print(e)
=======
      print (e)
>>>>>>> f11bec995ed76f9952a2a128b9c8f4f5870a02eb
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
<<<<<<< HEAD
      print(e)
=======
      print (e)
>>>>>>> f11bec995ed76f9952a2a128b9c8f4f5870a02eb
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
<<<<<<< HEAD
      print(e)
=======
      print (e)
>>>>>>> f11bec995ed76f9952a2a128b9c8f4f5870a02eb
      raise
    
  
  def deleteMessage(self, message_id, receipt_handle, number_of_messages=1):
    """Delete message from queue"""
    

    # KL TODO Fix this for deleting multiple messages
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
<<<<<<< HEAD
        print(response['Failed']['Message'])
=======
        print (response['Failed']['Message'])
>>>>>>> f11bec995ed76f9952a2a128b9c8f4f5870a02eb
        raise
      else:
        return response
    except Exception as e:
<<<<<<< HEAD
      print(e)
=======
      print (e)
>>>>>>> f11bec995ed76f9952a2a128b9c8f4f5870a02eb
      raise


  def _populateEntries(self, message_id_list, receipt_handle_list):
    """Populate a list of Entries"""

    entries = []
    for message_id, receipt_handle in zip(message_id_list, receipt_handle_list):
      entries.append( 
          { 'Id' : message_id,
            'ReceiptHandle' : receipt_handle
          }
      )
    return entries
