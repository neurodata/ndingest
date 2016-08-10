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

from abc import abstractmethod
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


  @staticmethod
  def createQueue(queue_name, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Create the queue"""
    
    # creating the resource
    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url)
    try:
      # creating the queue
      queue = sqs.create_queue(
        QueueName = queue_name,
        Attributes = {
          'DelaySeconds' : '0',
          'MaximumMessageSize' : '262144'
        }
      )
    except Exception as e:
      print e
      raise


  @staticmethod
  def deleteQueue(queue_name, region_name='us-west-2', endpoint_url='http://localhost:4568'):
    """Delete the queue"""
    
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

  
  @abstractmethod
  def sendMessage(self, value):
    """Send message to the queue"""
    return NotImplemented   
  
  
  @abstractmethod
  def receiveMessage(self):
    """Receive a message from the queue"""
    return NotImplemented   
    
  
  @abstractmethod
  def deleteMessage(self, message):
    """Delete message from queue"""
    return NotImplemented   
