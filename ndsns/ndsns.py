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
  
from __future__ import print_function
from __future__ import absolute_import
from settings.settings import Settings
settings = Settings.load('Neurodata')
import boto3
import botocore


class NDSns():

  def __init__(self, topic_arn, region_name=settings.REGION_NAME, endpoint_url=None):
    """Initialize a topic"""
    # topic_name = NDSns.getTopicName(nd_proj)
    sns = boto3.resource('sns', region_name=region_name, endpoint_url=endpoint_url)
    # for test in sns.topics.all():
      # print sns
    try:
      self.topic = sns.Topic(topic_arn)
    except Exception as e:
      print (e)
      raise

  @staticmethod
  def createTopic(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create a topic"""
    topic_name = NDSns.getTopicName(nd_proj)
    sns = boto3.resource('sns', region_name=region_name, endpoint_url=endpoint_url)
    try:
      topic = sns.create_topic(
          Name = topic_name
      )
      return topic.arn
    except Exception as e:
      print (e)
      raise

  @staticmethod
  def deleteTopic(topic_arn, region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the topic"""
    # topic_name = NDSns.getTopicName(nd_proj)
    sns = boto3.resource('sns', region_name=region_name, endpoint_url=endpoint_url)
    try:
      topic = sns.Topic(topic_arn)
      topic.delete()
    except Exception as e:
      print (e)
      raise
    
  @staticmethod
  def getTopicName(nd_proj):
    """Generate the topic name based on project information"""
    # does not line &
    return '-'.join(nd_proj.generateProjectInfo())

  def publish(self, target_arn, message):
    """Publish a message"""
    try:
      response = self.topic.publish(
          TargetArn = target_arn,
          Message = message
      )
      return response
    except Exception as e:
      print (e)
      raise
  
  def subscribe(self, lambda_arn):
    """Subscribe to a topic"""
    try:
      subscription = self.topic.subscribe(
          Protocol = 'lambda',
          Endpoint = lambda_arn
      )
    except Exception as e:
      print (e)
      raise
