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
from settings.settings import Settings
settings = Settings.load()
from abc import abstractmethod
import boto3
import botocore
import json

class NDQueue(object):

  def __init__(self, queue_name, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the queue"""
    
    self.region_name = region_name
    self.endpoint_url = endpoint_url
    self.queue_name = queue_name

    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      self.queue = sqs.get_queue_by_name(
          QueueName = queue_name
      )
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
  
  @staticmethod
  @abstractmethod
  def generateNeurodataQueueName():
    return NotImplemented

  @staticmethod
  @abstractmethod
  def generateBossQueueName():
    return NotImplemented
  
  @classmethod
  def getNameGenerator(cls):
    if settings.PROJECT_NAME == 'Neurodata':
      return cls.generateNeurodataQueueName
    elif settings.PROJECT_NAME == 'Boss':
      return cls.generateBossQueueName
    else:
      print ("Unknown Project Name {}".format(settings.PROJECT_NAME))
  
  def createPolicy(self, policy, name=None, description=''):
    """Create a policy for this queue.

    The policy parameter defines actions allowed on the queue.  This policy 
    must be assigned to an AWS user to give access to the queue.  Simple 
    policies can be represented with a single IAM statement.

    Sample IAM statement dictionary: 
        { 'Sid': 'Receive Access Statement',
          'Effect': 'Allow',
          'Action': ['sqs:ReceiveMessage'] }

    Args:
        policy (list(dict)): List of IAM statements. 
        name (optional[string]): Custom name for this policy.  By default, the queue's name is used.
        description (optional[string]): Description of policy.

    Returns:
        (iam.Policy)
    """
    iam = boto3.resource(
        'iam',
        region_name=self.region_name, endpoint_url=self.endpoint_url, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    full_policy = { 'Version': '2012-10-17', 'Statement': policy }

    # Specify this queue as the resource for each policy statement.
    for statement in policy:
        statement['Resource'] = self.queue.attributes['QueueArn']

    if name is None:
        full_policy['Id'] = self.queue_name
    else:
        full_policy['Id'] = name

    doc = json.dumps(full_policy)
    return iam.create_policy(
        PolicyName=full_policy['Id'],
        PolicyDocument=json.dumps(full_policy),
        Path=settings.IAM_POLICY_PATH,
        Description=description)


  def deletePolicy(self, name=None):
    """Deletes the queue's policy.

    Args:
        name (optional[string]): Defaults to name of queue.
    """

    if name is not None:
        policy_name = name
    else:
        policy_name = self.queue_name

    arn = self.getPolicyArn(policy_name)
    if arn is None:
        raise RuntimeError('Policy {} could not be found.'.format(policy_name))

    iam = boto3.resource(
        'iam',
        region_name=self.region_name, endpoint_url=self.endpoint_url, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    policy = iam.Policy(arn)
    policy.delete()


  def getPolicyArn(self, name):
    """Find the named policy and return its Arn.

    Only user created policies with a path prefix as defined in settings.ini are retrieved.  
    Global AWS policies are ignored.

    Args:
        name (string): Name of policy.

    Returns:
        (string|None): None if policy cannot be found.
    """
    iam = boto3.resource(
        'iam',
        region_name=self.region_name, endpoint_url=self.endpoint_url, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    for policy in iam.policies.all():
        if policy.policy_name == name:
            return policy.arn

    return None


  def sendMessage(self, message_body, delay_seconds=0):
    """Send message to the queue"""
    try:
      response = self.queue.send_message(
          MessageBody = message_body,
          DelaySeconds = delay_seconds
      )
      return response
    except Exception as e:
      print (e)
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
      print (e)
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
        print (response['Failed']['Message'])
        raise
      else:
        return response
    except Exception as e:
      print (e)
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
