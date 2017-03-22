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
from ndingest.settings.settings import Settings
settings = Settings.load()
from abc import abstractmethod
import boto3
import botocore
import json

class NDQueue(object):
  """Base class for SQS queues that support ingest.

  Attributes:
    queue (SQS.Queue): Interface to the queue.
    region_name (string): AWS region queue lives in.
    endpoint_url (string|None): Alternative URL boto3 should use for testing instead of connecting to AWS.
    queue_name (string): The friendly name of the queue.
  """

  def __init__(self, queue_name, region_name=settings.REGION_NAME, endpoint_url=settings.SQS_ENDPOINT):
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

  @property
  def url(self):
      """Gets the URL of the queue.

      Returns:
        (string)
      """
      return self.queue.url


  @property
  def arn(self):
      """Gets the ARN of the queue.

      Returns:
        (string)
      """
      return self.queue.attributes['QueueArn']

  
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


  @staticmethod
  def deleteQueueByName(name, region_name=settings.REGION_NAME, endpoint_url=settings.SQS_ENDPOINT, delete_deadletter_queue=False):
    """Delete the named queue.
    
    Also delete the dead letter queue if delete_deadletter_queue is true.

    Args:
        name (string): Name of queue to delete.
        region_name (optional[string]): AWS region queue lives in.  Extracted from settings.ini if not provided.
        endpoint_url (optional[string]): Provide if using a mock or fake Boto3 service.
        delete_deadletter_queue (optional[bool]): Also delete the dead letter queue.  Defaults to False.
    """

    sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      # try fetching queue first
      queue = sqs.get_queue_by_name(QueueName=name)

      if delete_deadletter_queue:
          NDQueue.deleteDeadLetterQueue(sqs, queue)

      # delete the queue
      queue.delete()
    except Exception as e:
      print (e)
      raise


  def addDeadLetterQueue(self, max_receive_count, queue_arn=None):
      """Sets the dead letter queue.

      A dead letter queue will be created if queue_arn is not supplied.  If
      creating, the dead letter queue will have the same name as the queue it
      supports, but with '_dead_letter' appended.

      Args:
        max_receive_count (int): Max times a message may be received before sending to the dead letter queue.
        queue_arn (optional[string]): ARN of existing queue to use for the dead letter queue.

      Return:
        (SQS.Queue): The dead letter queue.
      """

      sqs = boto3.resource(
          'sqs',
          region_name=self.region_name, endpoint_url=self.endpoint_url, 
          aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
          aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

      if queue_arn is not None:
          arn = queue_arn
          queue = NDQueue.findQueueByArn(sqs, arn)
          if queue is None:
              msg = 'Queue {} not found.'.format(arn)
              print(msg)
              raise RuntimeError(msg)
      else:
          # Create dead letter queue.
          name = self.queue_name + '_dead_letter'
          sqs = boto3.resource(
              'sqs',
              region_name=self.region_name, endpoint_url=self.endpoint_url, 
              aws_access_key_id=settings.AWS_ACCESS_KEY_ID, 
              aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
          try:
              queue = sqs.create_queue(
                  QueueName=name,
                  Attributes = {
                      'DelaySeconds': '0',
                      'MaximumMessageSize': '262144'
                  }
              )
              arn = queue.attributes['QueueArn']
          except Exception as e:
              print(e)
              raise

      policy = {'maxReceiveCount': max_receive_count, 'deadLetterTargetArn': arn}

      # Set dead letter policy on the queue.
      self.queue.set_attributes(Attributes={'RedrivePolicy': json.dumps(policy)})

      # Return the dead letter queue.
      return queue


  @staticmethod
  def deleteDeadLetterQueue(sqs, queue):
      """Delete the dead letter queue associated with the given queue.

      Args:
        sqs (SQS.ServiceResource): Live resource used for queue operations.
        queue (SQS.Queue): The queue that "owns" the dead letter queue.
      """

      if 'RedrivePolicy' not in queue.attributes:
          print('Delete failed - queue {} does not have a dead letter queue.'.format(queue.url))
          return

      policy = json.loads(queue.attributes['RedrivePolicy'])
      arn = policy['deadLetterTargetArn']

      dlet_queue = NDQueue.findQueueByArn(sqs, arn)
      if dlet_queue is None:
          print('Delete failed for {} - not found.'.format(arn))
          return

      dlet_queue.delete()


  @staticmethod
  def findQueueByArn(sqs, arn):
      """Find a queue by its ARN.

      Args:
        sqs (SQS.ServiceResource): Live resource used for queue operations.
        arn (string): ARN of queue.

      Returns:
        (SQS.Queue|None): None if queue not found.
      """

      (_, name) = arn.rsplit(':', 1)
      return sqs.get_queue_by_name(QueueName=name)


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
