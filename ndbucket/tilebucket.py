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

from __future__ import print_function
from __future__ import absolute_import
from settings.settings import Settings
settings = Settings.load()
import boto3
import botocore
import hashlib
import json
from util.util import Util
UtilClass = Util.load()

class TileBucket:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the upload queue"""
    
    self.region_name = region_name
    self.endpoint_url = endpoint_url
    bucket_name = TileBucket.getBucketName()
    self.project_name = project_name
    self.s3 = boto3.resource(
        's3', region_name=region_name, endpoint_url=endpoint_url, 
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      self.bucket = self.s3.Bucket(bucket_name)
    except botocore.exceptions.ClientError as e:
      print (e)
      raise

  @staticmethod
  def createBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the upload bucket"""
    
    bucket_name = TileBucket.getBucketName()
    s3 = boto3.resource(
        's3', region_name=region_name, endpoint_url=endpoint_url,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket(bucket_name)
    try:
      # creating the bucket
      response = bucket.create(
          ACL = 'private'
      )
    except Exception as e:
      print (e)
      raise

  @staticmethod
  def deleteBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the upload bucket"""
    
    bucket_name = TileBucket.getBucketName()
    s3 = boto3.resource(
        's3', region_name=region_name, endpoint_url=endpoint_url,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket(bucket_name)
    try:
      # deleting the bucket
      response = bucket.delete()
    except Exception as e:
      print (e)
      raise
  
  @staticmethod
  def getBucketName():
    """Generate the bucket name"""
    return settings.S3_TILE_BUCKET

  def encodeObjectKey(self, channel_name, resolution, x_index, y_index, z_index, t_index=0):
    """Generate the key for the file in scratch space"""
    hashm = hashlib.md5()
    hashm.update('{}&{}&{}&{}&{}&{}&{}'.format(self.project_name, channel_name, resolution, x_index, y_index, z_index, t_index).encode('utf-8'))
    return '{}&{}&{}&{}&{}&{}&{}&{}'.format(hashm.hexdigest(), self.project_name, channel_name, resolution, x_index, y_index, z_index, t_index)

  @staticmethod
  def decodeObjectKey(object_key):
    """Decode an object key"""
    return object_key.split('&')

  def createPolicy(self, policy, name, folder=None, description=''):
    """Create an IAM policy for the bucket.

    The policy parameter defines actions allowed on the bucket.  This policy 
    must be assigned to an AWS user to give access to the bucket.  Simple 
    policies can be represented with a single IAM statement.  The bucket's ARN
    will be added to each statment (as the Resource the statement applies to).

    If folder is provided, each IAM statement is targeted at that folder.  
    For example, if folder='/foo/bar' and the bucket is named my_tile_bucket,
    this is added to each statement dictionary:
        'Resource':'arn:aws:s3:::my_tile_bucket/foo/bar/*'


    Sample IAM statement dictionary: 
        { 'Sid': 'Receive Access Statement',
          'Effect': 'Allow',
          'Action': ['s3:PutObject'] }

    Args:
        policy (list(dict)): List of IAM statements. 
        name (string): Name for this policy.
        folder (optional[string]): Optionally target each statement to a specific folder.
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

    arn = TileBucket.buildArn(self.bucket.name, folder)

    # Specify this bucket as the resource for each policy statement.
    for statement in policy:
        statement['Resource'] = arn

    full_policy['Id'] = name

    doc = json.dumps(full_policy)
    return iam.create_policy(
        PolicyName=full_policy['Id'],
        PolicyDocument=json.dumps(full_policy),
        Path=settings.IAM_POLICY_PATH,
        Description=description)


  @staticmethod
  def buildArn(bucket_name, folder=None):
    """Build an S3 ARN for use in an IAM policy.

    Example: arn:aws:s3:::my_bucket/some/folder/*

    Args:
      bucket_name (string): Name of bucket.
      folder (optional[string]): Optional folder to apply IAM policy to.

    Returns:
      (string)
    """
    arn = 'arn:aws:s3:::' + bucket_name
    if folder is not None:
        if folder[0] != '/':
            arn += '/'
        arn += folder
        if not folder.endswith('/'):
            arn += '/'
        arn += '*'
    else:
        arn += '/*'

    return arn


  def deletePolicy(self, name):
    """Deletes the queue's policy.

    Args:
        name (string): Policy name.
    """

    arn = self.getPolicyArn(name)
    if arn is None:
        raise RuntimeError('Policy {} could not be found.'.format(name))

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


  def putObject(self, tile_handle, channel_name, resolution, x_tile, y_tile, z_tile, message_id, receipt_handle, time=0):
    """Put object in the upload bucket"""
    
    # generate the key
    tile_key = self.encodeObjectKey(channel_name, resolution, x_tile, y_tile, z_tile, time)

    try:
      response = self.bucket.put_object(
          ACL = 'private',
          Body = tile_handle,
          Key = tile_key,
          Metadata = {
            'message_id' : message_id,
            'receipt_handle' : receipt_handle
          },
          StorageClass = 'STANDARD'
      )
      return response
    except Exception as e:
      print (e)
      raise

  def getObjectByKey(self, tile_key):
    """Get object by tile key"""
    try:
      s3_obj = self.s3.Object(self.bucket.name, tile_key)
      response = s3_obj.get()
      return response['Body'].read(), response['Metadata']['message_id'], response['Metadata']['receipt_handle']
    except Exception as e:
      if e.response['Error']['Code'] == 'NoSuchKey':
        return None
      else:
        print (e)
        raise

  def getObject(self, channel_name, resolution, x_tile, y_tile, z_tile, time_index=0):
    """Get object from the upload bucket"""
    tile_key = self.encodeObjectKey(channel_name, resolution, x_tile, y_tile, z_tile, time_index)
    return self.getObjectByKey(tile_key)

  def getMetadata(self, object_key):
    """Get the object key from the upload bucket"""

    message_body, message_id, receipt_handle = self.getObjectByKey(object_key)
    return message_id, receipt_handle

  def deleteObject(self, object_key):
    """Delete object from the upload bucket"""
    
    try:
      s3_obj = self.s3.Object(self.bucket.name, object_key)
      response = s3_obj.delete()
      return response
      # response = self.bucket.delete_objects(
          # Delete = {
            # 'Objects' : [
              # {
                # 'Key' : self.generateKey(file_name)
              # },
            # ],
            # 'Quiet' : False
          # }
      # )
    except Exception as e:
      print (e)
      raise

  def getAllObjects(self):
    """Get a collection of ObjectSummary for all objects in the bucket."""

    try:
      return self.bucket.objects.all()
    except Exception as e:
      print (e)
      raise
