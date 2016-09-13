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
import hashlib
import boto3
import botocore
from ndlib.s3util import generateS3Key


class CuboidBucket:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the cuboid queue"""
    
    bucket_name = CuboidBucket.getBucketName()
    self.project_name = project_name
    self.s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.bucket = self.s3.Bucket(bucket_name)
    except botocore.exceptions.ClientError as e:
      print (e)
      raise

  @staticmethod
  def createBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the cuboid bucket"""
    
    bucket_name = CuboidBucket.getBucketName()
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
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
    """Delete the cuboid bucket"""
    
    bucket_name = CuboidBucket.getBucketName()
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    try:
      # deleting the bucket
      response = bucket.delete()
    except Exception as e:
      print (e)
      raise
  
  @staticmethod
  def getBucketName():
    """Generate the Bucket Name"""
    return settings.S3_CUBOID_BUCKET
  

  def putObject(self, channel_name, resolution, morton_index, cube_data, time_index=0):
    """Put object in the cuboid bucket"""
    supercuboid_key = self.generateSupercuboidKey(channel_name, resolution, morton_index, time_index)
    return self.putObjectByKey(supercuboid_key, cube_data)
    
  def putObjectByKey(self, supercuboid_key, cube_data):
    """Put object in the cuboid bucket by key"""
    
    try:
      response = self.bucket.put_object(
          ACL = 'private',
          Body = cube_data,
          Key = supercuboid_key,
          StorageClass = 'STANDARD'
      )
      return response
    except Exception as e:
      print (e)
      raise
   
  def getObjectByKey(self, supercuboid_key):
    """Get an object from the cuboid bucket based on key"""

    try:
      s3_obj = self.s3.Object(self.bucket.name, supercuboid_key)
      response = s3_obj.get()
      return response['Body'].read()
    except Exception as e:
      print (e)
      raise

  def getObject(self, channel_name, resolution, morton_index, time_index=0):
    """Get object from the cuboid bucket based on parameters"""
    supercuboid_key = self.generateSupercuboidKey(channel_name, resolution, morton_index, time_index)
    return self.getObjectByKey(supercuboid_key)

  def generateSupercuboidKey(self, channel_name, resolution, morton_index, time_index=0):
    """Generate the supercuboid key"""
    return generateS3Key(self.project_name, channel_name, resolution, morton_index, time_index)

  def deleteObject(self, supercuboid_key):
    """Delete object from the upload bucket"""
    
    try:
      s3_obj = self.s3.Object(self.bucket.name, supercuboid_key)
      response = s3_obj.delete()
      return response
    except Exception as e:
      print (e)
      raise
