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

import hashlib
import boto3
import botocore
from django.conf import settings


class CuboidBucket:

  def __init__(self, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the upload queue"""
    
    bucket_name = CuboidBucket.getBucketName()
    self.s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.bucket = self.s3.Bucket(bucket_name)
    except botocore.exceptions.ClientError as e:
      print e
      raise


  @staticmethod
  def createBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the upload bucket"""
    
    bucket_name = CuboidBucket.getBucketName()
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    try:
      # creating the bucket
      response = bucket.create(
          ACL = 'private'
      )
    except Exception as e:
      print e
      raise


  @staticmethod
  def deleteBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the upload bucket"""
    
    bucket_name = CuboidBucket.getBucketName()
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    try:
      # deleting the bucket
      response = bucket.delete()
    except Exception as e:
      print e
      raise
  
  @staticmethod
  def getBucketName():
    """Generate the Bucket Name"""
    return settings.S3_CUBOID_BUCKET
  
  # def putObject(self, tile_handle, project_name, channel_name, res, x_tile, y_tile, z_tile, message_id, receipt_handle):
    # """Put object in the upload bucket"""
    
    # # generate the key
    # object_key = self.generateObjectKey(project_name, channel_name, res, x_tile, y_tile, z_tile)
    
    # try:
      # response = self.bucket.put_object(
          # ACL = 'private',
          # Body = tile_handle,
          # Key = object_key,
          # Metadata = {
            # 'message_id' : message_id,
            # 'receipt_handle' : receipt_handle
          # },
          # StorageClass = 'STANDARD'
      # )
      # return response
    # except Exception as e:
      # print e
      # raise
  

  # def getObject(self, object_key):
    # """Get object from the upload bucket"""

    # try:
      # s3_obj = self.s3.Object(self.bucket.name, object_key)
      # response = s3_obj.get()
      # return response['Body'].read(), response['Metadata']['receipt_handle'], response['Metadata']['message_id']
    # except Exception as e:
      # print e
      # raise e
 

  def deleteObject(self, object_key):
    """Delete object from the upload bucket"""
    
    try:
      s3_obj = self.s3.Object(self.bucket.name, object_key)
      response = s3_obj.delete()
      return response
    except Exception as e:
      print e
      raise
