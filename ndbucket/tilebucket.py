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
import hashlib
from util.util import Util
UtilClass = Util.load('Neurodata')

class TileBucket:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create resource for the upload queue"""
    
    bucket_name = TileBucket.getBucketName()
    self.project_name = project_name
    self.s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.bucket = self.s3.Bucket(bucket_name)
    except botocore.exceptions.ClientError as e:
      print (e)
      raise

  @staticmethod
  def createBucket(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the upload bucket"""
    
    bucket_name = TileBucket.getBucketName()
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
    """Delete the upload bucket"""
    
    bucket_name = TileBucket.getBucketName()
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
    """Generate the bucket name"""
    return settings.S3_TILE_BUCKET

  def encodeObjectKey(self, channel_name, resolution, x_index, y_index, z_index, t_index=0):
    """Generate the key for the file in scratch space"""
    hashm = hashlib.md5()
    hashm.update('{}&{}&{}&{}&{}&{}&{}'.format(self.project_name, channel_name, resolution, x_index, y_index, z_index, t_index))
    return '{}&{}&{}&{}&{}&{}&{}&{}'.format(hashm.hexdigest(), self.project_name, channel_name, resolution, x_index, y_index, z_index, t_index)

  @staticmethod
  def decodeObjectKey(object_key):
    """Decode an object key"""
    return object_key.split('&')

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
