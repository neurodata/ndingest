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

# TODO KL Load the queue name here
upload_bucket = 'upload_bucket'

class UploadBucket:

  def __init__(self, region_name='us-west-2', endpoint_url='http://localhost:4567'):
    """Create resource for the upload queue"""

    self.s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    # s3 = boto3.resource('s3')
    try:
      self.bucket = self.s3.Bucket(upload_bucket)
    except botocore.exceptions.ClientError as e:
      print e
      raise


  @staticmethod
  def createBucket(region_name='us-west-2', endpoint_url='http://localhost:4567'):
    """Create the upload bucket"""
    
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    # s3 = boto3.resource('s3')
    bucket = s3.Bucket(upload_bucket)
    try:
      # creating the bucket
      response = bucket.create(
          ACL = 'private'
      )
    except Exception as e:
      print e
      raise


  @staticmethod
  def deleteBucket(region_name='us-west-2', endpoint_url='http://localhost:4567'):
    """Delete the upload bucket"""
    
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    # s3 = boto3.resource('s3')
    bucket = s3.Bucket(upload_bucket)
    try:
      # deleting the bucket
      response = bucket.delete()
    except Exception as e:
      print e
      raise
  
  def generateKey(self, file_name):
    """Generate the key for the file in scratch space"""
    return file_name


  def putobject(self, message):
    """Put object in the upload bucket"""
    
    # opening the file
    try:
      tile_handle = open(message.body)
    # if the file does not exist then send an empty file
    except IOError as e:
      import cStringIO
      tile_handle = cStringIO.StringIO()
   
    key = self.generateKey(message.body)
    try:
      upload_obj = self.bucket.put_object(
          ACL = 'private',
          Body = tile_handle,
          Key = key,
          Metadata = {
            'receipt_handle' : message.receipt_handle
          },
          StorageClass = 'STANDARD'
      )
    except Exception as e:
      print e
      raise
    finally:
      tile_handle.close()


  def getobject(self, object_key):
    """Get object from the upload bucket"""

    try:
      s3_obj = self.s3.Object(self.bucket.name, object_key)
      response = s3_obj.get()
      return response['Body'].read(), response['Metadata']['receipt_handle'], response['Metadata']['task_id']
    except Exception as e:
      print e
      raise e


  def deleteobject(self, object_key):
    """Delete object from the upload bucket"""
    
    try:
      s3_obj = self.s3.Object(self.bucket.name, object_key)
      s3_obj.delete()
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
      print e
      raise
