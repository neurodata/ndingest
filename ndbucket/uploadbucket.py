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

    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
    try:
      self.bucket = s3.Bucket(upload_bucket)
    except botocore.exceptions.ClientError as e:
      print e
      raise


  @staticmethod
  def createBucket(region_name='us-west-2', endpoint_url='http://localhost:4567'):
    """Create the upload bucket"""
    
    s3 = boto3.resource('s3', region_name=region_name, endpoint_url=endpoint_url)
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
    
    try:
      upload_obj = self.bucket.put_object(
          ACL = 'private',
          Body = open(message.body),
          Key = self.generateKey(message.body),
          Metadata = {
            'receipt_handle' : message.receipt_handle
          },
          StorageClass = 'STANDARD'
      )
    except Exception as e:
      print e
      raise


  def deleteobject(self, file_name):
    """Delete object from the upload bucket"""
    
    try:
      response = self.bucket.delete_objects(
          Delete = {
            'Objects' : [
              {
                'Key' : self.generateKey(file_name)
              },
            ],
            'Quiet' : False
          }
      )
    except Exception as e:
      print e
      raise
