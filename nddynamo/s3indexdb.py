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

import blosc
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
from django.conf import settings
import ndlib
from s3util import generateS3Key

class S3IndexDB:

  def __init__(self, project_name, channel_name, region_name=settings.REGION_NAME, endpoint_url=None):

    db = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    self.table = db.Table(settings.DYNAMO_S3INDEX_TABLE)
    self.project = project_name
    self.channel = channel_name
 

  @staticmethod
  def createTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the s3index database in dynamodb"""
    
    db = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      table = db.create_table(
          TableName = settings.DYNAMO_S3INDEX_TABLE,
          KeySchema = [
            {
              'AttributeName': 'supercuboid_key',
              'KeyType': 'HASH'
            },
            {
              'AttributeName': 'version_number',
              'KeyType': 'RANGE'
            }
          ],
          AttributeDefinitions = [
            {
              'AttributeName': 'supercuboid_key',
              'AttributeType': 'S'
            },
            {
              'AttributeName': 'version_number',
              'AttributeType': 'N'
            },
            {
              'AttributeName': 'project_name',
              'AttributeType': 'S'
            },
            {
              'AttributeName': 'channel_resolution_taskid',
              'AttributeType': 'S'
            }
          ],
          GlobalSecondaryIndexes = [
            {
              'IndexName': 'info_index',
              'KeySchema': [
                {
                  'AttributeName': 'project_name',
                  'KeyType': 'HASH'
                },
                {
                  'AttributeName': 'channel_resolution_taskid',
                  'KeyType': 'RANGE'
                }
              ],
              'Projection': {
                'ProjectionType': 'ALL'
              },
              'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
              }
            },
          ],
          ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
          }
      )
    except Exception as e:
      print e
      raise e


  @staticmethod
  def deleteTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the ingest database in dynamodb"""

    db = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      table = db.Table(settings.DYNAMO_S3INDEX_TABLE)
      table.delete()
    except Exception as e:
      print e
      raise e


  def generatePrimaryKey(self, resolution, x, y, z):
    """Generate key for each supercuboid"""
    zidx = ndlib.XYZMorton([x, y, z])
    return generateS3Key(self.project, self.channel, resolution, zidx)
    # return '{}&{}&{}&{}&{}&{}'.format(self.project, self.channel, resolution, x, y, z)

  
  # def generateInfoKey(self, resolution, task_id):
    # """Generate task key for the project channel"""
    # return '{}&{}&{}&{}'.format(self.project, self.channel, resolution, task_id)


  def putItem(self, resolution, x, y, z, task_id=0):
    """Inserting an index for each supercuboid_array"""
    
    supercuboid_key = self.generatePrimaryKey(resolution, x, y, z)
    version_number = 0

    try:
      response = self.table.put_item(
          Item = {
            'supercuboid_key' : supercuboid_key,
            'version_number' : version_number,
            'project_name' : self.project,
            'channel_resolution_taskid' : '{}&{}&{}'.format(self.channel, resolution, task_id)
          },
          ReturnValues = 'NONE',
          ReturnConsumedCapacity = 'INDEXES'
      )
    except botocore.exceptions.ClientError as e:
      print e
      raise e
 

  def getItem(self, resolution, x, y, z):
    """Get an item based on supercuboid_key"""

    supercuboid_key = self.generatePrimaryKey(resolution, x, y, z)
    version_number = 0
    try:
      response =  self.table.get_item(
          Key = {
            'supercuboid_key' : supercuboid_key,
            'version_number' : version_number
          },
          ConsistentRead = True,
          ReturnConsumedCapacity = 'INDEXES'
      )
      return response['Item'] if 'Item' in response else None
      # response = self.table.query(
          # KeyConditionExpression = Key('supercuboid_key').eq(supercuboid_key)
      # )
    except Exception as e:
      print e
      raise e


  def queryProjectItems(self):
    """Query items based on project name"""
    
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project)
        # ExpressionAttributeValues = {
          # ':info_value' : info
        # }
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print e
      raise e


  def queryChannelItems(self):
    """Query items based on channel name"""
    
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(self.channel)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print e
      raise e


  def queryResolutionItems(self, resolution):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}'.format(self.channel, resolution)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print e
      raise e


  def queryTaskItems(self, resolution, task_id):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}&{}'.format(self.channel, resolution, task_id)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print e
      raise e


  def deleteXYZ(self, resolution, x, y, z):
    """Delete item from database"""
    
    supercuboid_key = self.generatePrimaryKey(resolution, x, y, z)
    return self.deleteItem(supercuboid_key)
  

  def deleteItem(self, supercuboid_key):
    """Delete item from database"""
    
    version_number = 0
    try:
      response = self.table.delete_item(
          Key = {
            'supercuboid_key' : supercuboid_key,
            'version_number' : version_number
          }
      )
      return response
    except botocore.exceptions.ClientError as e:
      print e
      raise e
