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
# from __future__ import absolute_import
from settings.settings import Settings
settings = Settings.load()
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
import blosc
from ndctypelib import XYZMorton
from util.util import Util
UtilClass = Util.load()

class CuboidIndexDB:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):

    # create the resource
    table_name = CuboidIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    self.table = dynamo.Table(table_name)
    self.project = project_name
 

  @staticmethod
  def createTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the s3index database in dynamodb"""
    
    # create the resource
    table_name = CuboidIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      table = dynamo.create_table(
          TableName = table_name,
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
      print (e)
      raise


  @staticmethod
  def deleteTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the ingest database in dynamodb"""
    
    # create the resource
    table_name = CuboidIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    try:
      table = dynamo.Table(table_name)
      table.delete()
    except Exception as e:
      print (e)
      raise
  
  @staticmethod
  def getTableName():
    """Return table name"""
    return settings.DYNAMO_CUBOIDINDEX_TABLE

  def generatePrimaryKey(self, channel_name, resolution, x, y, z, time_index=0):
    """Generate key for each supercuboid"""
    morton_index = XYZMorton([x, y, z])
    return UtilClass.generateCuboidKey(self.project, channel_name, resolution, morton_index, time_index)


  def putItem(self, channel_name, resolution, x, y, z, time=0, task_id=0):
    """Inserting an index for each supercuboid_array"""
    
    supercuboid_key = self.generatePrimaryKey(channel_name, resolution, x, y, z, time)
    version_number = 0

    try:
      response = self.table.put_item(
          Item = {
            'supercuboid_key' : supercuboid_key,
            'version_number' : version_number,
            'project_name' : self.project,
            'channel_resolution_taskid' : '{}&{}&{}'.format(channel_name, resolution, task_id)
          },
          ReturnValues = 'NONE',
          ReturnConsumedCapacity = 'INDEXES'
      )
    except botocore.exceptions.ClientError as e:
      print (e)
      raise 
 

  def getItem(self, channel_name, resolution, x, y, z):
    """Get an item based on supercuboid_key"""

    supercuboid_key = self.generatePrimaryKey(channel_name, resolution, x, y, z)
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
      print (e)
      raise


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
      print (e)
      raise


  def queryChannelItems(self, channel_name):
    """Query items based on channel name"""
    
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(channel_name)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print (e)
      raise


  def queryResolutionItems(self, channel_name, resolution):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}'.format(channel_name, resolution)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print (e)
      raise


  def queryTaskItems(self, channel_name, resolution, task_id):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}&{}'.format(channel_name, resolution, task_id)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      for item in response['Items']:
        yield item
    except Exception as e:
      print (e)
      raise


  def deleteXYZ(self, channel_name, resolution, x, y, z):
    """Delete item from database"""
    
    supercuboid_key = self.generatePrimaryKey(channel_name, resolution, x, y, z)
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
      print (e)
      raise
