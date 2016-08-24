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

# TODO KL Import this from settings/parameter file
table_name = 's3index_db'

class S3IndexDB:

  def __init__(self, project_name='kasthuri11', channel_name='image', resolution=0):

    db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    self.table = db.Table(table_name)
    self.project = project_name
    self.channel = channel_name
    self.resolution = resolution
 

  @staticmethod
  def createTable():
    """Create the s3index database in dynamodb"""
    
    db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    try:
      table = db.create_table(
          TableName = table_name,
          KeySchema = [
            {
              'AttributeName': 'supercuboid_key',
              'KeyType': 'HASH'
            },
            {
              'AttributeName': 'version_number',
              'KeyType': 'SORT'
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
                  'KeyType': 'SORT'
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
  def deleteTable():
    """Delete the ingest database in dynamodb"""

    db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    try:
      table = db.Table(table_name)
      table.delete()
    except Exception as e:
      print e
      raise e


  def generateKey(self, x, y, z):
    """Generate key for each supercuboid"""
    # return {'supercuboid_key': '{}&{}&{}&{}&{}&{}'.format(self.project, self.channel, self.resolution, x, y, z)}
    return '{}&{}&{}&{}&{}&{}'.format(self.project, self.channel, self.resolution, x, y, z)

  
  def generateInfoKey(self, task_id):
    """Generate task key for the project channel"""
    return '{}&{}&{}&{}'.format(self.project, self.channel, self.resolution, task_id)


  def putItem(self, x, y, z, task_id=0):
    """Inserting an index for each supercuboid_array"""
    
    supercuboid_key = self.generateKey(x, y, z)
    # info_key = self.generateInfoKey(task_id)
    version_number = 0

    try:
      response = self.table.put_item(
          Item = {
            'supercuboid_key' : supercuboid_key,
            'version_number' : version_number,
            'project_name' : self.project,
            'channel_resolution_taskid' : '{}&{}&{}'.format(self.channel, self.resolution, task_id)
          },
          ReturnValues = 'NONE',
          ReturnConsumedCapacity = 'INDEXES'
      )
    except botocore.exceptions.ClientError as e:
      print e
      raise e
 

  def getItem(self, x, y, z):
    """Get an item based on supercuboid_key"""

    supercuboid_key = self.generateKey(x, y, z)
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
      return response, 0
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
      return response, 0
    except Exception as e:
      print e
      raise e

  def queryResolutionItems(self):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}'.format(self.channel, self.resolution)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      return response, 0
    except Exception as e:
      print e
      raise e
  
  def queryTaskItems(self, task_id):
    """Query items based on channel name"""
    
    filter_expression = '{}&{}&{}'.format(self.channel, self.resolution, task_id)
    try:
      response = self.table.query(
        IndexName = 'info_index',
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression = Key('project_name').eq(self.project) & Key('channel_resolution_taskid').begins_with(filter_expression)
      )
      return response, 0
    except Exception as e:
      print e
      raise e

  def deleteItem(self, supercuboid_key):
    """Delete item from database"""
    
    try:
      self.table.delete_item(
          Key = {
            'supercuboid_key' : supercuboid_key
          }
      )
    except botocore.exceptions.ClientError as e:
      print e
      raise e
