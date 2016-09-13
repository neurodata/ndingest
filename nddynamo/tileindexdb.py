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
settings = Settings.load()
import botocore
import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import div
from ndlib.ndlib import XYZMorton
from ndlib.s3util import generateS3Key


class TileIndexDB:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):

    # creating the resource
    table_name = TileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    self.table = dynamo.Table(table_name)
    self.project_name = project_name
 
  @staticmethod
  def createTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the ingest database in dynamodb"""
    
    # creating the resource
    table_name = TileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      table = dynamo.create_table(
          TableName = table_name,
          KeySchema = [
            {
              'AttributeName': 'supercuboid_key',
              'KeyType': 'HASH'
            }
          ],
          AttributeDefinitions = [
            {
              'AttributeName': 'supercuboid_key',
              'AttributeType': 'S'
            },
            {
              'AttributeName': 'task_id',
              'AttributeType': 'N'
            }
          ],
          GlobalSecondaryIndexes = [
            {
              'IndexName': 'task_index',
              'KeySchema': [
                {
                  'AttributeName': 'task_id',
                  'KeyType': 'HASH'
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

    # creating the resource
    table_name = TileIndexDB.getTableName()
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
    return settings.DYNAMO_TILEINDEX_TABLE


  def generatePrimaryKey(self, channel_name, resolution, x_index, y_index, z_index, t_index=0):
    """Generate key for each supercuboid"""
    # TODO KL divide by SC size
    morton_index = XYZMorton(map(div, [x_index, y_index, z_index], settings.SUPER_CUBOID_SIZE))
    return generateS3Key(self.project_name, channel_name, resolution, morton_index, t_index)

  def supercuboidReady(self, z_index, zindex_list):
    """Verify if we have all tiles for a given supercuboid"""
    return zindex_list == set(range(z_index / settings.SUPER_CUBOID_SIZE[2], settings.SUPER_CUBOID_SIZE[2], 1))

  def putItem(self, channel_name, resolution, x_index, y_index, z_index, t_index=0, task_id=0):
    """Updating item for a give slice number"""
    
    # x, y, z = [int(i) for i in file_name.split('.')[0].split('_')]
    supercuboid_key = self.generatePrimaryKey(channel_name, resolution, x_index, y_index, z_index, t_index)
    
    try:
      response = self.table.update_item(
          Key = {
            'supercuboid_key': supercuboid_key
          },
          UpdateExpression = 'ADD zindex_list :z_index SET task_id = :task_id',
          ExpressionAttributeValues = {
              ':z_index': set([z_index]),
              ':task_id': task_id
          },
          ReturnValues = 'ALL_NEW'
      )
      return supercuboid_key, self.supercuboidReady(z_index, response['Attributes']['zindex_list'])
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
  
  
  def getItem(self, supercuboid_key):
    """Get the item from the ingest table"""
    
    try:
      response = self.table.get_item(
          Key = {
            'supercuboid_key' : supercuboid_key
          },
          ConsistentRead = True,
          ReturnConsumedCapacity = 'INDEXES'
      )
      # response = self.table.query(
          # KeyConditionExpression = Key('cuboid_key').eq(key)
      # )
      # TODO write a yield function to pop one item at a time
      return response['Item'] if 'Item' in response else None
    except Exception as e:
      print (e)
      raise

  def getTaskItems(self, task_id):
    """Get all the items for a given task from the ingest table"""
    
    try:
      response = self.table.query(
          IndexName = 'task_index',
          KeyConditionExpression = 'task_id = :task_id',
          ExpressionAttributeValues = {
            ':task_id' : task_id
          }
      )
      for item in response['Items']:
        yield item
      # return response['Items'], response['Count']
    except Exception as e:
      print (e)
      raise


  def deleteItem(self, supercuboid_key):
    """Delete item from database"""
   
    try:
      response = self.table.delete_item(
          Key = {
            'supercuboid_key' : supercuboid_key
          }
      )
      return response
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
