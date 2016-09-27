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
import botocore
import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import floordiv
from util.util import Util
UtilClass = Util.load()
import time
try:
    # Temp try-catch while developing on Windows.
    from spdb.c_lib.ndlib import XYZMorton
except Exception:
    pass


class BossTileIndexDB:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):

    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    self.table = dynamo.Table(table_name)
    self.project_name = project_name
 
  @staticmethod
  def createTable(schema, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the tile index table in dynamodb.  
    
    The table's name will be taken from settings.ini ([aws]tile_index_table).
    This method blocks until the table is created in DynamoDB.

    Args:
        schema (dict): Table's schema encoded in a dictionary.  If TableName is set, it will be overwritten by the name in settings.ini.
        region_name (optional[string]): AWS region queue lives in.  Extracted from settings.ini if not provided.
        endpoint_url (optional[string]): Provide if using a mock or fake Boto3 service.
    """
    
    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    schema['TableName'] = table_name
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      table = dynamo.create_table(**schema)
    except Exception as e:
      print (e)
      raise

    BossTileIndexDB.wait_table_create(table_name, region_name, endpoint_url)


  @staticmethod
  def wait_table_create(table_name, region_name=settings.REGION_NAME, endpoint_url=None):
      """Poll dynamodb at a 2s interval until the table creates."""
      print('Waiting for creation of table {}'.format(
            table_name), end='', flush=True)
      client = boto3.client('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
      cnt = 0
      while True:
          time.sleep(2)
          cnt += 1
          if cnt > 50:
              # Give up waiting.
              return
          try:
              print('.', end='', flush=True)
              resp = client.describe_table(TableName=table_name)
              if resp['Table']['TableStatus'] == 'ACTIVE':
                  print('')
                  return
          except:
              # May get an exception if table doesn't currently exist.
              pass


  @staticmethod
  def deleteTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the ingest database in dynamodb"""

    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
    
    try:
      table = dynamo.Table(table_name)
      table.delete()
    except Exception as e:
      print (e)
      raise


  @staticmethod
  def getTableName():
      return settings.DYNAMO_TILEINDEX_TABLE


  def generatePrimaryKey(self, channel_name, resolution, x_index, y_index, z_index, t_index=0):
    """Generate key for each supercuboid"""
    morton_index = XYZMorton(map(floordiv, [x_index, y_index, z_index], settings.SUPER_CUBOID_SIZE))
    return UtilClass.generateCuboidKey(self.project_name, channel_name, resolution, morton_index, t_index)


  def createCuboidEntry(self, channel_name, resolution, x_index, y_index, z_index, t_index=0, task_id=0):
    """Create the initial entry for tracking tiles uploaded for a cuboid.

    Call this before using markTileAsUploaded().

    Args:
        channel_name (string): Name of the channel the cuboid belongs to.
        resolution (int): 0 for native resolution.
        x_index (int): Starting x value of the cuboid.
        y_index (int): Starting y value of the cuboid.
        z_index (int): Starting z value of the cuboid.
        t_index (optional[int]): Starting time index.  Defaults to 0.
        task_id (optional[int]): Task or job id that this cuboid belongs to.  Defaults to 0.

    Returns:
        (string): Chunk key that holds the cuboid's entry in the table.
    """
    chunk_key = self.generatePrimaryKey(channel_name, resolution, x_index, y_index, z_index, t_index)

    try:
        response = self.table.put_item(
            Item = {
                'chunk_key': chunk_key,
                'tile_uploaded_map': {},
                'task_id': task_id
            })
        return chunk_key
    except botocore.exceptions.ClientError as e:
        print (e)
        raise

  def markTileAsUploaded(self, tile_key, channel_name, resolution, x_index, y_index, z_index, t_index=0, task_id=0):
    """Mark the tile as uploaded.

    Marks the tile belonging to the cuboid specified by the channel name, resolution, and coordinates as uploaded.

    Args:
        tile_key (string): Key to retrieve tile from S3 bucket.
        channel_name (string): Name of the channel the cuboid belongs to.
        resolution (int): 0 for native resolution.
        x_index (int): Starting x value of the cuboid.
        y_index (int): Starting y value of the cuboid.
        z_index (int): Starting z value of the cuboid.
        t_index (optional[int]): Starting time index.  Defaults to 0.
        task_id (optional[int]): Task or job id that this cuboid belongs to.  Defaults to 0.

    Returns:
        (string, dict): Chunk key (cuboid's key) and the map of uploaded tiles.
    """
    
    chunk_key = self.generatePrimaryKey(channel_name, resolution, x_index, y_index, z_index, t_index)
    
    try:
      response = self.table.update_item(
          Key = {
            'chunk_key': chunk_key
          },
          UpdateExpression = 'ADD tile_uploaded_map.{} :uploaded SET task_id = :task_id'.format(tile_key),
          ExpressionAttributeValues = {
              ':uploaded': 1,
              ':task_id': task_id
          },
          ReturnValues = 'ALL_NEW'
      )
      return chunk_key, self.cuboidReady(response['Attributes']['tile_uploaded_map'])
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
  

  def cuboidReady(self, tile_uploaded_map):
    """Verify if we have all tiles for a given cuboid"""
    return len(tile_uploaded_map) == settings.SUPER_CUBOID_SIZE[2]
  

  def getItem(self, chunk_key):
    """Get the item from the tile index table"""
    
    try:
      response = self.table.get_item(
          Key = {
            'chunk_key' : chunk_key
          },
          ConsistentRead = True,
          ReturnConsumedCapacity = 'INDEXES'
      )
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
    except Exception as e:
      print (e)
      raise


  def deleteItem(self, chunk_key):
    """Delete item from database"""
   
    try:
      response = self.table.delete_item(
          Key = {
            'chunk_key' : chunk_key
          }
      )
      return response
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
