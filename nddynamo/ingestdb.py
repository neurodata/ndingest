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

# TODO KL Import this from settings/parameter file
table_name = 'Test'

class IngestDB:

  def __init__(self, project_name='kasthuri11', channel_name='image', resolution=0):

    self.db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    self.table = self.db.Table(table_name)
    self.project = project_name
    self.channel = channel_name
    self.resolution = resolution
 
  @staticmethod
  def createDB():
    """Create the ingest database in dynamodb"""
    
    db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    table = db.create_table(
        TableName = table_name,
        KeySchema = [
          {
            'AttributeName': 'cuboid_key',
            'KeyType': 'HASH'
          }
        ],
        AttributeDefinitions = [
          {
            'AttributeName': 'cuboid_key',
            'AttributeType': 'S'
          },
          {
            'AttributeName': 'task_id',
            'AttributeType': 'N'
          }
        ],
        GlobalSecondaryIndexes = [
          {
            'IndexName': 'task_id',
            'KeySchema': [
              {
                'AttributeName': 'task_id',
                'KeyType': 'HASH'
              },
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

    print "Table Status: ", table.table.status


  @staticmethod
  def deleteDB():
    """Delete the ingest database in dynamodb"""

    db = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
    table = self.db.Table(table_name)
    table.delete()


  def generateKey(self, x, y, z):
    """Generate key for each supercuboid"""
    return {'cuboid_key': '{}&{}&{}&{}&{}&{}'.format(self.project, self.channel, self.resolution, x, y, z)}


  def updateItem(self, x=0, y=0, z=0, slice_number, task_id):
    """Updating item for a give slice number"""
    
    try:
      self.table.update_item(
          Key = self.generateKey(x, y, z),
          UpdateExpression = 'ADD slice_list :slice_number',
          ExpressionAttributeValues = 
            {
              ':slice_number': slice_number,
              ':task_id': task_id
            }
      )
    except botocore.exceptions.ClientError as e:
        raise e
  

  def deleteItem(self, key):
    """Delete item from database"""
    
    try:
      self.table.delete_item(
          Key = self.generateKey(0, 0, 0)
      )
    except botocore.exceptions.ClientError as e:
      raise e
