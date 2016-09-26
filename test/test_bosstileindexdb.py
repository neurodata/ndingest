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
import sys
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
from nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingestproj.bossingestproj import BossIngestProj
job_id = '123'
nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, job_id, 'test.boss.io')
import unittest
from unittest.mock import patch


schema = {"KeySchema": [
        {
          "AttributeName": "chunk_key",
          "KeyType": "HASH"
        }
      ],
      "AttributeDefinitions" : [
        {
          "AttributeName": "chunk_key",
          "AttributeType": "S"
        },
        {
          "AttributeName": "task_id",
          "AttributeType": "N"
        }
      ],
      "GlobalSecondaryIndexes": [
        {
          "IndexName": "task_index",
          "KeySchema": [
            {
              "AttributeName": "task_id",
              "KeyType": "HASH"
            }
          ],
          "Projection": {
            "ProjectionType": "ALL"
          },
          "ProvisionedThroughput": {
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
          }
        },
      ],
      "ProvisionedThroughput": {
        "ReadCapacityUnits": 10,
        "WriteCapacityUnits": 10
      }
}

class Test_BossTileIndexDB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup parameters"""
        BossTileIndexDB.createTable(schema)
        
        cls.tileindex_db = BossTileIndexDB(nd_proj.project_name)
        
    @classmethod
    def tearDownClass(cls):
        """Teardown parameters"""
        BossTileIndexDB.deleteTable()

    def test_cuboidReady_false(self):
        fake_map = { 'o': 1 }
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            assert(False == self.tileindex_db.cuboidReady(fake_map))

    def test_cuboidReady_true(self):
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1,
            's9': 1, 's10': 1, 's11': 1, 's12': 1, 's13': 1, 's14': 1, 's15': 1, 's16': 1
        }
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            assert(self.tileindex_db.cuboidReady(fake_map))

   
  #def test_putItem(self):
  #  """Test data insertion"""
    
  #  x_tile = 0
  #  y_tile = 0
  #  # inserting three values for task 0
  #  for z_tile in range(0, 3, 1):
  #    self.tileindex_db.putItem(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile, task_id=0)
    
  #  # inserting 2 values for task 1
  #  for z_tile in range(66, 68, 1):
  #    self.tileindex_db.putItem(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile, task_id=1)

  #  # checking if the items were inserted
  #  z_tile = 0
  #  supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #  item_value = self.tileindex_db.getItem(supercuboid_key)
  #  assert( item_value['zindex_list'] == set([0, 1, 2]) )
    
  #  z_tile = 65
  #  supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #  item_value = self.tileindex_db.getItem(supercuboid_key)
  #  assert( item_value['zindex_list'] == set([66, 67]) )
  
  #def test_queryTaskItems(self):
  #  """Test the query over SI"""
    
  #  for item in self.tileindex_db.getTaskItems(0):
  #    assert( item['zindex_list'] == set([0, 1, 2]) )

  #def test_supercuboidReady(self):
  #  """Test if the supercuboid is ready"""
    
  #  x_tile = 0
  #  y_tile = 0
  #  for z_tile in range(129, 129+settings.SUPER_CUBOID_SIZE[2], 1):
  #    supercuboid_key, supercuboid_ready = self.tileindex_db.putItem(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile, task_id=0)
  #    if z_tile < 129+settings.SUPER_CUBOID_SIZE[2]:
  #      assert(supercuboid_ready is False)
  #    else:
  #      assert(supercuboid_ready is True)


  #def test_deleteItem(self):
  #  """Test item deletion"""
    
  #  x_tile = 0
  #  y_tile = 0
  #  # inserting three values for task 0
  #  for z_tile in range(0, 3, 1):
  #    supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #    self.tileindex_db.deleteItem(supercuboid_key)
    
  #  # inserting 2 values for task 1
  #  for z_tile in range(66, 68, 1):
  #    supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #    self.tileindex_db.deleteItem(supercuboid_key)
    
  #  # inserting three values for task 0
  #  for z_tile in range(0, 3, 1):
  #    supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #    item = self.tileindex_db.getItem(supercuboid_key)
  #    assert(item == None)
    
  #  # inserting 2 values for task 1
  #  for z_tile in range(66, 68, 1):
  #    supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
  #    item = self.tileindex_db.getItem(supercuboid_key)
  #    assert(item == None)
