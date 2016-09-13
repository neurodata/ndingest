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
import sys
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
from nddynamo.cuboidindexdb import CuboidIndexDB
from ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
nd_proj = ProjClass('kasthuri11', 'image', '0')
nd_proj2 = ProjClass('kasthuri11', 'image2', '0')


class Test_CuboidIndexDB():

  def setup_class(self):
    """Setup parameters"""
    try:
      CuboidIndexDB.createTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    except Exception as e:
      pass
    self.cuboid_index = CuboidIndexDB(nd_proj.project_name, endpoint_url=settings.DYNAMO_ENDPOINT)
    

  def teardown_class(self):
    """Teardown parameters"""
    CuboidIndexDB.deleteTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    

  def test_putItem(self):
    """Test data insertion"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.cuboid_index.putItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
  
    # checking if the items were inserted
    for z_value in range(0, 2, 1):
      item_value = self.cuboid_index.getItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == nd_proj.project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(nd_proj.channel_name, nd_proj.resolution, 0) )
    
    # inserting two values for task 1, zvalues 0-1
    for z_value in range(0, 1, 1):
      self.cuboid_index.putItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value, task_id=1)
    
    # checking if the items were updated
    for z_value in range(0, 1, 1):
      item_value = self.cuboid_index.getItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == nd_proj.project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(nd_proj.channel_name, nd_proj.resolution, 1) )
    
  
  def test_queryProjectItems(self):
    """Test the query over SI"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.cuboid_index.putItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
    
    for item in self.cuboid_index.queryProjectItems():
      assert( item['project_name'] == nd_proj.project_name )
    
    for item in self.cuboid_index.queryChannelItems(nd_proj2.channel_name):
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(nd_proj2.channel_name, nd_proj.resolution, 0) )
      
    for item in self.cuboid_index.queryTaskItems(nd_proj.channel_name, nd_proj.resolution, 1):
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(nd_proj2.channel_name, nd_proj.resolution, 0) )


  def test_deleteXYZ(self):
    """Test item deletion"""
    
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      value = self.cuboid_index.deleteXYZ(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
      item = self.cuboid_index.getItem(nd_proj.channel_name, nd_proj.resolution, x_value, y_value, z_value)
      assert(item == None)
