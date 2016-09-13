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
settings = Settings.load('Neurodata')
from nddynamo.cuboidindexdb import CuboidIndexDB

project_name = 'kasthuri11'
channel_name = 'image'
channel_name2 = 'image2'
resolution = 0

class Test_CuboidIndexDB():

  def setup_class(self):
    """Setup parameters"""
    CuboidIndexDB.createTable(endpoint_url='http://localhost:8000')
    self.cuboid_index = CuboidIndexDB(project_name, endpoint_url='http://localhost:8000')
    

  def teardown_class(self):
    """Teardown parameters"""
    CuboidIndexDB.deleteTable(endpoint_url='http://localhost:8000')
    

  def test_putItem(self):
    """Test data insertion"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.cuboid_index.putItem(channel_name, resolution, x_value, y_value, z_value)
  
    # checking if the items were inserted
    for z_value in range(0, 2, 1):
      item_value = self.cuboid_index.getItem(channel_name, resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name, resolution, 0) )
    
    # inserting two values for task 1, zvalues 0-1
    for z_value in range(0, 1, 1):
      self.cuboid_index.putItem(channel_name, resolution, x_value, y_value, z_value, task_id=1)
    
    # checking if the items were updated
    for z_value in range(0, 1, 1):
      item_value = self.cuboid_index.getItem(channel_name, resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name, resolution, 1) )
    
  
  def test_queryProjectItems(self):
    """Test the query over SI"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.cuboid_index.putItem(channel_name, resolution, x_value, y_value, z_value)
    
    for item in self.cuboid_index.queryProjectItems():
      assert( item['project_name'] == project_name )
    
    for item in self.cuboid_index.queryChannelItems(channel_name2):
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name2, resolution, 0) )
      
    for item in self.cuboid_index.queryTaskItems(channel_name, resolution, 1):
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name2, resolution, 0) )


  def test_deleteXYZ(self):
    """Test item deletion"""
    
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      value = self.cuboid_index.deleteXYZ(channel_name, resolution, x_value, y_value, z_value)
      item = self.cuboid_index.getItem(channel_name, resolution, x_value, y_value, z_value)
      assert(item == None)
