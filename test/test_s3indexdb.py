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

import os
import sys
sys.path += [os.path.abspath('../../django')]
import ND.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ND.settings'

from nddynamo.s3indexdb import S3IndexDB as S3DB

project_name = 'kasthuri11'
channel_name = 'image'
channel_name2 = 'image2'
resolution = 0

class Test_S3IndexDB():

  def setup_class(self):
    """Setup parameters"""
    S3DB.createTable(endpoint_url='http://localhost:8000')
    self.s3db = S3DB(project_name, channel_name, endpoint_url='http://localhost:8000')
    self.s3db2 = S3DB(project_name, channel_name2, endpoint_url='http://localhost:8000')
    
  def teardown_class(self):
    """Teardown parameters"""
    S3DB.deleteTable(endpoint_url='http://localhost:8000')
    
  def test_putItem(self):
    """Test data insertion"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.s3db.putItem(resolution, x_value, y_value, z_value)
  
    # checking if the items were inserted
    for z_value in range(0, 2, 1):
      item_value = self.s3db.getItem(resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name, resolution, 0) )
    
    # inserting two values for task 1, zvalues 0-1
    for z_value in range(0, 1, 1):
      self.s3db.putItem(resolution, x_value, y_value, z_value, task_id=1)
    
    # checking if the items were updated
    for z_value in range(0, 1, 1):
      item_value = self.s3db.getItem(resolution, x_value, y_value, z_value)
      assert( item_value['project_name'] == project_name )
      assert( item_value['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name, resolution, 1) )
    
  
  def test_queryProjectItems(self):
    """Test the query over SI"""
    
    # inserting three values for task 0, zvalues 0-2
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      self.s3db.putItem(resolution, x_value, y_value, z_value)
    
    for item in self.s3db.queryProjectItems():
      assert( item['project_name'] == project_name )
    
    for item in self.s3db2.queryChannelItems():
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name2, resolution, 0) )
      
    for item in self.s3db.queryTaskItems(resolution, 1):
      # import pdb; pdb.set_trace()
      assert( item['channel_resolution_taskid'] == '{}&{}&{}'.format(channel_name2, resolution, 0) )


  def test_deleteXYZ(self):
    """Test item deletion"""
    
    x_value = 0
    y_value = 0
    for z_value in range(0, 2, 1):
      value = self.s3db.deleteXYZ(resolution, x_value, y_value, z_value)
      item = self.s3db.getItem(resolution, x_value, y_value, z_value)
      assert(item == None)
