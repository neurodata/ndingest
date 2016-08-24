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

import sys
import os
sys.path += [os.path.abspath('..')]
from nddynamo.s3indexdb import S3IndexDB as S3DB


class Test_S3IndexDB():

  def setup_class(self):
    """Setup parameters"""
    S3DB.createTable()
    self.s3db = S3DB('kasthuri11', 'image', 0)
    self.s3db2 = S3DB('kasthuri11', 'image2', 0)
    
  def teardown_class(self):
    """Teardown parameters"""
    S3DB.deleteTable()
    
  def test_putItem(self):
    """Test data insertion"""
    
    # inserting three values for task 0, zvalues 0-2
    self.s3db.putItem(0, 0, 0)
    self.s3db.putItem(0, 0, 1)
    self.s3db.putItem(0, 0, 2)
  
    # checking if the items were inserted
    item_value = self.s3db.getItem(0, 0, 0)
    assert( item_value['project_name'] == 'kasthuri11' )
    assert( item_value['channel_resolution_taskid'] == 'image&0&0' )
    
    item_value = self.s3db.getItem(0, 0, 1)
    assert( item_value['project_name'] == 'kasthuri11' )
    assert( item_value['channel_resolution_taskid'] == 'image&0&0' )
    
    item_value = self.s3db.getItem(0, 0, 2)
    assert( item_value['project_name'] == 'kasthuri11' )
    assert( item_value['channel_resolution_taskid'] == 'image&0&0' )
    
    # inserting two values for task 1, zvalues 0-1
    self.s3db.putItem(0, 0, 0, 1)
    self.s3db.putItem(0, 0, 1, 1)
    
    # checking if the items were updated
    item_value = self.s3db.getItem(0, 0, 0)
    assert( item_value['project_name'] == 'kasthuri11' )
    assert( item_value['channel_resolution_taskid'] == 'image&0&1' )
    
    item_value = self.s3db.getItem(0, 0, 2)
    assert( item_value['project_name'] == 'kasthuri11' )
    assert( item_value['channel_resolution_taskid'] == 'image&0&0' )

  
  def test_queryProjectItems(self):
    """Test the query over SI"""
    
    # inserting three values for task 0, zvalues 0-2
    self.s3db2.putItem(0, 0, 0)
    self.s3db2.putItem(0, 0, 1)
    self.s3db2.putItem(0, 0, 2)
    
    import pdb; pdb.set_trace()
    item_values, count = self.s3db.queryProjectItems()
    print item_values
    item_values, count = self.s3db2.queryChannelItems()
    print item_values
    # assert( count == 1 )
    # assert( item_values[0]['slice_list'] == set([1, 2, 3]) )


  # def test_deleteItem(self):
    # """Test item deletion"""
    
    # value = self.s3db.deleteItem('kasthuri11&image&0&0&0&0')
    # item_value = self.s3db.getItem('kasthuri11&image&0&0&0&0')
    # assert(item_value == None)
    
    # value = self.s3db.deleteItem('kasthuri11&image&0&0&0&1')
    # item_value = self.s3db.getItem('kasthuri11&image&0&0&0&1')
    # assert(item_value == None)
