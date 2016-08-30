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
import cStringIO
sys.path += [os.path.abspath('..')]
from ndbucket.uploadbucket import UploadBucket as UQ

# from moto import mock_s3
key_value = '1_2_3.tif'

project_name = 'kasthuri11'
channel_name = 'image'
resolution = 0

# proj_info = [project, channel, resolution]
proj_info = [project_name, channel_name, str(resolution)]

class Message:
  def __init__(self):
    self.body = key_value
    self.receipt_handle = 'test&handle'

class Test_Upload_Bucket():

  # @mock_s3
  def setup_class(self):
    """Setup Parameters"""
    UQ.createBucket(endpoint_url='http://localhost:4567')

  # @mock_s3
  def teardown_class(self):
    """Teardown Parameters"""
    # pass
    UQ.deleteBucket(endpoint_url='http://localhost:4567')


  # @mock_s3
  def test_put_object(self):
    """Testing put object"""
    
    # import pdb; pdb.set_trace()
    uq = UQ(endpoint_url='http://localhost:4567')
    
    x_tile = 0
    y_tile = 0
    message_id = '1123'
    receipt_handle = 'test_string'

    for z_tile in range(0, 2, 1):
      # creating a tile handle for test
      tile_handle = cStringIO.StringIO()
      # uploading object
      response = uq.putObject(tile_handle, project_name, channel_name, resolution, x_tile, y_tile, z_tile, message_id, receipt_handle)
      tile_handle.close()
      object_key = uq.generateObjectKey(project_name, channel_name, resolution, x_tile, y_tile, z_tile)
      # fetching object
      object_body, object_receipt_handle, object_message_id = uq.getObject(object_key)
      assert( object_message_id == message_id )
      assert( object_receipt_handle == receipt_handle )
      # delete the object
      response = uq.deleteObject(object_key)
