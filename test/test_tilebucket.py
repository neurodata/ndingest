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

from __future__ import absolute_import
from __future__ import print_function
import sys
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
import cStringIO
from ndbucket.tilebucket import TileBucket
from ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_Upload_Bucket():

  def setup_class(self):
    """Setup Parameters"""
    TileBucket.createBucket(endpoint_url='http://localhost:4567')
    self.tile_bucket = TileBucket(nd_proj.project_name, endpoint_url='http://localhost:4567')


  def teardown_class(self):
    """Teardown Parameters"""
    TileBucket.deleteBucket(endpoint_url='http://localhost:4567')


  def test_put_object(self):
    """Testing put object"""
    
    x_tile = 0
    y_tile = 0
    message_id = '1123'
    receipt_handle = 'test_string'

    for z_tile in range(0, 2, 1):
      # creating a tile handle for test
      tile_handle = cStringIO.StringIO()
      # uploading object
      response = self.tile_bucket.putObject(tile_handle, nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile, message_id, receipt_handle)
      tile_handle.close()
      object_key = self.tile_bucket.encodeObjectKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
      # fetching object
      object_body, object_message_id, object_receipt_handle = self.tile_bucket.getObject(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
      assert( object_message_id == message_id )
      assert( object_receipt_handle == receipt_handle )
      object_message_id, object_receipt_handle = self.tile_bucket.getMetadata(object_key)
      assert( object_message_id == message_id )
      assert( object_receipt_handle == receipt_handle )
      # delete the object
      response = self.tile_bucket.deleteObject(object_key)
