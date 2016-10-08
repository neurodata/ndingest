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
from ndingest.settings.settings import Settings
settings = Settings.load()
import cStringIO
import pytest
import emulambda
from ndctypelib import XYZMorton
from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.ndqueue.cleanupqueue import CleanupQueue
from ndingest.nddynamo.tileindexdb import TileIndexDB
from ndingest.nddynamo.cuboidindexdb import CuboidIndexDB
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.ndbucket.cuboidbucket import CuboidBucket
from ndingest.ndqueue.ndserializer import NDSerializer
from ndingest.ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_IngestLambda:

  def setup_class(self):
    """Setup class parameters"""
    
    # create the tile index table. skip if it exists
    try:
      TileIndexDB.createTable(endpoint_url='http://localhost:8000')
      CuboidIndexDB.createTable(endpoint_url='http://localhost:8000')
    except Exception as e:
      pass
    self.tileindex_db = TileIndexDB(nd_proj.project_name, endpoint_url='http://localhost:8000')
    
    # create the tile bucket
    TileBucket.createBucket(endpoint_url='http://localhost:4567')
    self.tile_bucket = TileBucket(nd_proj.project_name, endpoint_url='http://localhost:4567')
    self.tiles = [self.x_tile, self.y_tile, self.z_tile] = [0, 0, 0]
    
    message_id = 'testing'
    receipt_handle = '123456'
    # insert SUPER_CUBOID_SIZE tiles in the bucket
    for z_index in (self.z_tile, settings.SUPER_CUBOID_SIZE[2], 1):
      tile_handle = cStringIO.StringIO()
      self.tile_bucket.putObject(tile_handle, nd_proj.channel_name, nd_proj.resolution, self.x_tile, self.y_tile, z_index, message_id, receipt_handle)
    
    # creating the cuboid bucket
    CuboidBucket.createBucket(endpoint_url='http://localhost:4567')
    self.cuboid_bucket = CuboidBucket(nd_proj.project_name, endpoint_url='http://localhost:4567')
    
    # create the ingest queue
    IngestQueue.createQueue(nd_proj, endpoint_url='http://localhost:4568')
    self.ingest_queue = IngestQueue(nd_proj, endpoint_url='http://localhost:4568')
    
    # send message to the ingest queue
    morton_index = XYZMorton(self.tiles)
    supercuboid_key = self.cuboid_bucket.generateSupercuboidKey(nd_proj.channel_name, nd_proj.resolution, morton_index)
    response = self.ingest_queue.sendMessage(supercuboid_key)
    
    # create the cleanup queue
    CleanupQueue.createQueue(nd_proj, endpoint_url='http://localhost:4568')

  def teardown_class(self):
    """Teardown class parameters"""
     
    # cleanup tilebucket
    for z_index in (self.z_tile, settings.SUPER_CUBOID_SIZE[2], 1):
      tile_key = self.tile_bucket.encodeObjectKey(nd_proj.channel_name, nd_proj.resolution, self.x_tile, self.y_tile, z_index)
      self.tile_bucket.deleteObject(tile_key)
    
    morton_index = XYZMorton(self.tiles)
    supercuboid_key = self.cuboid_bucket.generateSupercuboidKey(nd_proj.channel_name, nd_proj.resolution, self.tiles)
    self.cuboid_bucket.deleteObject(supercuboid_key)
    # delete created entities
    TileIndexDB.deleteTable(endpoint_url='http://localhost:8000')
    CuboidIndexDB.deleteTable(endpoint_url='http://localhost:8000')
    IngestQueue.deleteQueue(nd_proj, endpoint_url='http://localhost:4568')
    CleanupQueue.deleteQueue(nd_proj, endpoint_url='http://localhost:4568')
    TileBucket.deleteBucket(endpoint_url='http://localhost:4567')
    try:
      CuboidBucket.deleteBucket(endpoint_url='http://localhost:4567')
    except Exception as e:
      pass

  def test_Uploadevent(self):
    """Testing the event"""
    # creating an emulambda function
    func = emulambda.import_lambda('ingestlambda.lambda_handler')
    # creating an emulambda event
    event = emulambda.parse_event(open('../ndlambda/functions/ingest/ingest_event.json').read())
    # calling the emulambda function to invoke a lambda
    emulambda.invoke_lambda(func, event, None, 0, None)
    
    # testing if the supercuboid was inserted in the bucket
    morton_index = XYZMorton(self.tiles)
    cuboid = self.cuboid_bucket.getObject(nd_proj.channel_name, nd_proj.resolution, morton_index)

    # testing if the message was removed from the ingest queue
    for message in self.ingest_queue.receiveMessage():
      # KL TODO write the message id into the JSON event file directly
      print (message)
      # assert False
