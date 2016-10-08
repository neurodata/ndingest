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
from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.nddynamo.tileindexdb import TileIndexDB
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.ndqueue.serializer import Serializer
serializer = Serializer.load()
from ndingest.ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
nd_proj = ProjClass('kasthuri11', 'image', '0')

class Test_UploadLambda:

  def setup_class(self):
    """Setup class parameters"""
    # create the tile index table. skip if it exists
    try:
      TileIndexDB.createTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    except Exception as e:
      pass
    self.tileindex_db = TileIndexDB(nd_proj.project_name, endpoint_url=settings.DYNAMO_ENDPOINT)
    # create the ingest queue
    IngestQueue.createQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)
    # create the upload queue
    UploadQueue.createQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)
    self.upload_queue = UploadQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)
    tile_bucket = TileBucket(nd_proj.project_name, endpoint_url=settings.S3_ENDPOINT)
    [self.x_tile, self.y_tile, self.z_tile] = [0, 0, 0]
    message = serializer.encodeUploadMessage(nd_proj.project_name, nd_proj.channel_name, nd_proj.resolution, self.x_tile, self.y_tile, self.z_tile)
    # insert message in the upload queue
    self.upload_queue.sendMessage(message)
    # receive message and upload object
    for message_id, receipt_handle, message_body in self.upload_queue.receiveMessage():
      tile_handle = cStringIO.StringIO()
      tile_bucket.putObject(tile_handle, nd_proj.channel_name, nd_proj.resolution, self.x_tile, self.y_tile, self.z_tile, message_id, receipt_handle)

  def teardown_class(self):
    """Teardown class parameters"""
    TileIndexDB.deleteTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    IngestQueue.deleteQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)
    UploadQueue.deleteQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)

  def test_Uploadevent(self):
    """Testing the event"""
    # creating an emulambda function
    func = emulambda.import_lambda('uploadlambda.lambda_handler')
    # creating an emulambda event
    event = emulambda.parse_event(open('../ndlambda/functions/upload/upload_event.json').read())
    # calling the emulambda function to invoke a lambda
    emulambda.invoke_lambda(func, event, None, 0, None)

    # testing if the index was updated in tileindexdb
    supercuboid_key = self.tileindex_db.generatePrimaryKey(nd_proj.channel_name, nd_proj.resolution, self.x_tile, self.y_tile, self.z_tile)
    item = self.tileindex_db.getItem(supercuboid_key)
    assert(item['zindex_list'] == set([0]))

    # testing if the message was deleted from the upload queue or not
    for message in self.upload_queue.receiveMessage():
      assert False
