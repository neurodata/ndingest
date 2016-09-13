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
from settings.settings import Settings
settings = Settings.load()
import urllib
import boto3
import json
from ndqueue.uploadqueue import UploadQueue
from ndqueue.ingestqueue import IngestQueue
from nddynamo.tileindexdb import TileIndexDB
from ndbucket.tilebucket import TileBucket
from ndingestproj.ndingestproj import NDIngestProj 


def lambda_handler(event, context):
  """Arrange data in the state database and ready it for ingest"""

  # extract bucket name and object key from the event
  bucket = event['Records'][0]['s3']['bucket']['name']
  tile_key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')

  nd_proj, tile_args = NDIngestProj.fromTileKey(tile_key)
  
  # update value in the dynamo table
  tileindex_db = TileIndexDB(nd_proj.project_name, endpoint_url='http://localhost:8000')
  supercuboid_key, supercuboid_ready = tileindex_db.putItem(nd_proj.channel_name, nd_proj.resolution, *tile_args)

  # ingest the supercuboid if we have all the tiles
  if supercuboid_ready:
    # insert a new job in the insert queue if we have all the tiles
    ingest_queue = IngestQueue(nd_proj, endpoint_url='http://localhost:4568')
    ingest_queue.sendMessage(supercuboid_key)
  
  # fetch message_id and receipt_handle from the s3 object
  tile_bucket = TileBucket(nd_proj.project_name, endpoint_url='http://localhost:4567')
  message_id, receipt_handle = tile_bucket.getMetadata(tile_key)

  # delete message from upload queue
  upload_queue = UploadQueue(nd_proj, endpoint_url='http://localhost:4568')
  upload_queue.deleteMessage(message_id, receipt_handle)
