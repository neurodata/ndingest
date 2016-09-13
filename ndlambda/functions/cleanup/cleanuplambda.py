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

import urllib
import boto3
import json
import cStringIO
from PIL import Image
from django.conf import settings
import ndlib
from ndqueue.ingestqueue import IngestQueue
from ndqueue.cleanupqueue import CleanupQueue
from nddynamo.tileindexdb import TileIndexDB
from ndbucket.tilebucket import TileBucket
from ndbucket.cuboidbucket import CuboidBucket
from nddynamo.cuboidindexdb import CuboidIndexDB
from ndingestproj.ndingestproj import NDIngestProj 
from ndqueue.serializer import Serializer

def lambda_handler(event, context):
  """Arrange data in the state database and ready it for ingest"""
  
  # receive SNS notification event
  # message_id, receipt_handle, supercuboid_key = ???
  import pdb; pdb.set_trace()
  message = event['Records'][0]['Sns']['Message']
  supercuboid_key, message_id, receipt_handle = NDSerializer.decodeDeleteMessage(message)
  
  # create ndproj from SNS notification
  nd_proj, (morton_index, time_index) = NDIngestProj.fromSupercuboidKey(supercuboid_key)
  [x_index, y_index, z_index] = ndlib.MortonXYZ(morton_index)

  # delete tiles from tile_bucket
  tile_bucket = TileBucket(nd_proj.project_name, endpoint_url=settings.S3_ENDPOINT)
  for z in range(z_index, SUPER_CUBOID_SIZE, 1):
    object_key = tile_bucket.encodeObjectKey(nd_proj.channel_name, nd_proj.resolution, x_index, y_index, z)
    tile_bucket.deleteObject(object_key) 

  # delete tiles from tileindex_db
  tileindex_db = TileIndexDB(nd_proj.project_name, endpoint_url=settings.DYNAMO_ENDPOINT)
  tileindex_db.deleteItem(supercuboid_key)

  # delete message from cleanup queue
  cleanup_queue = CleanupQueue(nd_proj, endpoint_url=settings.SQS_ENDPOINT)
  cleanup_queue.deleteMessage(message_id, receipt_handle)
