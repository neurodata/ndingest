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

import os
import sys
sys.path += [os.path.abspath('../../../')]

import urllib
import boto3
import json

from ndqueue.uploadqueue import UploadQueue
from ndqueue.insertqueue import InsertQueue
from nddynamo.ingestdb import IngestDB

def lambda_handler(event, context):
  """Arrange data in the state database and ready it for ingest"""
 
  # extract bucket name and object key from the event
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
  
  # update value in the dynamo table
  ingest_db = IngestDB()
  ingest_db.updateItem(key)

  # TODO KL Check if we have all tiles

  # insert a new job in the insert queue if we have all the tiles
  insert_queue = InsertQueue()
  insert_queue.sendMessage(key)

  # delete message from upload queue
  upload_queue = UploadQueue()
  # KL TODO How do we get the message id in here
  upload_queue.deleteMessage(message)
