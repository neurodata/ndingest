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
import json
from ndqueue.uploadqueue import UploadQueue
from ndqueue.serializer import Serializer
serializer = Serializer.load()
from ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_UploadQueue():

  def setup_class(self):
    """Setup the class"""
    if 'SQS_ENDPOINT' in dir(settings):
      self.endpoint_url = settings.SQS_ENDPOINT
    else:
      self.endpoint_url = None
    UploadQueue.createQueue(nd_proj, endpoint_url=self.endpoint_url)
    self.upload_queue = UploadQueue(nd_proj, endpoint_url=self.endpoint_url)


  def teardown_class(self):
    """Teardown parameters"""
    UploadQueue.deleteQueue(nd_proj, endpoint_url=self.endpoint_url)

  
  def test_Message(self):
    """Test put, get and delete Message"""
    
    x_tile = 0
    y_tile = 0

    for z_tile in range(0, 2, 1):
      # encode the message
      message = serializer.encodeUploadMessage(nd_proj.project_name, nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
      # send message to the queue
      self.upload_queue.sendMessage(message)
    
    # receive message from the queue
    for message_id, receipt_handle, message_body in self.upload_queue.receiveMessage(number_of_messages=3):
      # check if we get the tile_info back correctly
      assert(message_body['z_tile'] in [0, 1, 2])
      # delete message from the queue
      response = self.upload_queue.deleteMessage(message_id, receipt_handle)
      # check if the message was sucessfully deleted
      assert('Successful' in response)
