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
sys.path += [os.path.abspath('../../django/')]
import ND.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ND.settings'

import json
from ndqueue.uploadqueue import UploadQueue as UQ
from ndqueue.uploadmessage import UploadMessage as UM

project_name = 'kasthuri11'
channel_name = 'image'
resolution = 0

# proj_info = [project, channel, resolution]
proj_info = [project_name, channel_name, str(resolution)]

class Test_UploadQueue():

  def setup_class(self):
    """Setup the class"""
    UQ.createQueue(proj_info, endpoint_url='http://localhost:4568')
    self.upload_queue = UQ(proj_info, endpoint_url='http://localhost:4568')

  def teardown_class(self):
    """Teardown parameters"""
    UQ.deleteQueue(proj_info, endpoint_url='http://localhost:4568')

  
  def test_Message(self):
    """Test put, get and delete Message"""
    
    x_tile = 0
    y_tile = 0

    for z_tile in range(0, 2, 1):
      # encode the message
      message = UM.encode(project_name, channel_name, resolution, x_tile, y_tile, z_tile)
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
