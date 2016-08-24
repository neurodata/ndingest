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
import json
from ndqueue.uploadqueue import UploadQueue as UQ

# proj_info = [project, channel, resolution]
proj_info = ['kasthuri11', 'image', '0']

class Test_UploadQueue():

  def setup_class(self):
    """Setup the class"""
    UQ.createQueue(proj_info)
    self.upload_queue = UQ(proj_info)

  def teardown_class(self):
    """Teardown parameters"""
    UQ.deleteQueue(proj_info)

  
  def test_Message(self):
    """Test put, get and delete Message"""
    
    tile_info = { 'project' : 'kasthuri11',
                  'channel' : 'image',
                  'x_tile'  : 1,
                  'y_tile'  : 0,
                  'x_tile'  : 1
                }

    # send message to the queue
    self.upload_queue.sendMessage(tile_info)
    self.upload_queue.sendMessage(tile_info)
    self.upload_queue.sendMessage(tile_info)
    # receive message from the queue
    for message_id, receipt_handle, message_body in self.upload_queue.receiveMessage(number_of_messages=3):
      # check if we get the tile_info back correctly
      assert(tile_info == message_body)
      # delete message from the queue
      response = self.upload_queue.deleteMessage(message_id, receipt_handle)
      # check if the message was sucessfully deleted
      assert('Successful' in response)
