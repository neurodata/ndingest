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

import pytest
from ndqueue.insertqueue import InsertQueue as IQ

# proj_info = [project, channel, resolution]
proj_info = ['kasthuri11', 'image', '0']

class Test_Insert_Queue():

  def setup_class(self):
    """Setup class parameters"""
    IQ.createQueue(proj_info)
    self.insert_queue = IQ(proj_info)
    pass
  
  def teardown_class(self):
    """Teardown parameters"""
    IQ.deleteQueue(proj_info)
    pass

  def test_Message(self):
    """Testing the upload queue"""
    
    supercuboid_key = 'kasthuri11&image&0&0'
    self.insert_queue.sendMessage(supercuboid_key)
    for message_id, receipt_handle, message_body in self.insert_queue.receiveMessage():
      assert(supercuboid_key == message_body)
      response = self.insert_queue.deleteMessage(message_id, receipt_handle)
      assert('Successful' in response)
