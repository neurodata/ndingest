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
sys.path += [os.path.abspath('../../django')]
import ND.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ND.settings'

import pytest
from ndqueue.ingestqueue import IngestQueue
from ndingestproj.ndingestproj import NDIngestProj

# nd_proj == [project, channel, resolution]
nd_proj = NDIngestProj('kasthuri11', 'image', '0')

class Test_Ingest_Queue():

  def setup_class(self):
    """Setup class parameters"""
    IngestQueue.createQueue(nd_proj, endpoint_url='http://localhost:4568')
    self.ingest_queue = IngestQueue(nd_proj, endpoint_url='http://localhost:4568')
  
  def teardown_class(self):
    """Teardown parameters"""
    IngestQueue.deleteQueue(nd_proj, endpoint_url='http://localhost:4568')

  def test_Message(self):
    """Testing the upload queue"""
    
    supercuboid_key = 'kasthuri11&image&0&0'
    self.ingest_queue.sendMessage(supercuboid_key)
    for message_id, receipt_handle, message_body in self.ingest_queue.receiveMessage():
      assert(supercuboid_key == message_body)
      response = self.ingest_queue.deleteMessage(message_id, receipt_handle)
      assert('Successful' in response)
