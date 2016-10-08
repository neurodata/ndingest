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
import pytest
from ndingest.ndqueue.ingestqueue import IngestQueue
from ndingest.ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
if settings.PROJECT_NAME == 'Boss':
    nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, 12)
else:
    nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_Ingest_Queue():

  def setup_class(self):
    """Setup class parameters"""
    if 'SQS_ENDPOINT' in dir(settings):
      self.endpoint_url = settings.SQS_ENDPOINT
    else:
      self.endpoint_url = None
    IngestQueue.createQueue(nd_proj, endpoint_url=self.endpoint_url)
    self.ingest_queue = IngestQueue(nd_proj, endpoint_url=self.endpoint_url)
  
  def teardown_class(self):
    """Teardown parameters"""
    IngestQueue.deleteQueue(nd_proj, endpoint_url=self.endpoint_url)

  def test_Message(self):
    """Testing the upload queue"""
    
    supercuboid_key = 'kasthuri11&image&0&0'
    self.ingest_queue.sendMessage(supercuboid_key)
    for message_id, receipt_handle, message_body in self.ingest_queue.receiveMessage():
      assert(supercuboid_key == message_body)
      response = self.ingest_queue.deleteMessage(message_id, receipt_handle)
      assert('Successful' in response)
