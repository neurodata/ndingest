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
settings = Settings.load('Neurodata')
from ndingestproj.ndingestproj import NDIngestProj
from ndqueue.serializer import Serializer
serializer = Serializer.load('Neurodata')
from ndsns.ndsns import NDSns
nd_proj = NDIngestProj('kasthuri11', 'image', '0')


class Test_SNS:

  def setup_class(self):
    """Setup the class"""
    self.topic_arn = NDSns.createTopic(nd_proj, endpoint_url='http://localhost:9292')
    self.sns = NDSns(self.topic_arn, endpoint_url='http://localhost:9292')

  def teardown_class(self):
    """Teardown the class"""
    NDSns.deleteTopic(self.topic_arn, endpoint_url='http://localhost:9292')

  def test_publish(self):
    """Test publishing a message"""
    target_arn = 'testing'
    supercuboid_key = 'acd123'
    message_id = '123456'
    receipt_handle = 'a1b2c3d4'
    message = serializer.encodeIngestMessage(supercuboid_key, message_id, receipt_handle)
    self.sns.publish(self.topic_arn, message)
    message = self.sns.subscribe(self.topic_arn)
