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
from moto import mock_sqs

class Test_Insert_Queue():

  @mock_sqs
  def setup_class(self):
    """Setup class parameters"""
    # IQ.createQueue()
    pass
  
  @mock_sqs
  def teardown_class(self):
    """Teardown parameters"""
    # IQ.deleteQueue()
    pass

  @mock_sqs
  def test_sendMessage(self):
    """Testing the upload queue"""
    
    iq = IQ()
    iq.sendMessage('abc_123')
    message = iq.receiveMessage()
    print message
    iq.deleteMessage(message)
    
    assert(message == 'abc_123')
