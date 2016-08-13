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
from ndbucket.uploadbucket import UploadBucket as UQ

from moto import mock_s3
import pdb; pdb.set_trace()
key_value = '1_2_3.tif'

class Message:
  def __init__(self):
    self.body = key_value
    self.receipt_handle = 'test&handle'

class Test_Upload_Bucket():

  # @mock_s3
  def setup_class(self):
    """Setup Parameters"""
    UQ.createBucket()

  # @mock_s3
  def teardown_class(self):
    """Teardown Parameters"""
    UQ.deleteBucket()


  # @mock_s3
  def test_put_object(self):
    """Testing put object"""
    
    uq = UQ()
    m = Message()
    uq.putobject(m)
    value = uq.getobject(key_value)
    uq.deleteobject(key_value)
