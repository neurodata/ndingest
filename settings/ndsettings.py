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

# from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from .settings import Settings

import os
import sys

class NDSettings(Settings):

  def __init__(self, file_name):
    super(NDSettings, self).__init__(file_name)
    self.setPath()

  def setPath(self):
    """Add path to other libraries"""
    # sys.path.append('..')
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), self.parser.get('path', 'BASE_PATH')))
    NDLIB_PATH = os.path.join(BASE_PATH, self.parser.get('path', 'NDLIB_PATH'))
    SPDB_PATH = os.path.join(BASE_PATH, self.parser.get('path', 'SPDB_PATH'))
    sys.path += ([NDLIB_PATH, SPDB_PATH])
  
  @property
  def PROJECT_NAME(self):
    return self.parser.get('proj', 'PROJECT_NAME')
  
  @property
  def REGION_NAME(self):
    return self.parser.get('aws', 'REGION_NAME')

  @property
  def AWS_ACCESS_KEY_ID(self):
    return self.parser.get('aws', 'AWS_ACCESS_KEY_ID')

  @property
  def AWS_SECRET_ACCESS_KEY(self):
    return self.parser.get('aws', 'AWS_SECRET_ACCESS_KEY') 
  
  @property
  def S3_CUBOID_BUCKET(self):
    return self.parser.get('s3', 'S3_CUBOID_BUCKET')

  @property
  def S3_TILE_BUCKET(self):
    return self.parser.get('s3', 'S3_TILE_BUCKET')

  @property
  def DYNAMO_CUBOIDINDEX_TABLE(self):
    return self.parser.get('dynamo', 'DYNAMO_CUBOIDINDEX_TABLE')

  @property
  def DYNAMO_TILEINDEX_TABLE(self):
    return self.parser.get('dynamo', 'DYNAMO_TILEINDEX_TABLE')

  @property
  def SUPER_CUBOID_SIZE(self):
    return [int(i) for i in self.parser.get('cuboid', 'SUPER_CUBOID_SIZE').split(',')]

  @property
  def DEV_MODE(self):
    return self.parser.getboolean('proj', 'DEV_MODE') 

  @property
  def S3_ENDPOINT(self):
    if self.DEV_MODE:
      return self.parser.get('s3', 'S3_DEV_ENDPOINT')
    else:
      return None

  @property
  def DYNAMO_ENDPOINT(self):
    if self.DEV_MODE:
      return self.parser.get('dynamo', 'DYNAMO_DEV_ENDPOINT')
    else:
      return None
  
  @property
  def SQS_ENDPOINT(self):
    if self.DEV_MODE:
      return self.parser.get('sqs', 'SQS_DEV_ENDPOINT')
    else:
      return None
  
  @property
  def SNS_ENDPOINT(self):
    if self.DEV_MODE:
      return self.parser.get('sns', 'SNS_DEV_ENDPOINT')
    else:
      return None
  
  @property
  def LAMBDA_FUNCTION_LIST(self):
    return [i for i in self.parser.get('lambda', 'LAMBDA_FUNCTION_LIST').split(',')]
