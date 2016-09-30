# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
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
from ..settings.settings import Settings
settings = Settings.load()
import numpy as np
import blosc
#import cStringIO
import hashlib
from ..ndbucket.cuboidbucket import CuboidBucket
from ..ndingestproj.ingestproj import IngestProj
import pytest
ProjClass = IngestProj.load()
if settings.PROJECT_NAME == 'Boss':
    nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, 124)
else:
    nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_Cuboid_Bucket():

  def setup_class(self):
    """Setup Parameters"""
    if 'S3_ENDPOINT' in dir(settings):
      self.endpoint_url = settings.S3_ENDPOINT
    else:
      self.endpoint_url = None
    CuboidBucket.createBucket(endpoint_url=self.endpoint_url)
    self.cuboid_bucket = CuboidBucket(nd_proj.project_name, endpoint_url=self.endpoint_url)

  def teardown_class(self):
    """Teardown Parameters"""

    # Ensure bucket empty before deleting.
    for objs in self.cuboid_bucket.getAllObjects():
      self.cuboid_bucket.deleteObject(objs.key)

    CuboidBucket.deleteBucket(endpoint_url=self.endpoint_url)

  @pytest.mark.skipif(settings.PROJECT_NAME == 'Boss', reason='putObject() not supported by the Boss')
  def test_put_object(self):
    """Testing put object"""
    cube_data = blosc.pack_array(np.zeros(settings.SUPER_CUBOID_SIZE))
    for morton_index in range(0, 10, 1):
      self.cuboid_bucket.putObject(nd_proj.channel_name, nd_proj.resolution, morton_index, cube_data)

    for morton_index in range(0, 10, 1):
      supercuboid_key = self.cuboid_bucket.generateSupercuboidKey(nd_proj.channel_name, nd_proj.resolution, morton_index)
      self.cuboid_bucket.deleteObject(supercuboid_key)

  def test_put_object_by_key(self):
    hashm = hashlib.md5()
    hashm.update(b'test_cuboidbucket_data')
    cube_data = blosc.pack_array(np.zeros(settings.SUPER_CUBOID_SIZE))

    for morton_index in range(0, 10, 1):
      key = '{}&{}'.format(hashm.hexdigest(), morton_index)
      self.cuboid_bucket.putObjectByKey(key, cube_data)

    for morton_index in range(0, 10, 1):
      key = '{}&{}'.format(hashm.hexdigest(), morton_index)
      self.cuboid_bucket.deleteObject(key)

