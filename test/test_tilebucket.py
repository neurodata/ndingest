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
from io import BytesIO
from ..ndbucket.tilebucket import TileBucket
from ..ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()
if settings.PROJECT_NAME == 'Boss':
    nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, 124)
else:
    nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_Upload_Bucket():

  def setup_class(self):
    """Setup Parameters"""
    if 'S3_ENDPOINT' in dir(settings):
      self.endpoint_url = settings.S3_ENDPOINT
    else:
      self.endpoint_url = None
    TileBucket.createBucket(endpoint_url=self.endpoint_url)
    self.tile_bucket = TileBucket(nd_proj.project_name, endpoint_url=self.endpoint_url)


  def teardown_class(self):
    """Teardown Parameters"""

    # Ensure bucket empty before deleting.
    for objs in self.tile_bucket.getAllObjects():
      self.tile_bucket.deleteObject(objs.key)

    TileBucket.deleteBucket(endpoint_url=self.endpoint_url)


  def test_put_object(self):
    """Testing put object"""
    
    x_tile = 0
    y_tile = 0
    message_id = '1123'
    receipt_handle = 'test_string'

    for z_tile in range(0, 2, 1):
      # creating a tile handle for test
      tile_handle = BytesIO()
      # uploading object
      response = self.tile_bucket.putObject(tile_handle, nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile, message_id, receipt_handle)
      tile_handle.close()
      object_key = self.tile_bucket.encodeObjectKey(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
      # fetching object
      object_body, object_message_id, object_receipt_handle = self.tile_bucket.getObject(nd_proj.channel_name, nd_proj.resolution, x_tile, y_tile, z_tile)
      assert( object_message_id == message_id )
      assert( object_receipt_handle == receipt_handle )
      object_message_id, object_receipt_handle = self.tile_bucket.getMetadata(object_key)
      assert( object_message_id == message_id )
      assert( object_receipt_handle == receipt_handle )
      # delete the object
      response = self.tile_bucket.deleteObject(object_key)


  def test_buildArn_no_folder(self):
    """Test buildArn with folder's default value."""

    expected = 'arn:aws:s3:::my_bucket/*'
    actual = TileBucket.buildArn('my_bucket')
    assert(expected == actual)
    

  def test_buildArn_with_folder_no_slashes(self):
    """Test buildArn with a folder."""

    expected = 'arn:aws:s3:::my_bucket/some/folder/*'
    actual = TileBucket.buildArn('my_bucket', 'some/folder')
    assert(expected == actual)
    

  def test_buildArn_with_folder_with_slashes(self):
    """Test buildArn with folder with slashes at beginning and end."""

    expected = 'arn:aws:s3:::my_bucket/some/folder/*'
    actual = TileBucket.buildArn('my_bucket', '/some/folder/')
    assert(expected == actual)


  def test_createPolicy(self):
    """Test policy creation"""

    statements = [{
      'Sid': 'WriteAccess',
      'Effect': 'Allow',
      'Action': ['s3:PutObject'] 
    }]

    expName = 'ndingest_test_tile_bucket_policy'
    expDesc = 'Test policy creation'

    actual = self.tile_bucket.createPolicy(statements, expName, description=expDesc)

    try:
        assert(expName == actual.policy_name)
        assert(expDesc == actual.description)
        assert(settings.IAM_POLICY_PATH == actual.path)
        assert(actual.default_version is not None)

        # Test that the statements' resource set to this bucket.
        statements = actual.default_version.document['Statement']
        bucket_name = TileBucket.getBucketName()
        arn = 'arn:aws:s3:::{}/*'.format(bucket_name)
        for stmt in statements:
            assert(stmt['Resource'] == arn)
    finally:
        actual.delete()


  def test_createPolicy_with_folder(self):
    """Test policy creation with a folder"""

    statements = [{
      'Sid': 'WriteAccess',
      'Effect': 'Allow',
      'Action': ['s3:PutObject'] 
    }]

    expName = 'ndingest_test_tile_bucket_policy'
    folder = 'some/folder'

    actual = self.tile_bucket.createPolicy(statements, expName, folder)

    try:
        assert(expName == actual.policy_name)
        assert(settings.IAM_POLICY_PATH == actual.path)
        assert(actual.default_version is not None)

        # Test that the statements' resource set to this bucket and folder.
        statements = actual.default_version.document['Statement']
        bucket_name = TileBucket.getBucketName()
        arn = 'arn:aws:s3:::{}/{}/*'.format(bucket_name, folder)
        for stmt in statements:
            assert(stmt['Resource'] == arn)
    finally:
        actual.delete()


  def test_deletePolicy(self):
    """Test policy deletion"""

    statements = [{
      'Sid': 'WriteAccess',
      'Effect': 'Allow',
      'Action': ['s3:PutObject'] 
    }]

    expName = 'ndingest_test_tile_bucket_policy'
    policy = self.tile_bucket.createPolicy(statements, expName)
    assert(expName == policy.policy_name)
    self.tile_bucket.deletePolicy(expName)
    assert(self.tile_bucket.getPolicyArn(expName) is None)


if __name__ == '__main__':
    sut = Test_Upload_Bucket()
    sut.setup_class()
    sut.test_createPolicy()
    sut.teardown_class()
