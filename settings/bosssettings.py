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

from .settings import Settings
try:
    from ConfigParser import Error
except:
    from configparser import Error

class BossSettings(Settings):
    
    def __init(self, file_name, fp=None):
        super(BossSettings, self).__init__(file_name, fp)

    def setPath(self):
        """Add path to other libraries"""

    @property
    def PROJECT_NAME(self):
        return 'Boss'
  
    @property
    def REGION_NAME(self):
        """Defaults to us-east-1."""
        try:
            return self.parser.get('aws', 'region')
        except Error:
            return 'us-east-1'
  
    @property
    def AWS_ACCESS_KEY_ID(self):
        """Defaults to None.  Normally only set this for testing."""
        try:
            return self.parser.get('aws', 'access_key_id')
        except Error:
            return None

    @property
    def AWS_SECRET_ACCESS_KEY(self):
        """Defaults to None.  Normally only set this for testing."""
        try:
            return self.parser.get('aws', 'secret_key')
        except Error:
            return None
  
    @property
    def S3_CUBOID_BUCKET(self):
        return self.parser.get('aws', 'cuboid_bucket')

    @property
    def S3_TILE_BUCKET(self):
        return self.parser.get('aws', 'tile_bucket')

    @property
    def DYNAMO_CUBOIDINDEX_TABLE(self):
        return self.parser.get('aws', 'cuboid_index_table')

    @property
    def DYNAMO_TILEINDEX_TABLE(self):
        return self.parser.get('aws', 'tile_index_table')

    @property
    def UPLOAD_TASK_QUEUE(self):
        return self.parser.get('aws', 'upload_task_queue_url')

    @property
    def UPLOAD_TASK_DEADLETTER_QUEUE(self):
        return self.parser.get('aws', 'upload_task_deadletter_queue_url')

    @property
    def INGEST_QUEUE(self):
        return self.parser.get('aws', 'ingest_queue_url')

    @property
    def INGEST_DEADLETTER_QUEUE(self):
        return self.parser.get('aws', 'ingest_deadletter_queue_url')

    @property
    def SUPER_CUBOID_SIZE(self):
        return [int(i) for i in self.parser.get('spdb', 'SUPER_CUBOID_SIZE').split(',')]

    @property
    def UPLOAD_TASK_QUEUE(self):
        return self.parser.get('aws', 'upload_task_queue_url')

    @property
    def UPLOAD_TASK_DEADLETTER_QUEUE(self):
        return self.parser.get('aws', 'upload_task_deadletter_queue_url')
