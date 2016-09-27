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

from __future__ import print_function
# from __future__ import absolute_import
from settings.settings import Settings
settings = Settings.load()
import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
import blosc

try:
  # Temp try-catch while developing on Windows.
  from spdb.c_lib.ndlib import XYZMorton
except Exception:
  pass
from util.util import Util

UtilClass = Util.load()


class BossCuboidIndexDB:
    def __init__(self, region_name=settings.REGION_NAME, endpoint_url=None):

        # create the resource
        table_name = BossCuboidIndexDB.getTableName()
        dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url,
                                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        self.table = dynamo.Table(table_name)

    @staticmethod
    def getTableName():
        """Return table name"""
        return settings.DYNAMO_CUBOIDINDEX_TABLE

    def generatePrimaryKey(self, collection, experiment, channel, resolution, x, y, z, time=0):
        """Generate key for cuboid.

        Args:
            collection (string): Collection name.
            experiment (string): Experiment name.
            channel (string): Channel name.
            resolution (int): 0 = native resolution.
            time (optional[int]): Time sample, defaults to 0.

        Returns:
            (string)
        """
        morton_index = XYZMorton([x, y, z])
        lookup_key = '{}&{}&{}'.format(collection, experiment, channel)
        hash = 0
        return '{}&{}&{}&{}&{}'.format(hash, lookup_key, resolution, time, morton_index)

    def addCuboid(self, collection, experiment, channel, resolution, x, y, z, time=0, task_id=0, version_node=0):
        """Inserting an index for a cuboid.

        Args:
            collection (string): Collection name.
            experiment (string): Experiment name.
            channel (string): Channel name.
            resolution (int): 0 = native resolution.
            time (optional[int]): Time sample, defaults to 0.
            task_id (optional[int]): Task or job ID, defaults to 0.
            version_node (optional[int]): Support for versioning cuboids.  Defaults to 0.
        """

        # This key will be passed in as part of the queue's message.
        object_key = self.generatePrimaryKey(collection, experiment, channel, resolution, x, y, z, time)
        version_number = 0

        try:
            response = self.table.put_item(
                Item={
                    'object-key': object_key,
                    'version-node': version_number,
                    'ingest-job-hash': 'foo',  # hash of collection name
                    'ingest-job-range': '{}&{}&{}&{}&{}'.format(experiment, channel, resolution, task_id)
                },
                ReturnValues='NONE',
                ReturnConsumedCapacity='INDEXES'
            )
        except botocore.exceptions.ClientError as e:
            print(e)
            raise
