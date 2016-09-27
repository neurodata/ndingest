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
from __future__ import absolute_import
import sys
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
from nddynamo.boss_tileindexdb import BossTileIndexDB
from ndingestproj.bossingestproj import BossIngestProj
job_id = '123'
nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, job_id, 'test.boss.io')
import json
import six
import unittest
try:
    from unittest.mock import patch
except:
    from mock import patch
import warnings



class Test_BossTileIndexDB(unittest.TestCase):

    def setUp(self):
        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        with open('nddynamo/schemas/boss_tile_index.json') as fp:
            schema = json.load(fp)

        BossTileIndexDB.createTable(schema, endpoint_url=settings.DYNAMO_TEST_ENDPOINT)
        
        self.tileindex_db = BossTileIndexDB(
            nd_proj.project_name, endpoint_url=settings.DYNAMO_TEST_ENDPOINT)
        

    def tearDown(self):
        BossTileIndexDB.deleteTable(endpoint_url=settings.DYNAMO_TEST_ENDPOINT)


    def test_cuboidReady_false(self):
        fake_map = { 'o': 1 }
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            assert(False == self.tileindex_db.cuboidReady(fake_map))


    def test_cuboidReady_true(self):
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1,
            's9': 1, 's10': 1, 's11': 1, 's12': 1, 's13': 1, 's14': 1, 's15': 1, 's16': 1
        }
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            assert(self.tileindex_db.cuboidReady(fake_map))


    def test_createCuboidEntry(self):
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 1)
            preDelResp = self.tileindex_db.getItem('23')
            self.assertEqual('23', preDelResp['chunk_key'])
            self.assertEqual({}, preDelResp['tile_uploaded_map'])


    def test_markTileAsUploaded(self):
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            # Cuboid must first have an entry before one of its tiles may be marked
            # as uploaded.
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 1)

            self.tileindex_db.markTileAsUploaded('fakekey', 'chanA', 0, 1, 1, 1)

            expected = { 'fakekey': 1 }
            resp = self.tileindex_db.getItem('23')
            self.assertEqual(expected, resp['tile_uploaded_map'])


    def test_deleteItem(self):
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 1)
            preDelResp = self.tileindex_db.getItem('23')
            self.assertEqual('23', preDelResp['chunk_key'])
            self.tileindex_db.deleteItem('23')
            postDelResp = self.tileindex_db.getItem('23')
            self.assertIsNone(postDelResp)


    def test_getTaskItems(self):
        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '23'
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 1, task_id=3)

        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '25'
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 16, task_id=3)

        with patch.object(self.tileindex_db, 'generatePrimaryKey') as fake_generator:
            fake_generator.return_value = '29'
            self.tileindex_db.createCuboidEntry('chanA', 0, 1, 1, 32, task_id=3)

        expected = [ 
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': '23'},
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': '25'},
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': '29'}
        ]

        actual = list(self.tileindex_db.getTaskItems(3))

        six.assertCountEqual(self, expected, actual)

   
