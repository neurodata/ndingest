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
import warnings



class Test_BossTileIndexDB(unittest.TestCase):
    """
    Note that the chunk keys used, for testing, do not have real hash keys.
    The rest of the chunk key is valid.
    """

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
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertFalse(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_true(self):
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1,
            's9': 1, 's10': 1, 's11': 1, 's12': 1, 's13': 1, 's14': 1, 's15': 1, 's16': 1
        }
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertTrue(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_small_cuboid_true(self):
        """Test case where the number of tiles is smaller than a cuboid in the z direction."""
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1
        }

        num_tiles = 8
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertTrue(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_small_cuboid_false(self):
        """Test case where the number of tiles is smaller than a cuboid in the z direction."""
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1
        }

        num_tiles = 8
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertFalse(self.tileindex_db.cuboidReady(chunk_key, fake_map))

    def test_createCuboidEntry(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        task_id = 21
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)
        preDelResp = self.tileindex_db.getCuboid(chunk_key)
        self.assertEqual(chunk_key, preDelResp['chunk_key'])
        self.assertEqual({}, preDelResp['tile_uploaded_map'])


    def test_markTileAsUploaded(self):
        # Cuboid must first have an entry before one of its tiles may be marked
        # as uploaded.
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key, 231)

        self.tileindex_db.markTileAsUploaded(chunk_key, 'fakekey')

        expected = { 'fakekey': 1 }
        resp = self.tileindex_db.getCuboid(chunk_key)
        self.assertEqual(expected, resp['tile_uploaded_map'])


    def test_deleteItem(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key, 231)
        preDelResp = self.tileindex_db.getCuboid(chunk_key)
        self.assertEqual(chunk_key, preDelResp['chunk_key'])
        self.tileindex_db.deleteCuboid(chunk_key)
        postDelResp = self.tileindex_db.getCuboid(chunk_key)
        self.assertIsNone(postDelResp)


    def test_getTaskItems(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key1 = '<hash>&{}&111&222&333&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key1, task_id=3)

        chunk_key2 = '<hash>&{}&111&222&333&0&1&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key2, task_id=3)

        chunk_key3 = '<hash>&{}&111&222&333&0&2&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key3, task_id=3)

        # Cuboid for a different upload job.
        chunk_key4 = '<hash>&{}&555&666&777&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key4, task_id=5)

        expected = [ 
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': chunk_key1},
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': chunk_key2},
            {'task_id': 3, 'tile_uploaded_map': {}, 'chunk_key': chunk_key3}
        ]

        actual = list(self.tileindex_db.getTaskItems(3))

        six.assertCountEqual(self, expected, actual)

