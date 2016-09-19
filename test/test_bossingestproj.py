# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import six
from ndingestproj.bossingestproj import BossIngestProj

class TestBossIngestProj(unittest.TestCase):
    def test_generateProjectInfo(self):
        prj = BossIngestProj('col1', 'chanA', '0', 'exp1', 'test.boss.io')
        expected = ['test-boss-io', 'col1&exp1', 'chanA', '0']
        six.assertCountEqual(self, expected, prj.generateProjectInfo())

    def test_project_name(self):
        prj = BossIngestProj('col1', 'chanA', '0', 'exp1', 'test.boss.io')
        expected = 'col1&exp1'
        self.assertEqual(expected, prj.project_name)

    def test_project_name_defaults(self):
        prj = BossIngestProj('col1', 'chanA', '0')
        expected = 'col1&col1'
        self.assertEqual(expected, prj.project_name)

if __name__ == '__main__':
    unittest.main()
