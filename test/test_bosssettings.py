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
from ndingest.settings.bosssettings import BossSettings
from six import StringIO

class TestBossSettings(unittest.TestCase):
    def setUp(self):
        self.base_config = """
[boss]
domain = test.boss.io
[aws]
"""

        self.fp = StringIO(self.base_config)
        return super(TestBossSettings, self).setUp()

    def test_region_default(self):
        settings = BossSettings(None, self.fp)
        self.assertEqual('us-east-1', settings.REGION_NAME)

    def test_access_key_id_default(self):
        settings = BossSettings(None, self.fp)
        self.assertIsNone(settings.AWS_ACCESS_KEY_ID)

    def test_secret_key_default(self):
        settings = BossSettings(None, self.fp)
        self.assertIsNone(settings.AWS_SECRET_ACCESS_KEY)

    def test_domain_no_periods(self):
        settings = BossSettings(None, self.fp)
        self.assertEqual('test-boss-io', settings.DOMAIN)


if __name__ == '__main__':
    unittest.main()
