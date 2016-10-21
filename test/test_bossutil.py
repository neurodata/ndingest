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

import unittest
from ndingest.util.bossutil import BossUtil
from ndingest.ndbucket.tilebucket import TileBucket
from ndingest.ndqueue.uploadqueue import UploadQueue
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.settings.settings import Settings
settings = Settings.load()
import os
import warnings

@unittest.skipIf(settings.PROJECT_NAME == 'Neurodata', 'Test not applicable to Neurodata')
class TestBossUtil(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Silence warnings about open boto3 sessions.
        warnings.filterwarnings('ignore')

        cls.job_id = 123
        cls.nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, cls.job_id)

        TileBucket.createBucket()
        cls.tile_bucket = TileBucket(cls.nd_proj.project_name)

        UploadQueue.createQueue(cls.nd_proj)
        cls.upload_queue = UploadQueue(cls.nd_proj)

    @classmethod
    def tearDownClass(cls):
        UploadQueue.deleteQueue(cls.nd_proj)
        TileBucket.deleteBucket()

    def test_create_ingest_policy(self):
        policy = BossUtil.generate_ingest_policy(
            self.job_id, self.upload_queue, self.tile_bucket)
        try:
            self.assertEqual(settings.IAM_POLICY_PATH, policy.path)
            self.assertIsNotNone(policy.default_version)
            statements = policy.default_version.document['Statement']
            for stmt in statements:
                self.assertIn(
                    stmt['Resource'], 
                    [self.upload_queue.arn, TileBucket.buildArn(self.tile_bucket.bucket.name)])
        finally:
            policy.delete()

    def test_delete_ingest_policy(self):
        policy = BossUtil.generate_ingest_policy(
            self.job_id, self.upload_queue, self.tile_bucket)
        self.assertTrue(BossUtil.delete_ingest_policy(self.job_id))


if __name__ == '__main__':
    unittest.main()
