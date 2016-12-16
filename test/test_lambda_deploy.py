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

from __future__ import absolute_import
from __future__ import print_function
import sys
sys.path.append('..')
from ..settings.settings import Settings
settings = Settings.load()

# ProjClass = IngestProj.load()
# nd_proj = ProjClass('kasthuri11', 'image', '0')


class Test_Ingest_Lambda(object):

  def setup_class(self):
    """Setup the class"""
    pass

  def teardown_class(self):
    """Teardown the class"""
    pass

  def test_create_function(self):
    """Tesing creating function"""
    pass
