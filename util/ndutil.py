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

from __future__ import print_function
from __future__ import absolute_import
import hashlib
from ndingest.util.util import Util

class NDUtil(Util):

  @staticmethod
  def generateCuboidKey(project_name, channel_name, resolution, morton_index, time_index, neariso=False):
    """Generate the key for the supercube"""

    hashm = hashlib.md5()
    hashm.update('{}&{}&{}&{}&{}'.format(project_name, channel_name, resolution, morton_index, time_index))
    if neariso:
      return '{}&{}&{}&{}&{}&{}&neariso'.format(hashm.hexdigest(), project_name, channel_name, resolution, morton_index, time_index)
    else:  
      return '{}&{}&{}&{}&{}&{}'.format(hashm.hexdigest(), project_name, channel_name, resolution, morton_index, time_index)
