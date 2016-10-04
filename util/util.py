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
from abc import ABCMeta, abstractmethod
import six
from ndingest.settings.settings import Settings
settings = Settings.load()

@six.add_metaclass(ABCMeta)
class Util(object):

  @staticmethod
  def load():
    """Factory method to load the correct util file"""
    if settings.PROJECT_NAME == 'Neurodata':
      from ndingest.util.ndutil import NDUtil
      return NDUtil
    elif settings.PROJECT_NAME == 'Boss':
      from ndingest.util.bossutil import BossUtil
      return BossUtil
    else:
      print('Unknown project name {}'.format(settings.PROJECT_NAME ))
      raise

  @staticmethod
  @abstractmethod
  def generateCuboidKey(project_name, channel_name, resolution, morton_index, time_index=0):
    """Generate the cuboid key"""
    return NotImplemented
