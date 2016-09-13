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
import six
from abc import ABCMeta, abstractmethod

@six.add_metaclass(ABCMeta)
class IngestProj(object):

  @staticmethod
  def load(project_type):
    """Factory to load the correct ndproj type"""
    if project_type == 'Neurodata':
      from ndingestproj.ndingestproj import NDIngestProj
      return NDIngestProj
    elif project_type == 'Boss':
      from ndingestproj.bossingestproj import BossIngestProj
      return BossIngestProj
    else:
      print ("Unknown Project-type {}".format(project_type))
      raise

  @classmethod
  def fromTileKey(cls, tile_key):
    """Create a ndproj from supercuboid_key"""
    return NotImplemented

  def generateProjectInfo(self):
    return self.__dict__.values()
  
  @classmethod
  @abstractmethod
  def fromTileKey(cls, tile_key):
    """Create a ndproj from tile key"""
    return NotImplemented

  @classmethod
  @abstractmethod
  def fromSupercuboidKey(cls, supercuboid_key):
    """Create a ndproj from supercuboid_key"""
    return NotImplemented
  
  @property
  @abstractmethod
  def project_name(self):
    return NotImplemented
    return self._project_name
  
  @project_name.setter
  @abstractmethod
  def project_name(self, value):
    return NotImplemented
    self._project_name = value

  @property 
  @abstractmethod
  def channel_name(self):
    return NotImplemented
    return self._channel_name

  @channel_name.setter
  @abstractmethod
  def channel_name(self, value):
    return NotImplemented
    self._channel_name = value

  @property
  @abstractmethod
  def resolution(self):
    return NotImplemented
    return self._resolution

  @resolution.setter
  @abstractmethod
  def resolution(self, value):
    return NotImplemented
