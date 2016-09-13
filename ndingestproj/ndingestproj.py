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
from ndingestproj.ingestproj import IngestProj

class NDIngestProj(IngestProj):

  def __init__(self, project_name, channel_name, resolution):
    self._project_name = project_name
    self._channel_name = channel_name
    self._resolution = resolution
  
  @classmethod
  def fromTileKey(cls, tile_key):
    """Create a ndproj from tile key"""
    hash_value, project_name, channel_name, resolution, x_index, y_index, z_index, t_index = tile_key.split('&')
    return cls(project_name, channel_name, resolution), (int(x_index), int(y_index), int(z_index), int(t_index))

  @classmethod
  def fromSupercuboidKey(cls, supercuboid_key):
    """Create a ndproj from supercuboid_key"""
    hash_value, project_name, channel_name, resolution, morton_index, t_index = supercuboid_key.split('&')
    return cls(project_name, channel_name, resolution), (int(morton_index), int(t_index))
  
  @property
  def project_name(self):
    return self._project_name
  
  @project_name.setter
  def project_name(self, value):
    self._project_name = value

  @property 
  def channel_name(self):
    return self._channel_name

  @channel_name.setter
  def channel_name(self, value):
    self._channel_name = value

  @property
  def resolution(self):
    return self._resolution

  @resolution.setter
  def resolution(self, value):
    self._resolution = value
