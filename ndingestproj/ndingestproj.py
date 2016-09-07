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

class NDIngestProj:

  def __init__(self, project_name, channel_name, resolution):
    self.project_name = project_name
    self.channel_name = channel_name
    self.resolution = resolution
  
  @classmethod
  def fromSupercuboidKey(cls, tile_key):
    """Create a ndproj from supercuboid_key"""
    hash_value, project_name, channel_name, resolution, x_tile, y_tile, z_tile = supercuboid_key.split('&')
    return cls(project_name, channel_name, resolution)

  @property
  def project_name(self):
    return self.project_name

  @property 
  def channel_name(self):
    return self.channel_name

  @property
  def resolution(self):
    return self.resolution

  def generateProjectInfo(self):
    return self.__dict__.keys()
