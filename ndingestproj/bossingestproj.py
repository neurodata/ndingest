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
from ndingestproj.ingestproj import IngestProj

class BossIngestProj(IngestProj):

    def __init__(
        self, collection_name, channel_name, 
        resolution, experiment_name=None, domain_name='test.boss.io'):
        """Constructor.

        Collection name and experiment name are joined with an & to form the
        project name.  If experiment name isn't provided, the project_name is
        '{}&{}'.format(collection_name, collection_name).  
        
        Both experiment_name and domain_name are optional to maintain 
        compatibility with Neurodata.

        Args:
            collection_name (string): Collection name.
            channel_name (string): Channel name.
            resolution (string): '0' indicates native resolution.
            experiment_name (optional[string]): Defaults to None.
            domain_name (optional[string]): Defaults to 'test.boss.io'.  Domain ndingest running in.  Note, periods will be replaced with dashes for compatibility with AWS.
        """
        self._domain_name = domain_name.replace('.', '-')
        if experiment_name is not None:
            self._project_name = collection_name + '&' + experiment_name
        else:
            self._project_name = collection_name + '&' + collection_name
        self._channel_name = channel_name
        self._resolution = resolution

    @classmethod
    def fromTileKey(cls, tile_key):
        """Create a ndproj from supercuboid_key"""
        return NotImplemented

    @classmethod
    def fromSupercuboidKey(cls, supercuboid_key):
        """Create a ndproj from supercuboid_key"""
        return NotImplemented
  
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

    @property
    def domain(self):
        return self._domain_name

