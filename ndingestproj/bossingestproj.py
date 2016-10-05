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
from ndingest.ndingestproj.ingestproj import IngestProj
from ndingest.util.bossutil import BossUtil


class BossIngestProj(IngestProj):
    def __init__(self, collection_name, experiment_name, channel_name, resolution, job_id):
        """Constructor.

        Collection name and experiment name are joined with an & to form the
        project name.

        Args:
            collection_name (string): Collection name.
            experiment_name (string): Experiment name.
            channel_name (string): Channel name.
            resolution (string): '0' indicates native resolution.
            job_id (string): Id for this ingest job.

        Returns
            (BossIngestProj): An instance
        """
        self._project_name = collection_name + '&' + experiment_name
        self._channel_name = channel_name
        self._resolution = resolution
        self._job_id = job_id

    @classmethod
    def fromTileKey(cls, tile_key):
        """Create a BossIngestProj instance from the chunk key

        Note, the BossIngestProj returned will have None for its job_id and
        will have ids for collection, experiment,  and channel, instead of names.

        Args:
            supercuboid_key(str): The chunk key

        Returns:
            (BossIngestProj):
        """
        parts = BossUtil.decode_tile_key(tile_key)
        return cls(
            parts['collection'], parts['experiment'], parts['channel'],
            parts['resolution'], None)

    @classmethod
    def fromSupercuboidKey(cls, supercuboid_key):
        """Create a BossIngestProject instance from a supercuboid_key aka the chunk key

        Note, the BossIngestProj returned will have None for its job_id and
        will have ids for collection, experiment,  and channel, instead of names.

        Args:
            supercuboid_key(str): The chunk key

        Returns:
            (BossIngestProj):
        """
        parts = BossUtil.decode_chunk_key(supercuboid_key)
        return cls(
            parts['collection'], parts['experiment'], parts['channel'],
            parts['resolution'], None)

    @property
    def project_name(self):
        """For the Boss, this is the collection name and the experiment name, combined.
        """
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
    def job_id(self):
        return self._job_id

    @job_id.setter
    def job_id(self, value):
        self._job_id = value
