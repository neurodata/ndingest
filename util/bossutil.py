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

from __future__ import absolute_import
import hashlib
from .util import Util

class BossUtil(Util):

    @staticmethod
    def generateCuboidKey(project_name, channel_name, resolution, morton_index, time_index=0):
        """Generate the key for the supercube.  Not used by the Boss.
        """
        return NotImplemented

    @staticmethod
    def decode_chunk_key(key):
        """A method to decode the chunk key

        The tile key is the key used for each individual tile file.

        This should match chunk key encoding/decoding done by the ingest client.

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        result = {}
        parts = key.split('&')
        result["num_tiles"] = int(parts[1])
        result["collection"] = int(parts[2])
        result["experiment"] = int(parts[3])
        result["channel_layer"] = int(parts[4])
        result["resolution"] = int(parts[5])
        result["x_index"] = int(parts[6])
        result["y_index"] = int(parts[7])
        result["z_index"] = int(parts[8])
        result["t_index"] = int(parts[9])

        return result
