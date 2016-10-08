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
# limitations under the License.\

from __future__ import absolute_import
from __future__ import print_function
from ndingest.ndqueue.serializer import Serializer
import json

class BossSerializer(Serializer):
  """This class will likely not be used by the Boss.

    The Boss endpoint will encode and decode messages within its own code base.
  """
    
  @staticmethod
  def encodeUploadMessage(project_name, channel_name, resolution, x_tile, y_tile, z_tile, time_range=None):
    """Encode a message for the upload queue"""
    msg = {}
    msg['chunk_key'] = '{}&{}&{}&{}&{}&{}'.format(
        project_name, channel_name, resolution, x_tile, y_tile, z_tile)
    if time_range is not None:
        msg['chunk_key'] += '&{}'.format(time_range)

    # Temporarily used for existing upload queue test.
    msg['z_tile'] = z_tile

    return json.dumps(msg)

  @staticmethod
  def decode(message_body):
    """Decode a message from the upload queue"""
    return NotImplemented

  @staticmethod
  def encodeIngestMessage(supercuboid_key, message_id, receipt_handle):
    """Enode a message for the ingest queue"""
    return NotImplemented

  @staticmethod
  def decodeIngestMessage(message):
    """Delete a decode message"""
    return NotImplemented

  @staticmethod 
  def encodeDeleteMessage(supercuboid_key, message_id, receipt_handle):
    """Encode a delete message"""
    return NotImplemented

  @staticmethod
  def decodeDeleteMessage(message):
    """Delete a decode message"""
    return NotImplemented
