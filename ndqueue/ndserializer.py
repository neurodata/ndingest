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
import json
from ..ndqueue.serializer import Serializer

class NDSerializer(Serializer):
    
  @staticmethod
  def encodeUploadMessage(project_name, channel_name, resolution, x_tile, y_tile, z_tile, time_range=None):
    """Encode a message for the upload queue"""
    message = { 
                 'project' : project_name,
                 'channel' : channel_name,
                 'resolution' : resolution,
                 'x_tile' : x_tile,
                 'y_tile' : y_tile,
                 'z_tile' : z_tile,
                 'time_range' : time_range
              }
    return json.dumps(message)

  @staticmethod
  def decode(message_body):
    """Decode a message from the upload queue"""
    return message_body

  @staticmethod
  def encodeIngestMessage(supercuboid_key, message_id, receipt_handle):
    """Enode a message for the ingest queue"""
    message = {
                'supercuboid_key' : supercuboid_key,
                'message_id' : message_id,
                'receipt_handle' : receipt_handle
              }
    return json.dumps(message)

  @staticmethod
  def decodeIngestMessage(message):
    """Delete a decode message"""
    message_dict = json.loads(message)
    return message_dict['supercuboid_key'], message_dict['message_id'], message_dict['receipt_handle']

  @staticmethod 
  def encodeDeleteMessage(supercuboid_key, message_id, receipt_handle):
    """Encode a delete message"""
    message = {
                'supercuboid_key' : supercuboid_key,
                'message_id' : message_id,
                'receipt_handle' : receipt_handle
              }
    return json.dumps(message)

  @staticmethod
  def decodeDeleteMessage(message):
    """Delete a decode message"""
    message_dict = json.loads(message)
    return message_dict['supercuboid_key'], message_dict['message_id'], message_dict['receipt_handle']
