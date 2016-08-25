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

class UploadMessage:
    
  @staticmethod
  def encode(project_name, channel_name, resolution, x_tile, y_tile, z_tile, time_range=None):
    """Encode a message for the upload queue"""
    return { 'project' : project_name,
             'channel' : channel_name,
             'resolution' : resolution,
             'x_tile' : x_tile,
             'y_tile' : y_tile,
             'z_tile' : z_tile,
             'time_range' : time_range
           }

  @staticmethod
  def decode(message_body):
    """Decode a message from the upload queue"""

    return message_body
