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
import six
from abc import ABCMeta, abstractmethod

@six.add_metaclass(ABCMeta)
class Serializer(object):

  @staticmethod
  def setSerializer(serializer_name):
    """Factory method to fetch the correct serializer"""
    if serializer_name == 'Neurodata':
      from serializer.ndserializer import NDSerializer
      return NDSerializer()
    elif serializer_name == 'Boss':
      from bossserializer import BossSerializer
      return BossSerializer()
    else:
      print ("Incorrect Serializer {}".format(serializer_name))
      raise

  @staticmethod
  @abstractmethod
  def encodeUploadMessage(project_name, channel_name, resolution, x_tile, y_tile, z_tile, time_range=None):
    """Encode a message for the upload queue"""
    return NotImplemented

  @staticmethod
  @abstractmethod
  def decode(message_body):
    """Decode a message from the upload queue"""
    return NotImplemented

  @staticmethod
  @abstractmethod
  def encodeIngestMessage(supercuboid_key, message_id, receipt_handle):
    """Enode a message for the ingest queue"""
    return NotImplemented

  @staticmethod
  @abstractmethod
  def decodeIngestMessage(message):
    """Delete a decode message"""
    return NotImplemented

  @staticmethod 
  @abstractmethod
  def encodeDeleteMessage(supercuboid_key, message_id, receipt_handle):
    """Encode a delete message"""
    return NotImplemented

  @staticmethod
  @abstractmethod
  def decodeDeleteMessage(message):
    """Delete a decode message"""
    return NotImplemented
