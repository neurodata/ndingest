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
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import sys
import six
from abc import ABCMeta, abstractmethod
try:
  from ConfigParser import SafeConfigParser
except:
  from configparser import ConfigParser as SafeConfigParser

@six.add_metaclass(ABCMeta)
class Settings(object):

  def __init__(self, file_name, fp=None):
    """Load settings from a config file or a file-like object.
    Args:
        file_name (string): Load config from the named file.
        fp (file-like object): If not None, load config from this.
    """
    self.parser = SafeConfigParser()
    if fp is None:
        self.parser.read(file_name)
        return

    if 'read_file' in dir(self.parser):
        self.parser.read_file(fp)
    else:
        self.parser.readfp(fp)
  
  @abstractmethod
  def setPath(self):
    """Add path to other libraries"""
    return NotImplemented

  @staticmethod
  def load(file_name='settings.ini'):
    """Factory method to load the correct settings file"""
    file_name = os.path.join(os.path.dirname(__file__), file_name)
    parser = SafeConfigParser()
    parser.read(file_name)
    if parser.get('proj', 'PROJECT_NAME') == 'Neurodata':
      from .ndsettings import NDSettings
      return NDSettings(file_name)
    elif parser.get('proj', 'PROJECT_NAME') == 'Boss':
      from .bosssettings import BossSettings
      return BossSettings(file_name)
    else:
      print("Unknown stack {}".format(stack_name))
      raise
  
  @property
  @abstractmethod
  def PROJECT_NAME(self):
    return NotImplemented
  
  @property
  @abstractmethod
  def REGION_NAME(self):
    return NotImplemented
  
  @property
  @abstractmethod
  def AWS_ACCESS_KEY_ID(self):
    return NotImplemented

  @property
  @abstractmethod
  def AWS_SECRET_ACCESS_KEY(self):
    return NotImplemented
  
  @property
  @abstractmethod
  def S3_CUBOID_BUCKET(self):
    return NotImplemented

  @property
  @abstractmethod
  def S3_TILE_BUCKET(self):
    return NotImplemented

  @property
  @abstractmethod
  def DYNAMO_CUBOIDINDEX_TABLE(self):
    return NotImplemented

  @property
  @abstractmethod
  def DYNAMO_TILEINDEX_TABLE(self):
    return NotImplemented

  @property
  @abstractmethod
  def SUPER_CUBOID_SIZE(self):
    return NotImplemented
