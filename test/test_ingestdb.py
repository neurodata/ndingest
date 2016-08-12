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

import sys
import os
sys.path += [os.path.abspath('..')]
from nddynamo.ingestdb import IngestDB as IDB


try:
  IDB.createTable()
  idb = IDB()
  idb.updateItem('0_0_1.tif')
  idb.updateItem('0_0_2.tif')
  idb.updateItem('0_0_3.tif')
  item_value = idb.getItem('kasthuri11&image&0&0&0&0')
  print item_value
  assert(item_value == [1])
  value = idb.deleteItem(key_value)
  print value
except Exception as e:
  print e
  pass

IDB.deleteTable()
