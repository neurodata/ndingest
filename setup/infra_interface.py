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
sys.path.append('..')
from settings.settings import Settings
settings = Settings.load()
from nddyanmo.cuboidindexdb import CuboidIndexDB
from nddyanmo.tileindexdb import TileIndexDB
from ndbucket.cuboidbucket import CuboidBucket
from ndbucket.tilebucket import TileBucket
from ndlambda.lambdainterface import LambdaInterface

class InfraInterface(object):
  
  def __init__(self):
    pass

  def createInfrastructure(self):
    # create cuboid index table
    CuboidIndexDB.createTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    # create the tile index table
    TileIndexDB.createTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    # create the cuboid bucket
    CuboidBucket.createBucket(endpoint_url=settings.S3_ENDPOINT)
    # create the tile bucket
    TileBucket.createBucket(endpoint_url=settings.S3_ENDPOINT)
    # create lambda functions
    for func_name in settings.LAMBDA_FUNCTION_LIST:
      lambda_interface = LambdaInterface(func_name)
      lambda_interface.createFunction()


  def deleteInfrastructure(self):
    # delete cuboid index table
    CuboidIndexDB.deleteTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    # delete the tile index table
    TileIndexDB.deleteTable(endpoint_url=settings.DYNAMO_ENDPOINT)
    # delete the cuboid bucket
    CuboidBucket.deleteBucket(endpoint_url=settings.S3_ENDPOINT)
    # delete the tile bucket
    TileBucket.deleteBucket(endpoint_url=settings.S3_ENDPOINT)
    # delete lambda functions
    for func_name in settings.LAMBDA_FUNCTION_LIST:
      lambda_interface = LambdaInterface(func_name)
      lambda_interface.deleteFunction()

def main()
  
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--action', dest='action', action='store', choices=['create', 'delete'], help='Action')
  result = parser.parse_args()

  infra_interface = InfraInterface()
  if result.action == 'create':
    infra_interface.createInfrastructure()
  elif result.action == 'delete':
    infra_interface.deleteInfrastructure()
  else:
    raise ValueError("Error: Invalid value for action {}".format(result.action))

if __name__ == '__main__':
  main()
