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

import boto3
from ndingest.settings.settings import Settings
settings = Settings.load()

class LambdaInterface(object):

  def __init__(self, func_name, region_name=settings.REGION_NAME, endpoint_url=''):
    self.client = boto3.client('lambda', region_name=settings.REGION_NAME, endpoint_url=endpoint_url)
    self.func_name = func_name

  def readZipFile(self, zip_file, encode_base64=True):
    return None

  def createFunction(self, zip_file, timeout=10, memory_size=128):
    """Create a lambda function"""
    try:
      response = self.client.create_function(
          FunctionName = self.func_name,
          Runtime = 'python2.7',
          Role = '',
          Handler = 'lambda_handler',
          Code = {
            'ZipFile': self.readZipFile(zip_file),
          },
          Timeout = timeout,
          MemorySize = memory_size,
          Publis = True
      )
      return response
    except Exception as e:
      print (e)
      raise
  
  def deleteFunction(self):
    """Delete a lambda function"""
    try:
      response = self.client.delete_function(
          FunctionName = self.func_name
      )
      return response
    except Exception as e:
      print (e)
      raise

  def updateFunctionCode(self, zip_file):
    """Update the code of a lambda function"""
    try:
      response = self.client.update_function_code(
          FunctionName = self.func_name,
          ZipFile = self.readZipFile(zip_file, encode_base64=False),
          Publish = True
      )
      return response
    except Exception as e:
      print (e)
      raise

  def updateFunctionConfiguration(self, timeout=10, memory_size=128):
    """Update the lambda function configuration"""
    try:
      response = self.client.update_function_configuration(
          FunctionName = self.func_name,
          Timeout = timeout,
          MemorySize = memory_size,
      )
      return response
    except Exception as e:
      print (e)
      raise

  def getFunctionConfiguration(self):
    """Fetch the function configuration for a lambda function"""
    try:
      response = client.get_function_configuration(
          FunctionName = self.func_name
      )
      return response
    except:
      print (e)
      raise
