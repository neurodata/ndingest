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

import ndproj
from ndqueue.uploadqueue import UploadQueue

class NdWorker():

  def __init__(self, token, channel, resolution):
    
    # set the channel, res
    self.channel = channel
    self.resolution = resolution
    # load the project here 
    self.proj = ndproj.NDProjectsDB().loadToken(token)


  def generateMessage(x_tile, y_tile, z_tile, time_range):
    """Generate the message to be inserted into the queue"""
    return { 'project' : self.proj.getProjectName(),
             'resolution' : self.resolution,
             'channel' : self.channel,
             'x_tile' : x_tile,
             'y_tile' : y_tile,
             'z_tile' : z_tile,
             'time_range' : time_range
           }
 

  def setupQueue():
    """Setup the queue and return the queue name"""
    queue_name = '{}_{}_{}'.format(self.proj.getProjectName(), self.channel, self.res)
    UploadQueue.createQueue(queue_name)
    self.queue = UploadQueue(queue_name)

    return queue_name


  def populateQueue(tile_size=1024, time_interval=0):
    """Populate the message queue"""
    
    # setup the queue
    queue_name = self.setupQueue()
    
    # load the image sizes
    [[ximage_size, yimage_size, zimage_size],(start_time, end_time)] = self.proj.datasetcfg.imageSize(self.resolution)
    # load the image offsets
    [x_offset, y_offset, z_offset] = proj.datasetcfg.getOffset()[self.resolution]

    # calculate the number of tiles
    # TODO KL account for xoffset and yoffset here
    num_xtiles = ximage_size / tile_size
    num_ytiles = yimage_size / tile_size

    # iterate over time
    for time in range(start_time, end_time, time_interval):
    
      # iterate over the x and y range
      for ytile in range(0, num_ytiles, 1):
        for xtile in range(0, num_xtiles, 1):
        
          # iterate over zrange
          for ztile in range(z_offset, zimage_size, 1):
            
            time_range = None if time_interval is 0 else [time, time_interval]
            # generate a message for each one
            print "inserting message:x{}y{}z{}".format(xtile, ytile, ztile)
            self.queue.sendMessage(self.generateMessage(xtile, ytile, ztile, time_range))

    return queue_name
