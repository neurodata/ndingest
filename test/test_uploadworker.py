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
import boto3
import botocore

from ndbucket.uploadbucket import UploadBucket
from ndqueue.uploadqueue import UploadQueue

# read manifest file

# TODO validate the file

# post the manifest file


# TODO make this section multi-threaded

# fetch messages from the upload queue
upload_queue = UploadQueue()
message = upload_queue.receiveMessage()

# upload the file to the s3 bucket
upload_bucket = UploadBucket()
upload_bucket.putobject(message)
