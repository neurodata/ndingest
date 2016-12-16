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
import botocore

class IAM(object):
  
  def __init__(self, region_name, endpoint_url):
    self.iam = boto3.resource(
        'iam',
        region_name=region_name, endpoint_url=endpoint_url,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
    )


  def createPolicy(self, policy, name, folder=None, description=''):
    """Create an IAM policy for the bucket.

    The policy parameter defines actions allowed on the bucket.  This policy
    must be assigned to an AWS user to give access to the bucket.  Simple
    policies can be represented with a single IAM statement.  The bucket's ARN
    will be added to each statment (as the Resource the statement applies to).

    If folder is provided, each IAM statement is targeted at that folder.
    For example, if folder='/foo/bar' and the bucket is named my_tile_bucket,
    this is added to each statement dictionary:
        'Resource':'arn:aws:s3:::my_tile_bucket/foo/bar/*'


    Sample IAM statement dictionary:
      { 'Sid': 'Receive Access Statement',
        'Effect': 'Allow',
        'Action': ['s3:PutObject'] }

    Args:
      policy (list(dict)): List of IAM statements.
      name (string): Name for this policy.
      folder (optional[string]): Optionally target each statement to a specific folder.
      description (optional[string]): Description of policy.

    Returns:
      (iam.Policy)
    """

    full_policy = { 'Version': '2012-10-17', 'Statement': policy }

    arn = TileBucket.buildArn(self.bucket.name, folder)

    # Specify this bucket as the resource for each policy statement.
    for statement in policy:
      statement['Resource'] = arn

    full_policy['Id'] = name

    doc = json.dumps(full_policy)
    
    return self.iam.create_policy(
        PolicyName=full_policy['Id'],
        PolicyDocument=json.dumps(full_policy),
        Path=settings.IAM_POLICY_PATH,
        Description=description
    )


  @staticmethod
  def buildArn(bucket_name, folder=None):
      """Build an S3 ARN for use in an IAM policy.

      Example: arn:aws:s3:::my_bucket/some/folder/*

      Args:
        bucket_name (string): Name of bucket.
        folder (optional[string]): Optional folder to apply IAM policy to.

      Returns:
        (string)
      """
      arn = 'arn:aws:s3:::' + bucket_name
      if folder is not None:
          if folder[0] != '/':
              arn += '/'
          arn += folder
          if not folder.endswith('/'):
              arn += '/'
          arn += '*'
      else:
          arn += '/*'

      return arn


  def deletePolicy(self, name):
      """Deletes the queue's policy.

      Args:
          name (string): Policy name.
      """

      arn = self.getPolicyArn(name)
      if arn is None:
          raise RuntimeError('Policy {} could not be found.'.format(name))

      policy = iam.Policy(arn)
      policy.delete()


  def getPolicyArn(self, name):
      """Find the named policy and return its Arn.

      Only user created policies with a path prefix as defined in settings.ini are retrieved.
      Global AWS policies are ignored.

      Args:
          name (string): Name of policy.

      Returns:
          (string|None): None if policy cannot be found.
      """
      for policy in self.iam.policies.all():
          if policy.policy_name == name:
              return policy.arn

      return None
