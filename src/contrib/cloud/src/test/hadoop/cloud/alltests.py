# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import unittest
from hadoop.cloud.testcluster import TestCluster
from hadoop.cloud.teststorage import TestJsonVolumeSpecManager
from hadoop.cloud.teststorage import TestJsonVolumeManager
from hadoop.cloud.testuserdata import TestInstanceUserData
from hadoop.cloud.testutil import TestUtilFunctions

def testSuite():
  alltests = unittest.TestSuite([
    unittest.makeSuite(TestCluster, 'test'),
    unittest.makeSuite(TestJsonVolumeSpecManager, 'test'),
    unittest.makeSuite(TestJsonVolumeManager, 'test'),
    unittest.makeSuite(TestInstanceUserData, 'test'),
    unittest.makeSuite(TestUtilFunctions, 'test'),
  ])
  return alltests

if __name__ == "__main__":
  runner = unittest.TextTestRunner()
  sys.exit(not runner.run(testSuite()).wasSuccessful())
