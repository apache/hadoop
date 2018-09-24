<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Ozone Acceptance Tests

This directory contains a [robotframework](http://robotframework.org/) based test suite for Ozone to make it easier to check the current state of the package.

You can run in in any environment after [installing](https://github.com/robotframework/robotframework/blob/master/INSTALL.rst)

```
cd $DIRECTORY_OF_OZONE
robot smoketest/basic
```

The argument of the `robot` could be any robot file or directory.

The current configuration in the robot files (hostnames, ports) are adjusted for the docker-based setup but you can easily modify it for any environment.

The `./test.sh` in this directory can start multiple type of clusters (ozone standalone or ozone + hdfs) and execute the test framework with all of the clusters.
