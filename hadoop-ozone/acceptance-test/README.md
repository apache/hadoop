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

# Acceptance test suite for Ozone/Hdds

This project contains acceptance tests for ozone/hdds using docker-compose and [robot framework](http://robotframework.org/).

## Run

To run the acceptance tests, please activate the `ozone-acceptance-test` profile and do a full build.

```
mvn clean install -Pdist -Phdds
cd hadoop-ozone/acceptance-test
mvn integration-test -Phdds,ozone-acceptance-test,dist -DskipTests
```

Notes:

 1. You need a hadoop build in hadoop-dist/target directory.
 2. The `ozone-acceptance-test` could be activated with profile even if the unit tests are disabled.
 3. This method does not require the robot framework on path as jpython is used.

## Development

You can also run manually the robot tests with `robot` cli. 
 (See robotframework docs to install it: http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#installation-instructions)

In the dev-support directory we have two wrapper scripts to run robot framework with local robot cli 
instead of calling it from maven.

It's useful during the development of the robot files as any robotframework cli 
arguments could be used.

 1. `dev-support/bin/robot.sh` is the simple wrapper. The .robot file should be used as an argument.
 2. `dev-support/bin/robot-all.sh` will call the robot.sh with the main acceptance test directory, 
 which means all the acceptance tests will be executed.
