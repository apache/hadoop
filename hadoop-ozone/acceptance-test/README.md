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

# Acceptance test suite for Ozone/Hdsl

This project contains acceptance tests for ozone/hdsl using docker-compose and [robot framework](http://robotframework.org/).

## Run

To run the acceptance tests, please activate the `ozone-acceptance-test` profile and do a full build.

Typically you need a `mvn install -Phdsl,ozone-acceptance-test,dist -DskipTests` for a build without unit tests but with acceptance test.

Notes:

 1. You need a hadoop build in hadoop-dist/target directory.  
 2. The `ozone-acceptance-test` could be activated with profile even if the unit tests are disabled.

 
## Development

You can run manually the robot tests with `robot` cli. (See robotframework docs to install it.)

 1. Go to the `src/test/robotframework`
 2. Execute `robot -v basedir:${PWD}/../../.. -v VERSION:3.2.0-SNAPSHOT .`
 
You can also use select just one test with -t `"*testnamefragment*"`