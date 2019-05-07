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

# Run tests in docker environment

In the ./compose folder there are additional test scripts to make it easy to run all tests or run a specific test in a docker environment.

## Test one environment

Go to the compose directory and execute the test.sh directly from there:

```
cd compose/ozone
./test.sh
```

The results will be saved to the `compose/ozone/results`

## Run all the tests

```
cd compose
./test-all.sh
```

The results will be combined to the `compose/results` folder.

## Run one specific test case

Start the compose environment and execute test:

```
cd compose/ozone
docker-compose up -d
#wait....
../test-single.sh scm basic/basic.robot
```