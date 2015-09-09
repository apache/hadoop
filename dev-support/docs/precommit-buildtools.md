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

Build Tool Support
===================

test-patch has the ability to support multiple build tools.  Build tool plug-ins have some extra hooks to do source and object maintenance at key points. Every build tool plug-in must have one line in order to be recognized:

```bash
add_build_tool <pluginname>
```

# Global Variables

* BUILDTOOLCWD

    - If the build tool does not always run from the ${BASEDIR} and instead needs to change the current working directory to work on a specific module, then set this to true.  The default is false.

* UNSUPPORTED_TEST

    - If pluginname\_modules\_worker is given a test type that is not supported by the build system, set UNSUPPORTED_TEST=true.  If it is supported, set UNSUPPORTED_TEST=false.

For example, the gradle build tool does not have a standard way to execute checkstyle. So when checkstyle is requested, gradle\_modules\_worker sets UNSUPPORTED_TEST to true and returns out of the routine.

# Required Functions

* pluginname\_buildfile

    - This should be an echo of the file that controls the build system.  This is used for module determination.

* pluginname\_executor

    - This should be an echo of how to run the build tool, any extra arguments, etc.

* pluginname\_modules\_worker

    - Input is the branch and the test being run.  This should call modules_workers with the generic parts to run that test on the build system.  For example, if it is convention to use 'test' to trigger 'unit' tests, then module_workers should be called with 'test' appended onto its normal parameters.

* pluginname\_builtin_personality\_modules

    - Default method to determine how to enqueue modules for processing.  Note that personalities may override this function.

* pluginname\_builtin_personality\_file\_tests

    - Default method to determine which tests to trigger.  Note that personalities may override this function.

# Optional Functions

* pluginname\_postapply\_install

    - After the install step, this allows the build tool plug-in to do extra work.

* pluginname\_parse\_args

    - executed prior to any other above functions except for pluginname\_usage. This is useful for parsing the arguments passed from the user and setting up the execution environment.

* pluginname\_initialize

    - After argument parsing and prior to any other work, the initialize step allows a plug-in to do any precursor work, set internal defaults, etc.

* pluginname\_count\_(test)\_probs

    - Certain language test plug-ins require assistance from the build tool to count problems in the compile log due to some tools having custom handling for those languages.  The test plug-in name should be in the (test) part of the function name.

# Ant Specific

## Command Arguments

test-patch always passes -noinput to Ant.  This forces ant to be non-interactive.

# Gradle Specific

The gradle plug-in always rebuilds the gradlew file and uses gradlew as the method to execute commands.

# Maven Specific

## Command Arguments

test-patch always passes --batch-mode to maven to force it into non-interactive mode.  Additionally, some tests will also force -fae in order to get all of messages/errors during that mode. Some tests are executed with -DskipTests.  Additional arguments should be handled via the personality.

## Test Profile

By default, test-patch will pass -Ptest-patch to Maven. This will allow you to configure special actions that should only happen when running underneath test-patch.

## Custom Maven Tests

* Maven will trigger a maven install as part of the precompile.
* Maven will test eclipse integration as part of the postcompile.
* If src/site is modified, maven site tests are executed.
