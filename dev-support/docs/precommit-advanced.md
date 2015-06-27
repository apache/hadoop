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

test-patch
==========

* [Docker Support](#Docker_Support)
* [Maven Profiles](#Maven_Profiles)
* [Plug-ins](#Plug-ins)
* [Configuring for Other Projects](#Configuring_for_Other_Projects)

# Docker Support

By default, test-patch runs in the same shell where it was launched.  It can alternatively use Docker to launch itself into a container.  This is particularly useful if running under a QA environment that does not provide all the necessary binaries. For example, the patch requires a newer version of Java.

The `--docker` parameter tells test-patch to run in Docker mode. The `--dockerfile` parameter allows one to provide a custom Dockerfile.  Be aware that test-patch will copy this file and append its necessary hooks in order to execute.

test-patch includes code to automatically manage broken/stale container images that are hanging around if it is run in --jenkins mode.  In this way, if Docker fails to build the image, the disk space should eventually return.

# Maven Profiles

By default, test-patch will pass -Ptest-patch and -D${PROJECT_NAME}PatchProcess to Maven. This will allow you to configure special actions that should only happen when running underneath test-patch.


# Plug-ins

test-patch allows one to add to its basic feature set via plug-ins.  There is a directory called test-patch.d off of the directory where test-patch.sh lives.  Inside this directory one may place some bash shell fragments that, if setup with proper functions, will allow for test-patch to call it as necessary.


Every plugin must have one line in order to be recognized:

```bash
add_plugin <pluginname>
```

This function call registers the `pluginname` so that test-patch knows that it exists.  This plug-in name also acts as the key to the custom functions that you can define. For example:

```bash
function pluginname_filefilter
```

This function gets called for every file that a patch may contain.  This allows the plug-in author to determine if this plug-in should be called, what files it might need to analyze, etc.

Similarly, there are other functions that may be defined during the test-patch run:

* pluginname_postcheckout
    - executed prior to the patch being applied but after the git repository is setup.  This is useful for any early error checking that might need to be done before any heavier work.

* pluginname_preapply
    - executed prior to the patch being applied.  This is useful for any "before"-type data collection for later comparisons

* pluginname_postapply
    - executed after the patch has been applied.  This is useful for any "after"-type data collection.

* pluginname_postinstall
    - executed after the mvn install test has been done.  If any tests require the Maven repository to be up-to-date with the contents of the patch, this is the place.

* pluginname_tests
    - executed after the unit tests have completed.

If the plug-in has some specific options, one can use following functions:

* pluginname_usage

    - executed when the help message is displayed. This is used to display the plug-in specific options for the user.

* pluginname_parse_args

    - executed prior to any other above functions except for pluginname_usage. This is useful for parsing the arguments passed from the user and setting up the execution environment.

    HINT: It is recommend to make the pluginname relatively small, 10 characters at the most.  Otherwise the ASCII output table may be skewed.


# Configuring for Other Projects

It is impossible for any general framework to be predictive about what types of special rules any given project may have, especially when it comes to ordering and Maven profiles.  In order to assist non-Hadoop projects, a project `personality` should be added that enacts these custom rules.

A personality consists of two functions. One that determines which test types to run and another that allows a project to dictate ordering rules, flags, and profiles on a per-module, per-test run.

There can be only **one** of each personality function defined.

## Test Determination

The `personality_file_tests` function determines which tests to turn on based upon the file name.  It is realtively simple.  For example, to turn on a full suite of tests for Java files:

```bash
function personality_file_tests
{
  local filename=$1

  if [[ ${filename} =~ \.java$ ]]; then
    add_test findbugs
    add_test javac
    add_test javadoc
    add_test mvninstall
    add_test unit
  fi

}
```

The `add_test` function is used to activate the standard tests.  Additional plug-ins (such as checkstyle), will get queried on their own.

## Module & Profile Determination

Once the tests are determined, it is now time to pick which [modules](precommit-glossary.md#genericoutside-definitions) should get used.  That's the job of the `personality_modules` function.

```bash
function personality_modules
{

    clear_personality_queue

...

    personality_enqueue_module <module> <flags>

}
```

It takes exactly two parameters `repostatus` and `testtype`.

The `repostatus` parameter tells the `personality` function exactly what state the repository is in.  It can only be in one of two states:  `branch` or `patch`.  `branch` means the patch has not been applied.  The `patch` state is after the patch has been applied.

The `testtype` state tells the personality exactly which test is about to be executed.

In order to communicate back to test-patch, there are two functions for the personality to use.

The first is `clear_personality_queue`. This removes the previous test's configuration so that a new module queue may be built.

The second is `personality_enqueue_module`.  This function takes two parameters.  The first parameter is the name of the module to add to this test's queue.  The second parameter is an option list of additional flags to pass to Maven when processing it. `personality_enqueue_module` may be called as many times as necessary for your project.

    NOTE: A module name of . signifies the root of the repository.

For example, let's say your project uses a special configuration to skip unit tests (-DskipTests).  Running unit tests during a javadoc build isn't very interesting. We can write a simple personality check to disable the unit tests:


```bash
function personality_modules
{
    local repostatus=$1
    local testtype=$2

    if [[ ${testtype} == 'javadoc' ]]; then
        personality_enqueue_module . -DskipTests
        return
    fi
    ...

```

This function will tell test-patch that when the javadoc test is being run, do the documentation test at the base of the repository and make sure the -DskipTests flag is passed to Maven.

