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
* [Maven Specific](#Maven_Specific)
* [Ant Specific](#Ant_Specific)
* [Plug-ins](#Plug-ins)
* [Configuring for Other Projects](#Configuring_for_Other_Projects)
* [Important Variables](#Important_Variables)

# Docker Support

By default, test-patch runs in the same shell where it was launched.  It can alternatively use Docker to launch itself into a container.  This is particularly useful if running under a QA environment that does not provide all the necessary binaries. For example, if the patch requires a newer version of Java.

The `--docker` parameter tells test-patch to run in Docker mode. The `--dockerfile` parameter allows one to provide a custom Dockerfile. The Dockerfile should contain all of the necessary binaries and tooling needed to run the test.  However be aware that test-patch will copy this file and append its necessary hooks to re-launch itself prior to executing docker.

NOTE: If you are using Boot2Docker, you must use directories under /Users (OSX) or C:\Users (Windows) as the base and patchprocess directories (specified by the --basedir and --patch-dir options respectively), because automatically mountable directories are limited to them. See [the Docker documentation](https://docs.docker.com/userguide/dockervolumes/#mount-a-host-directory-as-a-data-volume).

Dockerfile images will be named with a test-patch prefix and suffix with either a date or a git commit hash. By using this information, test-patch will automatically manage broken/stale container images that are hanging around if it is run in --jenkins mode.  In this way, if Docker fails to build the image, the disk space should eventually be cleaned and returned back to the system.

# Maven Specific

## Command Arguments

test-patch always passes --batch-mode to maven to force it into non-interactive mode.  Additionally, some tests will also force -fae in order to get all of messages/errors during that mode.  It *does not* pass -DskipTests.  Additional arguments should be handled via the personality.

## Test Profile

By default, test-patch will pass -Ptest-patch to Maven. This will allow you to configure special actions that should only happen when running underneath test-patch.

# Ant Specific

## Command Arguments

test-patch always passes -noinput to Ant.  This force ant to be non-interactive.

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
    - executed prior to the patch being applied.  This is useful for any "before"-type data collection for later comparisons.

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

    HINT: It is recommended to make the pluginname relatively small, 10 characters at the most.  Otherwise, the ASCII output table may be skewed.


# Configuring for Other Projects

It is impossible for any general framework to be predictive about what types of special rules any given project may have, especially when it comes to ordering and Maven profiles.  In order to direct test-patch to do the correct action, a project `personality` should be added that enacts these custom rules.

A personality consists of two functions. One that determines which test types to run and another that allows a project to dictate ordering rules, flags, and profiles on a per-module, per-test run.

There can be only **one** of each personality function defined.

## Test Determination

The `personality_file_tests` function determines which tests to turn on based upon the file name.  It is relatively simple.  For example, to turn on a full suite of tests for Java files:

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

The `repostatus` parameter tells the `personality` function exactly what state the source repository is in.  It can only be in one of two states:  `branch` or `patch`.  `branch` means the patch has not been applied.  The `patch` state is after the patch has been applied.

The `testtype` state tells the personality exactly which test is about to be executed.

In order to communicate back to test-patch, there are two functions for the personality to use.

The first is `clear_personality_queue`. This removes the previous test's configuration so that a new module queue may be built. Custom personality_modules will almost always want to do this as the first action.

The second is `personality_enqueue_module`.  This function takes two parameters.  The first parameter is the name of the module to add to this test's queue.  The second parameter is an option list of additional flags to pass to Maven when processing it. `personality_enqueue_module` may be called as many times as necessary for your project.

    NOTE: A module name of . signifies the root of the repository.

For example, let's say your project uses a special configuration to skip unit tests (-DskipTests).  Running unit tests during a javadoc build isn't very useful and wastes a lot of time. We can write a simple personality check to disable the unit tests:


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

This function will tell test-patch that when the javadoc test is being run, do the documentation build at the base of the source repository and make sure the -DskipTests flag is passed to our build tool.



# Important Variables

There are a handful of extremely important variables that make life easier for personality and plug-in writers:

* BUILD\_NATIVE will be set to true if the system has requested that non-JVM-based code be built (e.g., JNI or other compiled C code). Under Jenkins, this is always true.

* BUILDTOOL specifies which tool is currently being used to drive compilation.  Additionally, many build tools define xyz\_ARGS to pass on to the build tool command line. (e.g., MAVEN\_ARGS if maven is in use).  Projects may set this in their personality.  NOTE: today, only one build tool at a time is supported.  This may change in the future.

* CHANGED\_FILES is a list of all files that appear to be added, deleted, or modified in the patch.

* CHANGED\_UNFILTERED\_MODULES is a list of all modules that house all of the CHANGED\_FILES.  Be aware that the root of the source tree is reported as '.'.

* CHANGED\_MODULES reports which modules that appear to have source code in them.

* HOW\_TO\_CONTRIBUTE should be a URL that points to a project's on-boarding documentation for new users. Currently, it is used to suggest a review of patch naming guidelines. Since this should be project specific information, it is useful to set in a project's personality.

* ISSUE\_RE is to help test-patch when talking to JIRA.  It helps determine if the given project is appropriate for the given JIRA issue.

* MODULE and other MODULE\_\* are arrays that contain which modules, the status, etc, to be operated upon. These should be treated as read-only by plug-ins.

* PATCH\_BRANCH\_DEFAULT is the name of the branch in the git repo that is considered the master.  This is useful to set in personalities.

* PATCH\_DIR is the name of the temporary directory that houses test-patch artifacts (such as logs and the patch file itself)

* TEST\_PARALLEL if parallel unit tests have been requested. Project personalities are responsible for actually enabling or ignoring the request. TEST\_THREADS is the number of threads that have been requested to run in parallel.
