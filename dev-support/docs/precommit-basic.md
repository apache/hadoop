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

* [Purpose](#Purpose)
* [Pre-requisites](#Pre-requisites)
* [Basic Usage](#Basic_Usage)

## Purpose

As part of Hadoop's commit process, all patches to the source base go through a precommit test that does some (usually) light checking to make sure the proposed change does not break unit tests and/or passes some other prerequisites.  This is meant as a preliminary check for committers so that the basic patch is in a known state.  This check, called test-patch, may also be used by individual developers to verify a patch prior to sending to the Hadoop QA systems.

Other projects have adopted a similar methodology after seeing great success in the Hadoop model.  Some have even gone as far as forking Hadoop's precommit code and modifying it to meet their project's needs.

This is a modification to Hadoop's version of test-patch so that we may bring together all of these forks under a common code base to help the community as a whole.


## Pre-requisites

test-patch has the following requirements:

* Ant- or Maven-based project (and ant/maven installed)
* git-based project (and git installed)
* bash v3.2 or higher
* findbugs 3.x installed
* shellcheck installed
* GNU diff
* GNU patch
* POSIX awk
* POSIX grep
* POSIX sed
* wget
* file command
* smart-apply-patch.sh

Maven plugins requirements:

* Apache RAT
* Apache FindBugs

Optional:

* Apache JIRA-based issue tracking
* JIRA cli tools

The locations of these files are (mostly) assumed to be in the file path, but may be overridden via command line options.  For Solaris and Solaris-like operating systems, the default location for the POSIX binaries is in /usr/xpg4/bin.


## Basic Usage

This command will execute basic patch testing against a patch file stored in filename:

```bash
$ cd <your repo>
$ dev-support/test-patch.sh --dirty-workspace --project=projectname <filename>
```

The `--dirty-workspace` flag tells test-patch that the repository is not clean and it is ok to continue.  This version command does not run the unit tests.

To do that, we need to provide the --run-tests command:


```bash
$ cd <your repo>
$ dev-support/test-patch.sh --dirty-workspace --run-tests <filename>
```

This is the same command, but now runs the unit tests.

A typical configuration is to have two repositories.  One with the code you are working on and another, clean repository.  This means you can:

```bash
$ cd <workrepo>
$ git diff --no-prefix trunk > /tmp/patchfile
$ cd ../<testrepo>
$ <workrepo>/dev-support/test-patch.sh --basedir=<testrepo> --resetrepo /tmp/patchfile
```

We used two new options here.  --basedir sets the location of the repository to use for testing.  --resetrepo tells test patch that it can go into **destructive** mode.  Destructive mode will wipe out any changes made to that repository, so use it with care!

After the tests have run, there is a directory that contains all of the test-patch related artifacts.  This is generally referred to as the patchprocess directory.  By default, test-patch tries to make something off of /tmp to contain this content.  Using the `--patchdir` command, one can specify exactly which directory to use.  This is helpful for automated precommit testing so that the Jenkins or other automated workflow system knows where to look to gather up the output.

## Providing Patch Files

It is a fairly common practice within the Apache community to use Apache's JIRA instance to store potential patches.  As a result, test-patch supports providing just a JIRA issue number.  test-patch will find the *last* attachment, download it, then process it.

For example:

```bash
$ test-patch.sh (other options) HADOOP-9905
```

... will process the patch file associated with this JIRA issue.


A new practice is to use a service such as GitHub and its Pull Request (PR) feature.  Luckily, test-patch supports URLs and many services like GitHub provide ways to provide unified diffs via URLs.

For example:

```bash
$ test-patch.sh (other options) https://github.com/apache/flink/pull/773.patch
```

... will grab a unified diff of PR #773 and process it.

## In Closing

test-patch has many other features and command line options for the basic user.  Many of these are self-explanatory.  To see the list of options, run test-patch.sh without any options or with --help.


