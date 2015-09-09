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

# Some Philosophy

* Everyone's time is valuable.  The quicker contributors can get feedback and iterate, the more likely and faster their contribution will get checked in.  A committer should be able to focus on the core issues of a contribution rather than details that can be determined automatically.

* Precommit checks should be fast.  There is no value in testing parts of the source tree that are not immediately impacted by a change.  Unit testing is the target. They are not a replacement for full builds or integration tests.

* Many open source projects have a desire to have this capability.  Why not generalize a solution?

* In many build systems (especially with maven), a modular design has been picked.  Why not leverage that design to make checks faster?

* Projects that use the same language will, with a high degree of certainty, benefit from the same types of checks.

* Portability matters.  Tooling should be as operating system and language agnostic as possible.

# Phases

test-patch works effectively under several different phases:

## Initialize

This is where test-patch configures and validates the environment.  Some things done in this phase:

* Defaults
* Parameter handling
* Importing plug-ins and personalities
* Docker container launching
* Re-exec support
* Patch file downloading
* git repository management (fresh pull, branch switching, etc)

## Precheck

Checks done here are *fatal*.

This acts as a verification of all of the setup parts and is the final place to short-cut the full test cycle.  The most significant built-in check done here is verifying the patch file is a valid.

## Patch File Tests

Tests that only require the patch file are run.  Note that the repository is still from the initial checkout!

## Compile Cycle (Branch)

When compilation must be done, we follow these five steps:

* The list of modules that require analysis is built.
* A precompile step to set things up for the actual compile
* The actual compile
* A postcompile to do analysis on the output of that compile phase
* A rebuild phase to run tests that require recompiles

The first time this is done is with the pristine checkout.  This is called the "branch compile".  For this pass, this is where the 'before' work is handled.  Some things that typically get checked in this phase:

* The first pass of files and modules that will get patched
* Validation and information gathering of the source tree pre-patch
* javadoc, scaladoc, etc

## Distribution Clean

This step is to wipe the repository clean back to a pristine state such that the previous cycle will not impact the next cycle.

## Patch Application

The patch gets applied.

## Compile Cycle (Patch)

Now that the patch has been applied the steps to compile we outlined in the compilation (branch) phase are repeated but with the patch applied. This is where a lot of 'after' checks are performed.

## Unit Tests

Since unit tests are generally the slowest part of the precommit process, they are run last.  At this point, all the prerequisites to running them should be in place and ready to go.

## Reporting

Finally, the results are reported to the screen and, optionally, to JIRA and/or whatever bug system has been configured.

# Test Flow

The basic workflow for many of the sub-items in individual phases are:

1. print a header, so the end user knows that something is happening
1. verify if the test is needed.  If so, continue on.  Otherwise, return success and let the next part of the phase execute.
1. Ask the personality about what modules and what flags should get used
1. Execute maven (or some other build tool) in the given modules with the given flags. Log the output and record the time and result code.
1. Do any extra work as appropriate (diffs, counts, etc) and either accept the status and message given by the maven run or change the vote, message, log file, etc, based upon this extra work.
1. Add the outcome(s) to the report generator

As one can see, the modules list is one of the key inputs into what actually gets executed.  As a result, projects must full flexibility in either adding, modifying, or even removing modules from the test list.  If a personality removes the entire list of modules, then that test should just be ignored.

