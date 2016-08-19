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

# Extending the File System specification and its tests

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

The FileSystem specification is incomplete. It doesn't cover all operations or
even interfaces and classes in the FileSystem APIs. There may
be some minor issues with those that it does cover, such
as corner cases, failure modes, and other unexpected outcomes. It may also be that
a standard FileSystem significantly diverges from the specification, and
it is felt that this needs to be documented and coped with in tests.

Finally, the FileSystem classes and methods are not fixed forever.
They may be extended with new operations on existing classes, as well as
potentially entirely new classes and interfaces.

Accordingly, do not view this specification as a complete static document,
any more than the rest of the Hadoop code.

1. View it as a live document to accompany the reference implementation (HDFS),
and the tests used to validate filesystems.
1. Don't be afraid to extend or correct it.
1. If you are proposing enhancements to the FileSystem APIs, you should extend the
specification to match.

## How to update this specification

1. Although found in the `hadoop-common` codebase, the HDFS team has ownership of
the FileSystem and FileContext APIs. Work with them on the hdfs-dev mailing list.

1. Create JIRA issues in the `HADOOP` project, component `fs`, to cover changes
in the APIs and/or specification.

1. Code changes will of course require tests. Ideally, changes to the specification
itself are accompanied by new tests.

1. If the change involves operations that already have an `Abstract*ContractTest`,
add new test methods to the class and verify that they work on filesystem-specific
tests that subclass it. That includes the object stores as well as the local and
HDFS filesystems.

1. If the changes add a new operation, add a new abstract test class
with the same contract-driven architecture as the existing one, and an implementation
subclass for all filesystems that support the operation.

1. Add test methods to verify that invalid preconditions result in the expected
failures.

1. Add test methods to verify that valid preconditions result in the expected
final state of the filesystem. Testing as little as possible per test aids
in tracking down problems.

1. If possible, add tests to show concurrency expectations.

If a FileSystem fails a newly added test, then it may be because:

* The specification is wrong.
* The test is wrong.
* The test is looking for the wrong exception (i.e. it is too strict).
* The specification and tests are correct -and it is the filesystem is not
consistent with expectations.

HDFS has to be treated as correct in its behavior.
If the test and specification do not match this behavior, then the specification
needs to be updated. Even so, there may be cases where the FS could be changed:

1. The exception raised is a generic `IOException`, when a more informative
subclass, such as `EOFException` can be raised.
1. The FileSystem does not fail correctly when passed an invalid set of arguments.
This MAY be correctable, though must be done cautiously.

If the mismatch is in LocalFileSystem, then it probably can't be corrected, as
this is the native filesystem as accessed via the Java IO APIs.

For other FileSystems, their behaviour MAY be updated to more accurately reflect
the behavior of HDFS and/or LocalFileSystem. For most operations this is straightforward,
though the semantics of `rename()` are complicated enough that it is not clear
that HDFS is the correct reference.

If a test fails and it is felt that it is a unfixable FileSystem-specific issue, then
a new contract option to allow for different interpretations of the results should
be added to the `ContractOptions` interface, the test modified to react to the
presence/absence of the option, and the XML contract files for the standard
FileSystems updated to indicate when a feature/failure mode is present.
