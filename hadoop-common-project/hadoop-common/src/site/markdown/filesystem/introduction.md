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

# Introduction

This document defines the required behaviors of a Hadoop-compatible filesystem
for implementors and maintainers of the Hadoop filesystem, and for users of
the Hadoop FileSystem APIs

Most of the Hadoop operations are tested against HDFS in the Hadoop test
suites, initially through `MiniDFSCluster`, before release by vendor-specific
'production' tests, and implicitly by the Hadoop stack above it.

HDFS's actions have been modeled on POSIX filesystem behavior, using the actions and
return codes of Unix filesystem actions as a reference. Even so, there
are places where HDFS diverges from the expected behaviour of a POSIX
filesystem.

The behaviour of other Hadoop filesystems are not as rigorously tested.
The bundled S3 FileSystem makes Amazon's S3 Object Store ("blobstore")
accessible through the FileSystem API. The Swift FileSystem driver provides similar
functionality for the OpenStack Swift blobstore. The Azure object storage
FileSystem in branch-1-win talks to Microsoft's Azure equivalent. All of these
bind to object stores, which do have different behaviors, especially regarding
consistency guarantees, and atomicity of operations.

The "Local" FileSystem provides access to the underlying filesystem of the
platform. Its behavior is defined by the operating system and can
behave differently from HDFS. Examples of local filesystem quirks include
case-sensitivity, action when attempting to rename a file atop another file,
and whether it is possible to `seek()` past
the end of the file.

There are also filesystems implemented by third parties that assert
compatibility with Apache Hadoop. There is no formal compatibility suite, and
hence no way for anyone to declare compatibility except in the form of their
own compatibility tests.

These documents *do not* attempt to provide a normative definition of compatibility.
Passing the associated test suites *does not* guarantee correct behavior of applications.

What the test suites do define is the expected set of actions&mdash;failing these
tests will highlight potential issues.

By making each aspect of the contract tests configurable, it is possible to
declare how a filesystem diverges from parts of the standard contract.
This is information which can be conveyed to users of the filesystem.

### Naming

This document follows RFC 2119 rules regarding the use of MUST, MUST NOT, MAY,
and SHALL. MUST NOT is treated as normative.

## Implicit assumptions of the Hadoop FileSystem APIs

The original `FileSystem` class and its usages are based on an implicit set of
assumptions. Chiefly, that HDFS is
the underlying FileSystem, and that it offers a subset of the behavior of a
POSIX filesystem (or at least the implementation of the POSIX filesystem
APIs and model provided by Linux filesystems).

Irrespective of the API, it's expected that all Hadoop-compatible filesystems
present the model of a filesystem implemented in Unix:

* It's a hierarchical directory structure with files and directories.

* Files contain zero or more bytes of data.

* You cannot put files or directories under a file.

* Directories contain zero or more files.

* A directory entry has no data itself.

* You can write arbitrary binary data to a file. When the file's contents
  are read, from anywhere inside or outside of the cluster, the data is returned.

* You can store many gigabytes of data in a single file.

* The root directory, `"/"`, always exists, and cannot be renamed.

* The root directory, `"/"`, is always a directory, and cannot be overwritten by a file write operation.

* Any attempt to recursively delete the root directory will delete its contents (barring
  lack of permissions), but will not delete the root path itself.

* You cannot rename/move a directory under itself.

* You cannot rename/move a directory atop any existing file other than the
  source file itself.

* Directory listings return all the data files in the directory (i.e.
there may be hidden checksum files, but all the data files are listed).

* The attributes of a file in a directory listing (e.g. owner, length) match
 the actual attributes of a file, and are consistent with the view from an
 opened file reference.

* Security: if the caller lacks the permissions for an operation, it will fail and raise an error.

### Path Names

* A Path is comprised of Path elements separated by `"/"`.

* A path element is a unicode string of 1 or more characters.

* Path element MUST NOT include the characters `":"` or `"/"`.

* Path element SHOULD NOT include characters of ASCII/UTF-8 value 0-31 .

* Path element MUST NOT be `"."`  or `".."`

* Note also that the Azure blob store documents say that paths SHOULD NOT use
 a trailing `"."` (as their .NET URI class strips it).

 * Paths are compared based on unicode code-points.

 * Case-insensitive and locale-specific comparisons MUST NOT not be used.

### Security Assumptions

Except in the special section on security, this document assumes the client has
full access to the FileSystem. Accordingly, the majority of items in the list
do not add the qualification "assuming the user has the rights to perform the
operation with the supplied parameters and paths".

The failure modes when a user lacks security permissions are not specified.

### Networking Assumptions

This document assumes this all network operations succeed. All statements
can be assumed to be qualified as *"assuming the operation does not fail due
to a network availability problem"*

* The final state of a FileSystem after a network failure is undefined.

* The immediate consistency state of a FileSystem after a network failure is undefined.

* If a network failure can be reported to the client, the failure MUST be an
instance of `IOException` or subclass thereof.

* The exception details SHOULD include diagnostics suitable for an experienced
Java developer _or_ operations team to begin diagnostics. For example, source
and destination hostnames and ports on a ConnectionRefused exception.

* The exception details MAY include diagnostics suitable for inexperienced
developers to begin diagnostics. For example Hadoop tries to include a
reference to [ConnectionRefused](http://wiki.apache.org/hadoop/ConnectionRefused) when a TCP
connection request is refused.

<!--  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->

## Core Expectations of a Hadoop Compatible FileSystem

Here are the core expectations of a Hadoop-compatible FileSystem.
Some FileSystems do not meet all these expectations; as a result,
 some programs may not work as expected.

### Atomicity

There are some operations that MUST be atomic. This is because they are
often used to implement locking/exclusive access between processes in a cluster.

1. Creating a file. If the `overwrite` parameter is false, the check and creation
MUST be atomic.
1. Deleting a file.
1. Renaming a file.
1. Renaming a directory.
1. Creating a single directory with `mkdir()`.

* Recursive directory deletion MAY be atomic. Although HDFS offers atomic
recursive directory deletion, none of the other Hadoop FileSystems
offer such a guarantee (including local FileSystems).

Most other operations come with no requirements or guarantees of atomicity.



### Consistency

The consistency model of a Hadoop FileSystem is *one-copy-update-semantics*;
that of a traditional local POSIX filesystem. Note that even NFS relaxes
some constraints about how fast changes propagate.

* *Create.* Once the `close()` operation on an output stream writing a newly
created file has completed, in-cluster operations querying the file metadata
and contents MUST immediately see the file and its data.

* *Update.* Once the `close()`  operation on an output stream writing a newly
created file has completed, in-cluster operations querying the file metadata
and contents MUST immediately see the new data.

* *Delete.* once a `delete()` operation on a path other than "/" has completed successfully,
it MUST NOT be visible or accessible. Specifically,
`listStatus()`, `open()` ,`rename()` and `append()`
 operations MUST fail.

* *Delete then create.* When a file is deleted then a new file of the same name created, the new file
 MUST be immediately visible and its contents accessible via the FileSystem APIs.

* *Rename.* After a `rename()`  has completed, operations against the new path MUST
succeed; attempts to access the data against the old path MUST fail.

* The consistency semantics inside of the cluster MUST be the same as outside of the cluster.
All clients querying a file that is not being actively manipulated MUST see the
same metadata and data irrespective of their location.

### Concurrency

There are no guarantees of isolated access to data: if one client is interacting
with a remote file and another client changes that file, the changes may or may
not be visible.

### Operations and failures

* All operations MUST eventually complete, successfully or unsuccessfully.

* The time to complete an operation is undefined and may depend on
the implementation and on the state of the system.

* Operations MAY throw a `RuntimeException` or subclass thereof.

* Operations SHOULD raise all network, remote, and high-level problems as
an `IOException` or subclass thereof, and SHOULD NOT raise a
`RuntimeException` for such problems.

* Operations SHOULD report failures by way of raised exceptions, rather
than specific return codes of an operation.

* In the text, when an exception class is named, such as `IOException`,
the raised exception MAY be an instance or subclass of the named exception.
It MUST NOT be a superclass.

* If an operation is not implemented in a class, the implementation must
throw an `UnsupportedOperationException`.

* Implementations MAY retry failed operations until they succeed. If they do this,
they SHOULD do so in such a way that the *happens-before* relationship between
any sequence of operations meets the consistency and atomicity requirements
stated. See [HDFS-4849](https://issues.apache.org/jira/browse/HDFS-4849)
for an example of this: HDFS does not implement any retry feature that
could be observable by other callers.

### Undefined capacity limits

Here are some limits to FileSystem capacity that have never been explicitly
defined.

1. The maximum number of files in a directory.

1. Max number of directories in a directory

1. Maximum total number of entries (files and directories) in a filesystem.

1. The maximum length of a filename under a directory (HDFS: 8000).

1. `MAX_PATH` - the total length of the entire directory tree referencing a
file. Blobstores tend to stop at ~1024 characters.

1. The maximum depth of a path (HDFS: 1000 directories).

1. The maximum size of a single file.

### Undefined timeouts

Timeouts for operations are not defined at all, including:

* The maximum completion time of blocking FS operations.
MAPREDUCE-972 documents how `distcp` broke on slow s3 renames.

* The timeout for idle read streams before they are closed.

* The timeout for idle write streams before they are closed.

The blocking-operation timeout is in fact variable in HDFS, as sites and
clients may tune the retry parameters so as to convert filesystem failures and
failovers into pauses in operation. Instead there is a general assumption that
FS operations are "fast but not as fast as local FS operations", and that the latency of data
reads and writes scale with the volume of data. This
assumption by client applications reveals a more fundamental one: that the filesystem is "close"
as far as network latency and bandwidth is concerned.

There are also some implicit assumptions about the overhead of some operations.

1. `seek()` operations are fast and incur little or no network delays. [This
does not hold on blob stores]

1. Directory list operations are fast for directories with few entries.

1. Directory list operations are fast for directories with few entries, but may
incur a cost that is `O(entries)`. Hadoop 2 added iterative listing to
handle the challenge of listing directories with millions of entries without
buffering -at the cost of consistency.

1. A `close()` of an `OutputStream` is fast, irrespective of whether or not
the file operation has succeeded or not.

1. The time to delete a directory is independent of the size of the number of
child entries

### Object Stores vs. Filesystems

This specification refers to *Object Stores* in places, often using the
term *Blobstore*. Hadoop does provide FileSystem client classes for some of these
even though they violate many of the requirements. This is why, although
Hadoop can read and write data in an object store, the two which Hadoop ships
with direct support for &mdash;Amazon S3 and OpenStack Swift&mdash cannot
be used as direct replacement for HDFS.

*What is an Object Store?*

An object store is a data storage service, usually accessed over HTTP/HTTPS.
A `PUT` request uploads an object/"Blob"; a `GET` request retrieves it; ranged
`GET` operations permit portions of a blob to retrieved.
To delete the object, the HTTP `DELETE` operation is invoked.

Objects are stored by name: a string, possibly with "/" symbols in them. There
is no notion of a directory; arbitrary names can be assigned to objects &mdash;
within the limitations of the naming scheme imposed by the service's provider.

The object stores invariably provide an operation to retrieve objects with
a given prefix; a `GET` operation on the root of the service with the
appropriate query parameters.

Object stores usually prioritize availability &mdash;there is no single point
of failure equivalent to the HDFS NameNode(s). They also strive for simple
non-POSIX APIs: the HTTP verbs are the operations allowed.

Hadoop FileSystem clients for object stores attempt to make the
stores pretend that they are a FileSystem, a FileSystem with the same
features and operations as HDFS. This is &mdash;ultimately&mdash;a pretence:
they have different characteristics and occasionally the illusion fails.

1. **Consistency**. Object stores are generally *Eventually Consistent*: it
can take time for changes to objects &mdash;creation, deletion and updates&mdash;
to become visible to all callers. Indeed, there is no guarantee a change is
immediately visible to the client which just made the change. As an example,
an object `test/data1.csv` may be overwritten with a new set of data, but when
a `GET test/data1.csv` call is made shortly after the update, the original data
returned. Hadoop assumes that filesystems are consistent; that creation, updates
and deletions are immediately visible, and that the results of listing a directory
are current with respect to the files within that directory.

1. **Atomicity**. Hadoop assumes that directory `rename()` operations are atomic,
as are `delete()` operations. Object store FileSystem clients implement these
as operations on the individual objects whose names match the directory prefix.
As a result, the changes take place a file at a time, and are not atomic. If
an operation fails part way through the process, the the state of the object store
reflects the partially completed operation.  Note also that client code
assumes that these operations are `O(1)` &mdash;in an object store they are
more likely to be be `O(child-entries)`.

1. **Durability**. Hadoop assumes that `OutputStream` implementations write data
to their (persistent) storage on a `flush()` operation. Object store implementations
save all their written data to a local file, a file that is then only `PUT`
to the object store in the final `close()` operation. As a result, there is
never any partial data from incomplete or failed operations. Furthermore,
as the write process only starts in  `close()` operation, that operation may take
a time proportional to the quantity of data to upload, and inversely proportional
to the network bandwidth. It may also fail &mdash;a failure that is better
escalated than ignored.

Object stores with these characteristics, can not be used as a direct replacement
for HDFS. In terms of this specification, their implementations of the
specified operations do not match those required. They are considered supported
by the Hadoop development community, but not to the same extent as HDFS.
