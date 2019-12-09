<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

HDFS Snapshots
==============

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->


Overview
--------

HDFS Snapshots are read-only point-in-time copies of the file system.
Snapshots can be taken on a subtree of the file system or the entire file system.
Some common use cases of snapshots are data backup, protection against user errors
and disaster recovery.

The implementation of HDFS Snapshots is efficient:


* Snapshot creation is instantaneous:
  the cost is *O(1)* excluding the inode lookup time.

* Additional memory is used only when modifications are made relative to a snapshot:
  memory usage is *O(M)*,
  where *M* is the number of modified files/directories.

* Blocks in datanodes are not copied:
  the snapshot files record the block list and the file size.
  There is no data copying.

* Snapshots do not adversely affect regular HDFS operations:
  modifications are recorded in reverse chronological order
  so that the current data can be accessed directly.
  The snapshot data is computed by subtracting the modifications
  from the current data.


### Snapshottable Directories

Snapshots can be taken on any directory once the directory has been set as
*snapshottable*.
A snapshottable directory is able to accommodate 65,536 simultaneous snapshots.
There is no limit on the number of snapshottable directories.
Administrators may set any directory to be snapshottable.
If there are snapshots in a snapshottable directory,
the directory can be neither deleted nor renamed
before all the snapshots are deleted.

Nested snapshottable directories are currently not allowed.
In other words, a directory cannot be set to snapshottable
if one of its ancestors/descendants is a snapshottable directory.


### Snapshot Paths

For a snapshottable directory,
the path component *".snapshot"* is used for accessing its snapshots.
Suppose `/foo` is a snapshottable directory,
`/foo/bar` is a file/directory in `/foo`,
and `/foo` has a snapshot `s0`.
Then, the path `/foo/.snapshot/s0/bar`
refers to the snapshot copy of `/foo/bar`.
The usual API and CLI can work with the ".snapshot" paths.
The following are some examples.

* Listing all the snapshots under a snapshottable directory:

        hdfs dfs -ls /foo/.snapshot

* Listing the files in snapshot `s0`:

        hdfs dfs -ls /foo/.snapshot/s0

* Copying a file from snapshot `s0`:

        hdfs dfs -cp -ptopax /foo/.snapshot/s0/bar /tmp

    Note that this example uses the preserve option to preserve
    timestamps, ownership, permission, ACLs and XAttrs.


Upgrading to a version of HDFS with snapshots
---------------------------------------------

The HDFS snapshot feature introduces a new reserved path name used to
interact with snapshots: `.snapshot`. When upgrading from an
older version of HDFS which does not support snapshots, existing paths named `.snapshot` need
to first be renamed or deleted to avoid conflicting with the reserved path.
See the upgrade section in
[the HDFS user guide](HdfsUserGuide.html#Upgrade_and_Rollback)
for more information.


Snapshot Operations
-------------------


### Administrator Operations

The operations described in this section require superuser privilege.


#### Allow Snapshots


Allowing snapshots of a directory to be created.
If the operation completes successfully, the directory becomes snapshottable.

* Command:

        hdfs dfsadmin -allowSnapshot <path>

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |

See also the corresponding Java API
`void allowSnapshot(Path path)` in `HdfsAdmin`.


#### Disallow Snapshots

Disallowing snapshots of a directory to be created.
All snapshots of the directory must be deleted before disallowing snapshots.

* Command:

        hdfs dfsadmin -disallowSnapshot <path>

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |

See also the corresponding Java API
`void disallowSnapshot(Path path)` in `HdfsAdmin`.


### User Operations

The section describes user operations.
Note that HDFS superuser can perform all the operations
without satisfying the permission requirement in the individual operations.


#### Create Snapshots

Create a snapshot of a snapshottable directory.
This operation requires owner privilege of the snapshottable directory.

* Command:

        hdfs dfs -createSnapshot <path> [<snapshotName>]

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |
    | snapshotName | The snapshot name, which is an optional argument. When it is omitted, a default name is generated using a timestamp with the format `"'s'yyyyMMdd-HHmmss.SSS"`, e.g. `"s20130412-151029.033"`. |

See also the corresponding Java API
`Path createSnapshot(Path path)` and
`Path createSnapshot(Path path, String snapshotName)`
in [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html)
The snapshot path is returned in these methods.


#### Delete Snapshots

Delete a snapshot of from a snapshottable directory.
This operation requires owner privilege of the snapshottable directory.

* Command:

        hdfs dfs -deleteSnapshot <path> <snapshotName>

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |
    | snapshotName | The snapshot name. |

See also the corresponding Java API
`void deleteSnapshot(Path path, String snapshotName)`
in [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).


#### Rename Snapshots

Rename a snapshot.
This operation requires owner privilege of the snapshottable directory.

* Command:

        hdfs dfs -renameSnapshot <path> <oldName> <newName>

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |
    | oldName | The old snapshot name. |
    | newName | The new snapshot name. |

See also the corresponding Java API
`void renameSnapshot(Path path, String oldName, String newName)`
in [FileSystem](../../api/org/apache/hadoop/fs/FileSystem.html).


#### Get Snapshottable Directory Listing

Get all the snapshottable directories where the current user has permission to take snapshtos.

* Command:

        hdfs lsSnapshottableDir

* Arguments: none

See also the corresponding Java API
`SnapshottableDirectoryStatus[] getSnapshottableDirectoryListing()`
in `DistributedFileSystem`.


#### Get Snapshots Difference Report

Get the differences between two snapshots.
This operation requires read access privilege for all files/directories in both snapshots.

* Command:

        hdfs snapshotDiff <path> <fromSnapshot> <toSnapshot>

* Arguments:

    | --- | --- |
    | path | The path of the snapshottable directory. |
    | fromSnapshot | The name of the starting snapshot. |
    | toSnapshot | The name of the ending snapshot. |

    Note that snapshotDiff can be used to get the difference report between two snapshots, or between
    a snapshot and the current status of a directory. Users can use "." to represent the current status.

* Results:

    | --- | --- |
    | \+  | The file/directory has been created. |
    | \-  | The file/directory has been deleted. |
    | M   | The file/directory has been modified. |
    | R   | The file/directory has been renamed. |

A *RENAME* entry indicates a file/directory has been renamed but
is still under the same snapshottable directory. A file/directory is
reported as deleted if it was renamed to outside of the snapshottble directory.
A file/directory renamed from outside of the snapshottble directory is
reported as newly created.

The snapshot difference report does not guarantee the same operation sequence.
For example, if we rename the directory *"/foo"* to *"/foo2"*, and
then append new data to the file *"/foo2/bar"*, the difference report will
be:

    R. /foo -> /foo2
    M. /foo/bar

I.e., the changes on the files/directories under a renamed directory is
reported using the original path before the rename (*"/foo/bar"* in
the above example).

See also the corresponding Java API
`SnapshotDiffReport getSnapshotDiffReport(Path path, String fromSnapshot, String toSnapshot)`
in `DistributedFileSystem`.
