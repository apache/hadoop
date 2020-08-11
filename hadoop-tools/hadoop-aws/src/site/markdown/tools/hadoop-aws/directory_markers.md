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

# Controlling the S3A Directory Marker Behavior

## <a name="compatibility"></a> Critical: this is not backwards compatible!

This document shows how the performance of S3 I/O, especially applications
creating many files (for example Apache Hive) or working with versioned S3 buckets can
increase performance by changing the S3A directory marker retention policy.

Changing the policy from the default value, `"delete"` _is not backwards compatible_.

Versions of Hadoop which are incompatible with other marker retention policies

-------------------------------------------------------
|  Branch    | Compatible Since | Future Fix Planned? |
|------------|------------------|---------------------|
| Hadoop 2.x |                  | NO                  |
| Hadoop 3.0 |                  | NO                  |
| Hadoop 3.1 |                  | Yes                 |
| Hadoop 3.2 |                  | Yes                 |
| Hadoop 3.3 |      3.3.1       | Done                |
-------------------------------------------------------

External Hadoop-based applications should also be assumed to be incompatible
unless otherwise stated/known.

It is only safe change the directory marker policy if the following
 conditions are met:

1. You know exactly which applications are writing to and reading from
   (including backing up) an S3 bucket.
2. You know all applications which read data from the bucket are as compatible.

### <a name="backups"></a> Applications backing up data.

It is not enough to have a version of Apache Hadoop which is compatible, any
application which backs up an S3 bucket or copies elsewhere must have an S3
connector which is compatible. For the Hadoop codebase, that means that if
distcp is used, it _must_ be from a compatible hadoop version.

### <a name="fallure-mode"></a> How will incompatible applications/versions fail? 

Applications using an incompatible version of the S3A connector will mistake
directories containing data for empty directories. This means that

* Listing directories/directory trees may exclude files which exist.
* Queries across the data will miss data files.
* Renaming a directory to a new location may exclude files underneath.

### <a name="recovery"></a> If an application has updated a directory tree incompatibly-- what can be done?

There's a tool on the hadoop command line, [marker tool](#marker-tool) which can audit
a bucket/path for markers, and clean up any which were found.
It can be used to make a bucket compatible with older applications.

Now that this is all clear, let's explain the problem.


## <a name="background"></a> Background: Directory Markers: what and why?

Amazon S3 is not a filesystem, it is an object store.

The S3A connector not only provides a hadoop-compatible API to interact with
data in S3, it tries to maintain the filesystem metaphor.

One key aspect of the metaphor of a file system is "directories"

#### The directory concept

In normal Unix-style filesystems, the "filesystem" is really a "directory and
file tree" in which files are always stored in "directories"


* A directory may contain 0 or more files.
* A directory may contain 0 or more directories "subdirectories"
* At the base of a filesystem is the "root directory"
* All files MUST be in a directory "the parent directory"
* All directories other than the root directory must be in another directory.
* If a directory contains no files or directories, it is "empty"
* When a directory is _listed_, all files and directories in it are enumerated
  and returned to the caller


The S3A connector emulates this metaphor by grouping all objects which have
the same prefix as if they are in the same directory tree.

If there are two objects `a/b/file1` and `a/b/file2` then S3A pretends that there is a
directory `/a/b` containing two files `file1`  and `file2`.

The directory itself does not exist.

There's a bit of a complication here.

#### What does `mkdirs()` do?

1. In HDFS and other "real" filesystems, when `mkdirs()` is invoked on a path
whose parents are all directories, then an _empty directory_ is created.

1. This directory can be probed for "it exists" and listed (an empty list is
returned)

1. Files and other directories can be created in it.


Lots of code contains a big assumption here: after you create a directory it
exists. They also assume that after files in a directory are deleted, the
directory still exists.

Given filesystem mimics directories just by aggregating objects which share a
prefix, how can you have empty directories?

The original Hadoop `s3n://` connector created a Directory Marker -any path ending
in `_$folder$` was considered to be a sign that a directory existed. A call to
`mkdir(s3n://bucket/a/b)` would create a new marker object `a/b_$folder$` .

The S3A also has directory markers, but it just appends a "/" to the directory
name, so `mkdir(s3a://bucket/a/b)` will create a new marker object `a/b/` .

When a file is created under a path, the directory marker is deleted. And when a
file is deleted, if it was the last file in the directory, the marker is
recreated.

And, historically, When a path is listed, if a marker to that path is found, *it
has been interpreted as an empty directory.*

It is that little detail which is the cause of the incompatibility issues.

## <a name="problem"></a> Scale issues related to directory markers

Creating, deleting and the listing directory markers adds overhead and can slow
down applications.

Whenever a file is created we have to delete any marker which could exist in
parent directory _or any parent paths_. Rather than do a sequence of probes for
parent markers existing, the connector issues a single request to S3 to delete
all parents. For example, if a file `/a/b/file1` is created, a multi-object
`DELETE` request containing the keys `/a/` and `/a/b/` is issued.
If no markers exists, this is harmless.

When a file is deleted, a check for the parent directory continuing to exist
(i.e. are there sibling files/directories?), and if not a marker is created.

This all works well and has worked well for many years.

However, it turns out to have some scale problems, especially from the delete
call made whenever a file is created.

1. The number of the objects listed in each request is that of the number of
parent directories: deeper trees create longer requests.

1. Every single object listed in the delete request is considered to be a write
operation.

1. In versioned S3 buckets, tombstone markers are added to the S3 indices even
if no object was deleted.

1. There's also the overhead of actually issuing the request and awaiting the
response.

Issue #2 has turned out to cause significant problems on some interactions with
large hive tables:

Because each object listed in a DELETE call is treated as one operation, and
there is (as of summer 2020) a limit of
[3500 write requests/second in a directory tree](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html),
when writing many files to a deep directory tree, it is the delete calls which
create throttling problems. 
For a symptom of this, see HADOOP-16829,
[Large DeleteObject requests are their own Thundering Herd](https://issues.apache.org/jira/browse/HADOOP-16823).

The tombstone markers have follow-on consequences -it makes listings slower.
This can have adverse effects on those large directories, again.

## <a name="solutions"></a> How to avoid marker-related problems.

###  Presto: every path is a directory

In the Presto S3 connectors: `mkdirs()` is a no-op. Instead, whenever it list
any path which isn't an object or a prefix of one more more objects, it gets an
empty listing. That is:;  by default, every path is an empty directory.

Provided no code probes for a directory existing and fails if it is there, this
is very efficient. That's a big requirement however, -one Presto can pull off
because they know how their file uses data in S3.


###  Hadoop 3.3.1+: marker deletion is now optional

From Hadoop 3.3.1 onwards, th S3A client can be configured to skip deleting
directory markers when creating files under paths. This removes all scalability
problems caused by deleting these markers -however, it is achieved at the expense
of backwards compatibility.

## <a name="marker-retention"></a> Controlling marker retention with `fs.s3a.directory.marker.retention` 

There is now an option `fs.s3a.directory.marker.retention` which controls how
markers are managed when new files are created

*Default* `delete`: a request is issued to delete any parental directory markers
whenever a file or directory is created.

*New* `keep`: No delete request is issued. Any directory markers which exist are
not deleted. This is *not* backwards compatible

*New* `authoritative`: directory markers are deleted _except for files created
in "authoritative" directories_.
This is backwards compatible _outside authoritative directories_.

Until now, the notion of an "authoritative"
directory has only been used as a performance optimization for deployments
where it is known that all Applications are using the same S3Guard metastore
when writing and reading data. In such a deployment, if it is also known that
all applications are using a compatible version of the s3a connector, then they
can switch to the higher-performance mode for those specific directories.

Only the default setting, `fs.s3a.directory.marker.retention = delete` is compatible with
existing Hadoop releases.

##  <a name="authoritative"></a>Directory Markers and S3Guard

Applications which interact with S3A in S3A clients with S3Guard enabled still
create and delete markers. There's no attempt to skip operations, such as by having
`mkdirs() `create entries in the DynamoDB table but not the store.
Having the client always update S3 ensures that other applications and clients
do (eventually) see the changes made by the "guarded" application.

When S3Guard is configured to treat some directories as  [Authoritative](s3guard.html#authoritative)
then an S3A connector with a retention policy of `fs.s3a.directory.marker.retention` of
`authoritative` will omit deleting markers in authoritative directories.

*Note* there may be further changes in directory semantics in "authoritative mode";
only use in managed applications where all clients are using the same version of
hadoop, and configured consistently.

##  <a name="marker-tool"></a> The marker tool:`hadoop s3guard markers`

The marker tool aims to help migration by scanning/auditing directory trees
for surplus markers, and for optionally deleting them.
Leaf-node markers for empty directories are not considered surplus and
will be retained.

Syntax

```
> hadoop s3guard markers -verbose -nonauth 
markers (-audit | -clean) [-expected <count>] [-out <filename>] [-limit <limit>] [-nonauth] [-verbose] <PATH>
        View and manipulate S3 directory markers

```

*Options*

| Option                  | Meaning                 |
|-------------------------|-------------------------|
|  `-audit`               | Audit the path for surplus markers |
|  `-clean`               | Clean all surplus markers under a path |
|  `-expected <count>]`   | Expected number of markers to find (primarily for testing)  |
|  `-limit <count>]`      | Limit the number of objects to scan |
|  `-nonauth`             | Only consider markers in non-authoritative paths as errors  |
|  `-out <filename>`      | Save a list of all markers found to the nominated file  |
|  `-verbose`             | Verbose output  |

*Exit Codes*

| Code  | Meaning |
|-------|---------|
| 0     | Success |
| 3     | interrupted -the value of `-limit` was reached |
| 42     | Usage   |
| 46    | Markers were found (see HTTP "406", "unacceptable") |

All other non-zero status code also indicate errors of some form or other. 

###  <a name="marker-tool-report"></a>`markers -audit`

Audit the path and fail if any markers were found. 


```
> hadoop s3guard markers -limit 8000 -audit s3a://landsat-pds/

The directory marker policy of s3a://landsat-pds is "Delete"
2020-08-05 13:42:56,079 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://landsat-pds/
Scanned 1,000 objects
Scanned 2,000 objects
Scanned 3,000 objects
Scanned 4,000 objects
Scanned 5,000 objects
Scanned 6,000 objects
Scanned 7,000 objects
Scanned 8,000 objects
Limit of scan reached - 8,000 objects
2020-08-05 13:43:01,184 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://landsat-pds/: duration 0:05.107s
No surplus directory markers were found under s3a://landsat-pds/
Listing limit reached before completing the scan
2020-08-05 13:43:01,187 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 3:
```

Here the scan reached its object limit before completing the audit; the exit code of 3, "interrupted" indicates this.


Example: a verbose audit of a bucket whose policy if authoritative -it is not an error if markers
are found under the path `/tables`.

```
> bin/hadoop s3guard markers -audit s3a://london/

  2020-08-05 18:29:16,473 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143)) - Directory markers will be kept on authoritative paths
  The directory marker policy of s3a://london is "Authoritative"
  Authoritative path list is "/tables"
  2020-08-05 18:29:19,186 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://london/
  2020-08-05 18:29:21,610 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://london/: duration 0:02.425s
  Listed 8 objects under s3a://london/
  
Found 3 surplus directory markers under s3a://london/
    s3a://london/tables
    s3a://london/tables/tables-4
    s3a://london/tables/tables-4/tables-5
Found 5 empty directory 'leaf' markers under s3a://london/
    s3a://london/tables/tables-2
    s3a://london/tables/tables-3
    s3a://london/tables/tables-4/tables-5/06
    s3a://london/tables2
    s3a://london/tables3
  These are required to indicate empty directories
  Surplus markers were found -failing audit
  2020-08-05 18:29:21,614 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 46: 
```

This fails because surplus markers were found. This S3A bucket would *NOT* be safe for older Hadoop versions
to use.

The `-nonauth` option does not treat markers under authoritative paths as errors:


```
bin/hadoop s3guard markers -nonauth -audit s3a://london/

2020-08-05 18:31:16,255 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143)) - Directory markers will be kept on authoritative paths
The directory marker policy of s3a://london is "Authoritative"
Authoritative path list is "/tables"
2020-08-05 18:31:19,210 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://london/
2020-08-05 18:31:22,240 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://london/: duration 0:03.031s
Listed 8 objects under s3a://london/

Found 3 surplus directory markers under s3a://london/
    s3a://london/tables/
    s3a://london/tables/tables-4/
    s3a://london/tables/tables-4/tables-5/
Found 5 empty directory 'leaf' markers under s3a://london/
    s3a://london/tables/tables-2/
    s3a://london/tables/tables-3/
    s3a://london/tables/tables-4/tables-5/06/
    s3a://london/tables2/
    s3a://london/tables3/
These are required to indicate empty directories

Ignoring 3 markers in authoritative paths
```

All of this S3A bucket _other_ than the authoritative path `/tables` will be safe for
incompatible Hadoop releases to to use.


###  <a name="marker-tool-clean"></a>`markers clean`

The `markers clean` command will clean the directory tree of all surplus markers.
The `-verbose` option prints more detail on the operation as well as some IO statistics

```
> hadoop s3guard markers -verbose -clean s3a://london/

2020-08-05 18:33:25,303 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143)) - Directory markers will be kept on authoritative paths
The directory marker policy of s3a://london is "Authoritative"
Authoritative path list is "/tables"
2020-08-05 18:33:28,511 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://london/
  Directory Marker tables
  Directory Marker tables/tables-2/
  Directory Marker tables/tables-3/
  Directory Marker tables/tables-4/
  Directory Marker tables/tables-4/tables-5/
  Directory Marker tables/tables-4/tables-5/06/
  Directory Marker tables2/
  Directory Marker tables3/
2020-08-05 18:33:31,685 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://london/: duration 0:03.175s
Listed 8 objects under s3a://london/

Found 3 surplus directory markers under s3a://london/
    s3a://london/tables/
    s3a://london/tables/tables-4/
    s3a://london/tables/tables-4/tables-5/
Found 5 empty directory 'leaf' markers under s3a://london/
    s3a://london/tables/tables-2/
    s3a://london/tables/tables-3/
    s3a://london/tables/tables-4/tables-5/06/
    s3a://london/tables2/
    s3a://london/tables3/
These are required to indicate empty directories

3 markers to delete in 1 page of 250 keys/page
2020-08-05 18:33:31,688 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: Deleting markers
2020-08-05 18:33:31,812 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - Deleting markers: duration 0:00.124s

Storage Statistics for s3a://london

op_get_file_status	1
object_delete_requests	1
object_list_requests	2
```

The `markers -clean` command _does not_ delete markers above empty directories -only those which have
files underneath. If invoked on a path, it will clean up the directory tree into a state
where it is safe for older versions of Hadoop to interact with.

Note that if invoked with a `-limit` value, surplus markers found during the scan will be removed,
even though the scan will be considered a failure due to the limit being reached.

## Advanced Topics


### Probing for retention via `PathCapabilities` and `StreamCapabilities`

An instance of the filesystem can be probed for its directory marker retention ability/
policy can be probed for through the `org.apache.hadoop.fs.PathCapabilities` interface,
which all FileSystem instances have implemented since Hadoop 3.2.


| Probe                   | Meaning                 |
|-------------------------|-------------------------|
| `fs.s3a.capability.directory.marker.aware`  | Does the filesystem support surplus directory markers? |
| `fs.s3a.capability.directory.marker.keep`   | Does the path retain directory markers? |
| `fs.s3a.capability.directory.marker.delete` | Does the path delete directory markers? |


The probe `fs.s3a.capability.directory.marker.aware` allows for a filesystem to be
probed to determine if its file listing policy is "aware" of directory marker retention
-that is: it can safely work with S3 buckets where markers have not been deleted.

The other two probes dynamically query the marker retention behavior of a specific path.

The S3AFileSystem also implements the `org.apache.hadoop.fs.StreamCapabilities` interface, which
can be used to probe for marker awareness via the `fs.s3a.capability.directory.marker.aware` capability.

Again, this will be true if-and-only-if the S3A connector is safe to work with S3A buckets/paths where
directories are retained.

*If an S3A instance, probed by `PathCapabilities` or `StreamCapabilities` for the capability 
`fs.s3a.capability.directory.marker.aware` and it returns false, *it is not safe to be used with
S3A paths where markers have been retained*.

This is programmatic probe -however it can be accessed on the command line via the
external [`cloudstore`](https://github.com/steveloughran/cloudstore) tool:

```
> hadoop jar cloudstore-1.0.jar pathcapability fs.s3a.capability.directory.marker.aware  s3a://london/
Probing s3a://london/ for capability fs.s3a.capability.directory.marker.aware

Using filesystem s3a://london
Path s3a://london/ has capability fs.s3a.capability.directory.marker.aware
```

If the exit code of the command is 0, then the hadoop-aws release is safe to work with buckets
where markers have not been deleted.

The same tool can be used to dynamically probe for the policy.

Take a bucket with a retention policy of "authoritative" -only paths under `/tables` will have markers retained.

```xml
  <property>
    <name>fs.s3a.bucket.london.directory.marker.retention</name>
    <value>authoritative</value>
  </property>
  <property>
    <name>fs.s3a.bucket.london.authoritative.path</name>
    <value>/tables</value>
  </property>```
```

With this policy the path capability `fs.s3a.capability.directory.marker.keep` will hold under
the path `s3a://london/tables`

```
bin/hadoop jar cloudstore-1.0.jar pathcapability fs.s3a.capability.directory.marker.keep s3a://london/tables
Probing s3a://london/tables for capability fs.s3a.capability.directory.marker.keep
2020-08-11 22:03:31,658 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143)) - Directory markers will be kept on authoritative paths
Using filesystem s3a://london
Path s3a://london/tables has capability fs.s3a.capability.directory.marker.keep
```

However it will not hold for other paths, so indicating that older Hadoop versions will be safe
to work with data written there by this S3A client.

```
bin/hadoop jar $CLOUDSTORE pathcapability fs.s3a.capability.directory.marker.keep s3a://london/tempdir
Probing s3a://london/tempdir for capability fs.s3a.capability.directory.marker.keep
2020-08-11 22:06:56,300 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143)) - Directory markers will be kept on authoritative paths
Using filesystem s3a://london
Path s3a://london/tempdir lacks capability fs.s3a.capability.directory.marker.keep
2020-08-11 22:06:56,308 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status -1: 
```
