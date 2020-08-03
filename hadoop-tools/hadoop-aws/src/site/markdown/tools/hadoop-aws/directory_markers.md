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

# Altering the S3A Directory Marker Behavior

## <a name="compatibility"></a> Critical: this is not backwards compatible!

This document shows how the performance of S3 IO, especially applications
writing many files (hive) or working with versioned S3 buckets can
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
* When a directory is _listed_, all files and directories in it are enumerated and returned to the caller


The S3A connector mocks this entire metaphor by grouping all objects which have
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

The S3A also has directory markers, but it just appends a / to the directory
name, so `mkdir(s3a://bucket/a/b)` would create a new marker object `a/b/` .
When a file is created under a path, the directory marker is deleted. And when a
file is deleted, if it was the last file in the directory, the marker is
recreated.

And, historically, When a path is listed, if a marker to that path is found, *it
has been interpreted as an empty directory.*

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

###  Presto: there are no directories

In the Presto S3 connectors: `mkdirs()` is a no-op. Instead, whenever you list
any path which isn't an object or a prefix of one more more objects, you get an
empty listing. That is:;  by default, every path is an empty directory.

Provided no code probes for a directory existing and fails if it is there, this
is very efficient. That's a big requirement however, -one Presto can pull off
because they know how their file uses data in S3.


###  Hadoop 3.3.1+: marker deletion is now optional


## <a name="marker-retention"></a> Controlling marker retention with `fs.s3a.directory.marker.retention` 

There is now an option `fs.s3a.directory.marker.retention` which controls how
markers are managed when new files are created

*Default* `delete`: a request is issued to delete any parental directory markers
whenever a file or directory is created.

*New* `keep`: No delete request is issued. Any directory markers which exist are
not deleted. This is *not* backwards compatible

*New* `authoritative`: directory markers are deleted _except for files created
in "authoritative" directories_.
This is backwards compatible _outside
authoritative directories_.

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
> bin/hadoop s3guard markers 
markers [-verbose] [-expected <count>] (audit || report || clean) <PATH>
    view and manipulate S3 directory markers
```

### `markers report`

Scan the path and simply report on the markers found.

### `markers audit`

Audit the path and fail if any markers were found.

### `markers clean`

The `markers clean` command will clean the directory tree of all surplus markers

```
> hadoop s3guard markers clean s3a://ireland-bucket/

2020-07-28 18:58:36,612 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://ireland-bucket/
2020-07-28 18:58:37,516 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://ireland-bucket/: duration 0:00.906s
No surplus directory markers were found under s3a://ireland-bucket/
```

The `markers clean` command _does not_ delete markers above empty directories -only those which have
files underneath. If invoked on a path, it will clean up the directory tree into a state
where it is safe for older versions of Hadoop to interact with.