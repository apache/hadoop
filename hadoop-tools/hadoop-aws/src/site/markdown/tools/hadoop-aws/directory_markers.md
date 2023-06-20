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

This document discusses a performance feature of the S3A
connector: directory markers are not deleted unless the
client is explicitly configured to do so.

## <a name="compatibility"></a> Critical: this is not backwards compatible!

This document shows how the performance of S3 I/O, especially applications
creating many files (for example Apache Hive) or working with versioned S3 buckets can
increase performance by changing the S3A directory marker retention policy.

The default policy in this release of hadoop is "keep",
which _is not backwards compatible_ with hadoop versions
released before 2021.

The compatibility table of older releases is as follows:

| Branch     | Compatible Since | Supported | Released |
|------------|------------------|-----------|----------|
| Hadoop 2.x | 2.10.2           | Read-only | 05/2022  |
| Hadoop 3.0 | n/a              | WONTFIX   |          |
| Hadoop 3.1 | n/a              | WONTFIX   |          |
| Hadoop 3.2 | 3.2.2            | Read-only | 01/2022  |
| Hadoop 3.3 | 3.3.1            | Done      | 01/2021  |


*WONTFIX*

The Hadoop 3.0 and 3.1 lines will have no further releases, so will
not be upgraded.
The compatibility patch "HADOOP-17199. S3A Directory Marker HADOOP-13230 backport"
is present in both source code branches, for anyone wishing to make a private
release.

*Read-only*

These branches have read-only compatibility.

* They may list directories with directory markers, and correctly identify when
  such directories have child entries.
* They will open files under directories with such markers.

## How to re-enable backwards compatibility

The option `fs.s3a.directory.marker.retention` can be changed to "delete" to re-enable
the original policy.

```xml
  <property>
    <name>fs.s3a.directory.marker.retention</name>
    <value>delete</value>
  </property>
```
## Verifying read compatibility.

The `s3guard bucket-info` tool [can be used to verify support](#bucket-info).
This allows for a command line check of compatibility, including
in scripts.

External Hadoop-based applications should also be assumed to be incompatible
unless otherwise stated/known.

It is only safe change the directory marker policy if the following
 conditions are met:

1. You know exactly which applications are writing to and reading from
   (including backing up) an S3 bucket.
2. You know all applications which read data from the bucket are compatible.


### <a name="backups"></a> Applications backing up data.

It is not enough to have a version of Apache Hadoop which is compatible, any
application which backs up an S3 bucket or copies elsewhere must have an S3
connector which is compatible. For the Hadoop codebase, that means that if
distcp is used, it _must_ be from a compatible hadoop version.

### <a name="fallure-mode"></a> How will incompatible applications/versions fail?

Applications using an incompatible version of the S3A connector will mistake
directories containing data for empty directories. This means that:

* Listing directories/directory trees may exclude files which exist.
* Queries across the data will miss data files.
* Renaming a directory to a new location may exclude files underneath.

The failures are silent: there is no error message, stack trace or
other warning that files may have been missed. They simply aren't
found.

### <a name="recovery"></a> If an application has updated a directory tree incompatibly-- what can be done?

There's a tool on the hadoop command line, [marker tool](#marker-tool) which can audit
a bucket/path for markers, and clean up any markers which were found.
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


* A directory may contain zero or more files.
* A directory may contain zero or more directories "subdirectories"
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

Given the S3A connector mimics directories just by aggregating objects which share a
prefix, how can you have empty directories?

The original Hadoop `s3n://` connector created a Directory Marker -any path ending
in `_$folder$` was considered to be a sign that a directory existed. A call to
`mkdir(s3n://bucket/a/b)` would create a new marker object `a/b_$folder$` .

The S3A also has directory markers, but it just appends a "/" to the directory
name, so `mkdir(s3a://bucket/a/b)` will create a new marker object `a/b/` .

When a file is created under a path, the directory marker is deleted. And when a
file is deleted, if it was the last file in the directory, the marker is
recreated.

And, historically, when a path is listed, if a marker to that path is found, *it
has been interpreted as an empty directory.*

It is that little detail which is the cause of the incompatibility issues.

## <a name="problem"></a> The Problem with Directory Markers

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

2. Every single object listed in the delete request is considered to be a write
operation.

3. In versioned S3 buckets, tombstone markers are added to the S3 indices even
if no object was deleted.

4. There's also the overhead of actually issuing the request and awaiting the
response.

Issue #2 has turned out to cause significant problems on some interactions with
large hive tables:

Because each object listed in a DELETE call is treated as one operation, and
there is a limit of 3500 write requests/second in a directory
tree.
When writing many files to a deep directory tree, it is the delete calls which
create throttling problems.

The tombstone markers have follow-on consequences -it makes listings against
S3 versioned buckets slower.
This can have adverse effects on those large directories, again.

## <a name="solutions"></a> Strategies to avoid marker-related problems.

###  Presto: every path is a directory

In the Presto [S3 connector](https://prestodb.io/docs/current/connector/hive.html#amazon-s3-configuration),
`mkdirs()` is a no-op.
Whenever it lists any path which isn't an object or a prefix of one more objects, it returns an
empty listing. That is:;  by default, every path is an empty directory.

Provided no code probes for a directory existing and fails if it is there, this
is very efficient. That's a big requirement however, -one Presto can pull off
because they know how their file uses data in S3.


###  Hadoop 3.3.1+: marker deletion is now optional

From Hadoop 3.3.1 onwards, the S3A client can be configured to skip deleting
directory markers when creating files under paths. This removes all scalability
problems caused by deleting these markers -however, it is achieved at the expense
of backwards compatibility.

## <a name="marker-retention"></a> Controlling marker retention with `fs.s3a.directory.marker.retention`

There is now an option `fs.s3a.directory.marker.retention` which controls how
markers are managed when new files are created

1. `delete`: a request is issued to delete any parental directory markers
whenever a file or directory is created.
2. `keep`: No delete request is issued.
Any directory markers which exist are not deleted.
This is *not* backwards compatible
3. `authoritative`: directory markers are deleted _except for files created
in "authoritative" directories_. This is backwards compatible _outside authoritative directories_.

The setting, `fs.s3a.directory.marker.retention = delete` is compatible with
every shipping Hadoop release; that of `keep` compatible with
all releases since 2021.

##  <a name="s3guard"></a> Directory Markers and Authoritative paths


The now-deleted S3Guard feature included the concept of "authoritative paths";
paths where all clients were required to be using S3Guard and sharing the
same metadata store.
In such a setup, listing authoritative paths would skip all queries of the S3
store -potentially being much faster.

In production, authoritative paths were usually only ever for Hive managed
tables, where access was strictly restricted to the Hive services.


When the S3A client is configured to treat some directories as "Authoritative"
then an S3A connector with a retention policy of `fs.s3a.directory.marker.retention` of
`authoritative` will omit deleting markers in authoritative directories.

```xml
<property>
  <name>fs.s3a.bucket.hive.authoritative.path</name>
  <value>/tables</value>
</property>
```
This an option to consider if not 100% confident that all
applications interacting with a store are using an S3A client
which is marker aware.

## <a name="bucket-info"></a> Verifying marker policy with `s3guard bucket-info`

The `bucket-info` command has been enhanced to support verification from the command
line of bucket policies via the `-marker` option


| option                   | verifies                                              |
|--------------------------|-------------------------------------------------------|
| `-markers aware`         | the hadoop release is "aware" of directory markers    |
| `-markers delete`        | directory markers are deleted                         |
| `-markers keep`          | directory markers are kept (not backwards compatible) |
| `-markers authoritative` | directory markers are kept in authoritative paths     |

All releases of Hadoop which have been updated to be marker aware will support the `-markers aware` option.


1. Updated releases which do not support switching marker retention policy will also support the
`-markers delete` option.


Example: `s3guard bucket-info -markers aware` on a compatible release.

```
> hadoop s3guard bucket-info -markers aware s3a://landsat-pds/
Filesystem s3a://landsat-pds
Location: us-west-2

...

Directory Markers
        The directory marker policy is "keep"
        Available Policies: delete, keep, authoritative
        Authoritative paths: fs.s3a.authoritative.path=
        The S3A connector is compatible with buckets where directory markers are not deleted

```

The same command will fail on older releases, because the `-markers` option
is unknown

```
> hadoop s3guard bucket-info -markers aware s3a://landsat-pds/
Illegal option -markers
Usage: hadoop bucket-info [OPTIONS] s3a://BUCKET
    provide/check information about a specific bucket

Common options:
  -magic - Require the S3 filesystem to be support the "magic" committer
  -encryption -require {none, sse-s3, sse-kms} - Require encryption policy

When possible and not overridden by more specific options, metadata
repository information will be inferred from the S3A URL (if provided)

Generic options supported are:
  -conf <config file> - specify an application configuration file
  -D <property=value> - define a value for a given property

2020-08-12 16:47:16,579 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 42: Illegal option -markers
````

A specific policy check verifies that the connector is configured as desired

```
> hadoop s3guard bucket-info -markers keep s3a://landsat-pds/
Filesystem s3a://landsat-pds
Location: us-west-2

...

Directory Markers
        The directory marker policy is "keep"
        Available Policies: delete, keep, authoritative
        Authoritative paths: fs.s3a.authoritative.path=

```

When probing for a specific policy, the error code "46" is returned if the active policy
does not match that requested:

```
> hadoop s3guard bucket-info -markers delete s3a://landsat-pds/
Filesystem s3a://landsat-pds
Location: us-west-2

S3A Client
        Signing Algorithm: fs.s3a.signing-algorithm=(unset)
        Endpoint: fs.s3a.endpoint=s3.amazonaws.com
        Encryption: fs.s3a.encryption.algorithm=none
        Input seek policy: fs.s3a.experimental.input.fadvise=normal
        Change Detection Source: fs.s3a.change.detection.source=etag
        Change Detection Mode: fs.s3a.change.detection.mode=server

S3A Committers
        The "magic" committer is supported in the filesystem
        S3A Committer factory class: mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
        S3A Committer name: fs.s3a.committer.name=magic
        Store magic committer integration: fs.s3a.committer.magic.enabled=true

Security
        Delegation token support is disabled

Directory Markers
        The directory marker policy is "delete"
        Available Policies: delete, keep, authoritative
        Authoritative paths: fs.s3a.authoritative.path=

2021-11-22 16:03:59,175 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210))
 -Exiting with status 46: 46: Bucket s3a://landsat-pds: required marker polic is
  "keep" but actual policy is "delete"

```


##  <a name="marker-tool"></a> The marker tool: `hadoop s3guard markers`

The marker tool aims to help migration by scanning/auditing directory trees
for surplus markers, and for optionally deleting them.
Leaf-node markers for empty directories are not considered surplus and
will be retained.

Syntax

```
> hadoop s3guard markers -verbose -nonauth
markers (-audit | -clean) [-min <count>] [-max <count>] [-out <filename>] [-limit <limit>] [-nonauth] [-verbose] <PATH>
        View and manipulate S3 directory markers

```

*Options*

| Option            | Meaning                                                    |
|-------------------|------------------------------------------------------------|
| `-audit`          | Audit the path for surplus markers                         |
| `-clean`          | Clean all surplus markers under a path                     |
| `-min <count>`    | Minimum number of markers an audit must find (default: 0)  |
| `-max <count>]`   | Minimum number of markers an audit must find (default: 0)  |
| `-limit <count>]` | Limit the number of objects to scan                        |
| `-nonauth`        | Only consider markers in non-authoritative paths as errors |
| `-out <filename>` | Save a list of all markers found to the nominated file     |
| `-verbose`        | Verbose output                                             |

*Exit Codes*

| Code | Meaning                                             |
|------|-----------------------------------------------------|
| 0    | Success                                             |
| 3    | interrupted -the value of `-limit` was reached      |
| 42   | Usage                                               |
| 46   | Markers were found (see HTTP "406", "unacceptable") |

All other non-zero status code also indicate errors of some form or other.

###  <a name="marker-tool-report"></a>`markers -audit`

Audit the path and fail if any markers were found.


```
> hadoop s3guard markers -limit 8000 -audit s3a://landsat-pds/

The directory marker policy of s3a://landsat-pds is "Keep"
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
incompatible Hadoop releases to use.


###  <a name="marker-tool-clean"></a>`markers clean`

The `markers clean` command will clean the directory tree of all surplus markers.
The `-verbose` option prints more detail on the operation as well as some IO statistics

```
bin/hadoop s3guard markers -clean -verbose s3a://stevel-london/
The directory marker policy of s3a://stevel-london is "Keep"
2023-06-06 17:15:52,110 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://stevel-london/
  Directory Marker user/stevel/target/test/data/4so7pZebRx/
  Directory Marker user/stevel/target/test/data/OKvfC3oxlD/
  Directory Marker user/stevel/target/test/data/VSTQ1O4dMi/

Listing statistics:
  counters=((object_continue_list_request=0) (object_list_request.failures=0) (object_list_request=1) (object_continue_list_request.failures=0));
gauges=();
minimums=((object_list_request.min=540) (object_continue_list_request.min=-1) (object_continue_list_request.failures.min=-1) (object_list_request.failures.min=-1));
maximums=((object_continue_list_request.failures.max=-1) (object_list_request.failures.max=-1) (object_list_request.max=540) (object_continue_list_request.max=-1));
means=((object_list_request.mean=(samples=1, sum=540, mean=540.0000)) (object_continue_list_request.failures.mean=(samples=0, sum=0, mean=0.0000)) (object_list_request.failures.mean=(samples=0, sum=0, mean=0.0000)) (object_continue_list_request.mean=(samples=0, sum=0, mean=0.0000)));


2023-06-06 17:15:52,662 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://stevel-london/: duration 0:00.553s
Listed 3 objects under s3a://stevel-london/

No surplus directory markers were found under s3a://stevel-london/
Found 3 empty directory 'leaf' markers under s3a://stevel-london/
    s3a://stevel-london/user/stevel/target/test/data/4so7pZebRx/
    s3a://stevel-london/user/stevel/target/test/data/OKvfC3oxlD/
    s3a://stevel-london/user/stevel/target/test/data/VSTQ1O4dMi/
These are required to indicate empty directories

0 markers to delete in 0 pages of 250 keys/page
2023-06-06 17:15:52,664 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: Deleting markers
2023-06-06 17:15:52,664 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - Deleting markers: duration 0:00.000s

IO Statistics for s3a://stevel-london

counters=((audit_request_execution=1)
(audit_span_creation=3)
(object_list_request=1)
(op_get_file_status=1)
(store_io_request=1));

gauges=();

minimums=((object_list_request.min=540)
(op_get_file_status.min=2));

maximums=((object_list_request.max=540)
(op_get_file_status.max=2));

means=((object_list_request.mean=(samples=1, sum=540, mean=540.0000))
(op_get_file_status.mean=(samples=1, sum=2, mean=2.0000)));
```

The `markers -clean` command _does not_ delete markers above empty directories -only those which have
files underneath. If invoked on a path, it will clean up the directory tree into a state
where it is safe for older versions of Hadoop to interact with.

Note that if invoked with a `-limit` value, surplus markers found during the scan will be removed,
even though the scan will be considered a failure due to the limit being reached.

## <a name="advanced-topics"></a> Advanced Topics


### <a name="pathcapabilities"></a> Probing for retention via `PathCapabilities` and `StreamCapabilities`

An instance of the filesystem can be probed for its directory marker retention ability/
policy can be probed for through the `org.apache.hadoop.fs.PathCapabilities` interface,
which all FileSystem classes have supported since Hadoop 3.3.


| Probe                   | Meaning                 |
|-------------------------|-------------------------|
| `fs.s3a.capability.directory.marker.aware`  | Does the filesystem support surplus directory markers? |
| `fs.s3a.capability.directory.marker.policy.delete` | Is the bucket policy "delete"? |
| `fs.s3a.capability.directory.marker.policy.keep`   | Is the bucket policy "keep"? |
| `fs.s3a.capability.directory.marker.policy.authoritative` | Is the bucket policy "authoritative"? |
| `fs.s3a.capability.directory.marker.action.delete` | If a file was created at this path, would directory markers be deleted? |
| `fs.s3a.capability.directory.marker.action.keep`   | If a file was created at this path, would directory markers be retained? |


The probe `fs.s3a.capability.directory.marker.aware` allows for a filesystem to be
probed to determine if its file listing policy is "aware" of directory marker retention
-that is: can this s3a client safely work with S3 buckets where markers have not been deleted.

The `fs.s3a.capability.directory.marker.policy.` probes return the active policy for the bucket.

The two `fs.s3a.capability.directory.marker.action.` probes dynamically query the marker
retention behavior of a specific path.
That is: if a file was created at that location, would ancestor directory markers
be kept or deleted?

The `S3AFileSystem` class also implements the `org.apache.hadoop.fs.StreamCapabilities` interface, which
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

If the exit code of the command is `0`, then the S3A is safe to work with buckets
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
  </property>
```

With this policy the path capability `fs.s3a.capability.directory.marker.action.keep` will hold under
the path `s3a://london/tables`

```
bin/hadoop jar cloudstore-1.0.jar pathcapability fs.s3a.capability.directory.marker.action.keep s3a://london/tables
Probing s3a://london/tables for capability fs.s3a.capability.directory.marker.action.keep
2020-08-11 22:03:31,658 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143))
 - Directory markers will be kept on authoritative paths
Using filesystem s3a://london
Path s3a://london/tables has capability fs.s3a.capability.directory.marker.action.keep
```

However it will not hold for other paths, so indicating that older Hadoop versions will be safe
to work with data written there by this S3A client.

```
bin/hadoop jar cloudstore-1.0.jar pathcapability fs.s3a.capability.directory.marker.action.keep s3a://london/tempdir
Probing s3a://london/tempdir for capability fs.s3a.capability.directory.marker.action.keep
2020-08-11 22:06:56,300 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(143))
 - Directory markers will be kept on authoritative paths
Using filesystem s3a://london
Path s3a://london/tempdir lacks capability fs.s3a.capability.directory.marker.action.keep
2020-08-11 22:06:56,308 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status -1:
```


## <a name="glossary"></a> Glossary

#### Directory Marker

An object in an S3 bucket with a trailing "/", used to indicate that there is a directory at that location.
These are necessary to maintain expectations about directories in an object store:

1. After `mkdirs(path)`, `exists(path)` holds.
1. After `rm(path/*)`, `exists(path)` holds.

In previous releases of Hadoop, the marker created by a `mkdirs()` operation was deleted after a file was created.
Rather than make a slow HEAD probe + optional marker DELETE of every parent path element, HADOOP-13164 switched
to enumerating all parent paths and issuing a single bulk DELETE request.
This is faster under light load, but
as each row in the delete consumes one write operation on the allocated IOPs of that bucket partition, creates
load issues when many worker threads/processes are writing to files.
This problem is bad on Apache Hive as:
* The hive partition structure places all files within the same S3 partition.
* As they are deep structures, there are many parent entries to include in the bulk delete calls.
* It's creating a lot temporary files, and still uses rename to commit output.

Apache Spark has less of an issue when an S3A committer is used -although the partition structure
is the same, the delayed manifestation of output files reduces load.

#### Leaf Marker

A directory marker which has not files or directory marker objects underneath.
It genuinely represents an empty directory.

#### Surplus Marker

A directory marker which is above one or more files, and so is superfluous.
These are the markers which were traditionally deleted; now it is optional.

Older versions of Hadoop mistake such surplus markers as Leaf Markers.

#### Versioned Bucket

An S3 Bucket which has Object Versioning enabled.

This provides a backup and recovery mechanism for data within the same
bucket: older objects can be listed and restored through the AWS S3 console
and some applications.

## References

<!-- if extending, keep JIRAs separate, have them in numerical order; the rest in lexical.` -->

* [HADOOP-13164](https://issues.apache.org/jira/browse/HADOOP-13164). _Optimize S3AFileSystem::deleteUnnecessaryFakeDirectories._

* [HADOOP-13230](https://issues.apache.org/jira/browse/HADOOP-13230). _S3A to optionally retain directory markers_

* [HADOOP-16090](https://issues.apache.org/jira/browse/HADOOP-16090). _S3A Client to add explicit support for versioned stores._

* [HADOOP-16823](https://issues.apache.org/jira/browse/HADOOP-16823). _Large DeleteObject requests are their own Thundering Herd_

* [HADOOP-17199](https://issues.apache.org/jira/browse/HADOOP-17199).
  Backport HADOOP-13230 list/getFileStatus changes for preserved directory markers
* [Object Versioning](https://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html). _Using versioning_

* [Optimizing Performance](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html). _Best Practices Design Patterns: Optimizing Amazon S3 Performance_
