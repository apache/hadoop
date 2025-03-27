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

# S3A Directory Marker Behavior

This document discusses directory markers and a change to the S3A
connector: surplus directory markers are no longer deleted.

This document shows how the performance of S3 I/O, especially applications
creating many files (for example Apache Hive) or working with versioned S3 buckets can
increase performance by changing the S3A directory marker retention policy.

This release always retains markers
and _is potentially not backwards compatible_ with hadoop versions
released before 2021.

The compatibility table of older releases is as follows:

| Branch     | Compatible Since | Support                               | Released |
|------------|------------------|---------------------------------------|----------|
| Hadoop 2.x | 2.10.2           | Partial                               | 05/2022  |
| Hadoop 3.0 | n/a              | WONTFIX                               |          |
| Hadoop 3.1 | n/a              | WONTFIX                               |          |
| Hadoop 3.2 | 3.2.2            | Partial                               | 01/2022  |
| Hadoop 3.3 | 3.3.1            | Full: deletion is enabled             | 01/2021  |
| Hadoop 3.4 | 3.4.0            | Full: deletion is disabled            | 03/2024  |
| Hadoop 3.5 | 3.5.0            | Full: markers will always be retained |          |

*Full*

These releases are full marker-aware and will (possibly only optionally)
not delete them on file/dir creation.

*Partial*

These branches have partial compatibility, and are safe to use with hadoop versions
that do not delete markers.

* They can list directories with surplus directory markers, and correctly identify when
  such directories have child entries.
* They can open files under directories with such markers.
* They will always delete parent directory markers when creating their own files and directories.

All these branches are no longer supported by Apache for bugs and security fixes
-users should upgrade to more recent versions for those reasons alone.

*WONTFIX*

These stores may misinterpret a surplus directory marker for an empty directory.
This does not happen on listing, but it may be misinterpreted on rename operations.

The compatibility patch "HADOOP-17199. S3A Directory Marker HADOOP-13230 backport"
is present in both the Hadoop 3.0.x and 3.1.x source branches, for anyone wishing to make a private
release.
However, there will be no further Apache releases of the obsolete branches.

Everyone using these branches should upgrade to a supported version.

## History

### Hadoop 3.3.1 Directory marker retention is optional

[HADOOP-13230](https://issues.apache.org/jira/browse/HADOOP-13230)
 _S3A to optionally retain directory markers_

### Hadoop 3.4.0: markers are not deleted by default

[HADOOP-18752](https://issues.apache.org/jira/browse/HADOOP-18752)
_Change fs.s3a.directory.marker.retention to "keep"_ changed the default
policy.

Marker deletion can still be enabled.

Since this release there have been no reports of incompatibilities
surfacing "in the wild". That is: out of date hadoop versions are not
being used to work into the same parts of S3 buckets as modern releases.

### Hadoop 3.5: markers are never deleted

[HADOOP-19278](https://issues.apache.org/jira/browse/HADOOP-19278)
_S3A: remove option to delete directory markers_

Surplus directory markers are neither checked for nor deleted.

Removing the option to delete markers simplifies the code and significantly improves testing:
* There is no need to parameterize many tests based on the marker policy.
* Tests which make assertions about the number of http requests which take places no longer have to
  contain separate assertions for the keeping/deleting options.

Notes
* During the directory tree copy which takes place in a rename, surplus directory
markers are not copied. They are, after all, surplus.
* The `hadoop s3guard markers` command (see below) can purge directory markers from a bucket or path.

## <a name="background"></a> Background: Directory Markers: what and why?

Amazon S3 is not a Posix-like filesystem, it is an object store.

The S3A connector not only provides a hadoop-compatible API to interact with
data in S3, it tries to maintain the filesystem metaphor for applications
written to expect it.

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

In older versions of Hadoop, when a file was created under a path,
the directory marker is deleted. And when a file is deleted,
if it was the last file in the directory, the marker is
recreated.

This release does not delete directory markers.

And, historically, when a path is listed, if a marker to that path is found, *it
has been interpreted as an empty directory.*

It is that little detail which is the cause of the incompatibility issues.

## <a name="problem"></a> The Problem with Directory Markers

Creating, deleting and the listing directory markers adds overhead and can slow
down applications.

Whenever a file is created the S3A client had to delete any marker which could exist in the
parent directory _or any parent paths_. Rather than do a sequence of probes for
parent markers existing, the connector issued a single request to S3 to delete
all parents. For example, if a file `/a/b/file1` is created, a multi-object
`DELETE` request containing the keys `/a/` and `/a/b/` was issued.
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

## <a name="bucket-info"></a> Verifying marker policy with `s3guard bucket-info`

Although it is now moot, the `bucket-info` command has been enhanced to support verification from the command
line of bucket policies via the `-marker` option

| option                   | verifies                                                         | result |
|--------------------------|------------------------------------------------------------------|--------|
| `-markers aware`         | The hadoop release is "aware" of directory markers. Always true  | 0      |
| `-markers keep`          | Directory markers are kept. Always true                          | 0      |
| `-markers delete`        | Directory markers are deleted. Always false                      | 1      |
| `-markers authoritative` | Directory markers are kept in authoritative paths.  Always false | 1      |

All releases of Hadoop which have been updated to be marker aware will support the `-markers aware` option.


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
> hadoop s3guard markers -limit 8000 -audit s3a://noaa-isd-pds/

The directory marker policy of s3a://noaa-isd-pds is "Keep"
2020-08-05 13:42:56,079 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://noaa-isd-pds/
Scanned 1,000 objects
Scanned 2,000 objects
Scanned 3,000 objects
Scanned 4,000 objects
Scanned 5,000 objects
Scanned 6,000 objects
Scanned 7,000 objects
Scanned 8,000 objects
Limit of scan reached - 8,000 objects
2020-08-05 13:43:01,184 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://noaa-isd-pds/: duration 0:05.107s
No surplus directory markers were found under s3a://noaa-isd-pds/
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


###  <a name="marker-tool-clean"></a>`markers clean`

The `markers clean` command will clean the directory tree of all surplus markers.
The `-verbose` option prints more detail on the operation as well as some IO statistics

```
bin/hadoop s3guard markers -clean -verbose s3a://london/
The directory marker policy of s3a://london is "Keep"
2023-06-06 17:15:52,110 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: marker scan s3a://london/
  Directory Marker user/stevel/target/test/data/4so7pZebRx/
  Directory Marker user/stevel/target/test/data/OKvfC3oxlD/
  Directory Marker user/stevel/target/test/data/VSTQ1O4dMi/

Listing statistics:
  counters=((object_continue_list_request=0) (object_list_request.failures=0) (object_list_request=1) (object_continue_list_request.failures=0));
gauges=();
minimums=((object_list_request.min=540) (object_continue_list_request.min=-1) (object_continue_list_request.failures.min=-1) (object_list_request.failures.min=-1));
maximums=((object_continue_list_request.failures.max=-1) (object_list_request.failures.max=-1) (object_list_request.max=540) (object_continue_list_request.max=-1));
means=((object_list_request.mean=(samples=1, sum=540, mean=540.0000)) (object_continue_list_request.failures.mean=(samples=0, sum=0, mean=0.0000)) (object_list_request.failures.mean=(samples=0, sum=0, mean=0.0000)) (object_continue_list_request.mean=(samples=0, sum=0, mean=0.0000)));


2023-06-06 17:15:52,662 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - marker scan s3a://london/: duration 0:00.553s
Listed 3 objects under s3a://london/

No surplus directory markers were found under s3a://london/
Found 3 empty directory 'leaf' markers under s3a://london/
    s3a://london/user/stevel/target/test/data/4so7pZebRx/
    s3a://london/user/stevel/target/test/data/OKvfC3oxlD/
    s3a://london/user/stevel/target/test/data/VSTQ1O4dMi/
These are required to indicate empty directories

0 markers to delete in 0 pages of 250 keys/page
2023-06-06 17:15:52,664 [main] INFO  tools.MarkerTool (DurationInfo.java:<init>(77)) - Starting: Deleting markers
2023-06-06 17:15:52,664 [main] INFO  tools.MarkerTool (DurationInfo.java:close(98)) - Deleting markers: duration 0:00.000s

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

| Probe                                                     | Meaning                                                                  | Current value |
|-----------------------------------------------------------|--------------------------------------------------------------------------|---------------|
| `fs.s3a.capability.directory.marker.aware`                | Does the filesystem support surplus directory markers?                   | true          |
| `fs.s3a.capability.directory.marker.policy.delete`        | Is the bucket policy "delete"?                                           | false         |
| `fs.s3a.capability.directory.marker.policy.keep`          | Is the bucket policy "keep"?                                             | true          |
| `fs.s3a.capability.directory.marker.policy.authoritative` | Is the bucket policy "authoritative"?                                    | false         |
| `fs.s3a.capability.directory.marker.action.delete`        | If a file was created at this path, would directory markers be deleted?  | false         |
| `fs.s3a.capability.directory.marker.action.keep`          | If a file was created at this path, would directory markers be retained? | true          |

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
