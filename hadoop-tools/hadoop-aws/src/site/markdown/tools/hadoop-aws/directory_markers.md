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

From Hadoop 3.3.1 onwards, the S3A client can be configured to skip deleting
directory markers when creating files under paths. This removes all scalability
problems caused by deleting these markers -however, it is achieved at the expense
of backwards compatibility.

_This Hadoop release is compatible with versions of Hadoop which
can be configured to retain directory markers above files._ 

_It does not support any options to change the marker retention
policy to anything other than the default `delete` policy._

If the S3A filesystem is configured via 
`fs.s3a.directory.marker.retention` to use a different policy
(i.e `keep` or `authoritative`),
a message will be logged at INFO and the connector will
use the "delete" policy.

The `s3guard bucket-info` tool [can be used to verify support](#bucket-info).
This allows for a command line check of compatibility, including
in scripts.

_For details on alternative marker retention policies and strategies
for safe usage, consult the documentation of a Hadoop release which
supports the ability to change the marker policy._

## <a name="bucket-info"></a> Verifying marker policy with `s3guard bucket-info`

The `bucket-info` command has been enhanced to support verification from the command
line of bucket policies via the `-marker` option


| option | verifies |
|--------|--------|
| `-markers aware` | the hadoop release is "aware" of directory markers |
| `-markers delete` | directory markers are deleted |
| `-markers keep` | directory markers are kept (will always fail) |
| `-markers authoritative` | directory markers are kept in authoritative paths (will always fail) |

All releases of Hadoop which have been updated to be marker aware will support the `-markers aware` option.


1. Updated releases which do not support switching marker retention policy will also support the
`-markers delete` option.

1. As this is such a a release, the other marker options
(`-markers keep` and  `-markers authoritative`)] will always fail.


Probing for marker awareness: `s3guard bucket-info -markers aware`  

```
> bin/hadoop s3guard bucket-info -markers aware s3a://landsat-pds/
  Filesystem s3a://landsat-pds
  Location: us-west-2
  Filesystem s3a://landsat-pds is not using S3Guard
  The "magic" committer is not supported
  
  S3A Client
    Endpoint: fs.s3a.endpoint=s3.amazonaws.com
    Encryption: fs.s3a.server-side-encryption-algorithm=none
    Input seek policy: fs.s3a.experimental.input.fadvise=normal
  
  The directory marker policy is "delete"
  
  The S3A connector can read data in S3 buckets where directory markers
  are not deleted (optional with later hadoop releases),
  and with buckets where they are.

  Available Policies: delete
```

The same command will fail on older releases, because the `-markers` option
is unknown

```
> hadoop s3guard bucket-info -markers aware s3a://landsat-pds/
Illegal option -markers
Usage: hadoop bucket-info [OPTIONS] s3a://BUCKET
    provide/check S3Guard information about a specific bucket

Common options:
  -guarded - Require S3Guard
  -unguarded - Force S3Guard to be disabled
  -auth - Require the S3Guard mode to be "authoritative"
  -nonauth - Require the S3Guard mode to be "non-authoritative"
  -magic - Require the S3 filesystem to be support the "magic" committer
  -encryption -require {none, sse-s3, sse-kms} - Require encryption policy

When possible and not overridden by more specific options, metadata
repository information will be inferred from the S3A URL (if provided)

Generic options supported are:
  -conf <config file> - specify an application configuration file
  -D <property=value> - define a value for a given property

2020-08-12 16:47:16,579 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 42:
 Illegal option -markers
````

The `-markers delete` option will verify that this release will delete directory markers.

```
> hadoop s3guard bucket-info -markers delete s3a://landsat-pds/
 Filesystem s3a://landsat-pds
 Location: us-west-2
 Filesystem s3a://landsat-pds is not using S3Guard
 The "magic" committer is not supported
 
 S3A Client
    Endpoint: fs.s3a.endpoint=s3.amazonaws.com
    Encryption: fs.s3a.server-side-encryption-algorithm=none
    Input seek policy: fs.s3a.experimental.input.fadvise=normal
 
 The directory marker policy is "delete"

```

As noted: the sole option available on this Hadoop release is `delete`. Other policy
probes will fail, returning error code 46. "unsupported"


```
> hadoop s3guard bucket-info -markers keep s3a://landsat-pds/
Filesystem s3a://landsat-pds
Location: us-west-2
Filesystem s3a://landsat-pds is not using S3Guard
The "magic" committer is not supported

S3A Client
    Endpoint: fs.s3a.endpoint=s3.amazonaws.com
    Encryption: fs.s3a.server-side-encryption-algorithm=none
    Input seek policy: fs.s3a.experimental.input.fadvise=normal

The directory marker policy is "delete"

2020-08-25 12:20:18,805 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 46:
 46: Bucket s3a://landsat-pds: required marker policy is "keep" but actual policy is "delete"
```

Even if the bucket configuration attempts to change the marker policy, probes for `keep` and `authoritative`
will fail.

Take, for example, a configuration for a specific bucket to delete markers under the authoritative path `/tables`:

```xml
<property>
  <name>fs.s3a.bucket.s3-london.directory.marker.retention</name>
  <value>authoritative</value>
</property>
<property>
  <name>fs.s3a.bucket.s3-london.authoritative.path</name>
  <value>/tables</value>
</property>
```

The marker settings will be warned about on filesystem creation, and the marker policy to remain as `delete`.
Thus a check for `-markers authoritative` will fail

```
> hadoop s3guard bucket-info -markers authoritative s3a://s3-london/
2020-08-25 12:33:52,682 [main] INFO  impl.DirectoryPolicyImpl (DirectoryPolicyImpl.java:getDirectoryPolicy(163)) -
 Directory marker policy "authoritative" is unsupported, using "delete"
Filesystem s3a://s3-london
Location: eu-west-2
Filesystem s3a://s3-london is not using S3Guard
The "magic" committer is supported

S3A Client
    Endpoint: fs.s3a.endpoint=s3.eu-west-2.amazonaws.com
    Encryption: fs.s3a.server-side-encryption-algorithm=none
    Input seek policy: fs.s3a.experimental.input.fadvise=normal

The directory marker policy is "delete"

2020-08-25 12:33:52,746 [main] INFO  util.ExitUtil (ExitUtil.java:terminate(210)) - Exiting with status 46:
 46: Bucket s3a://s3-london: required marker policy is "authoritative" but actual policy is "delete"
```




### <a name="pathcapabilities"></a> Probing for retention via `PathCapabilities` and `StreamCapabilities`

An instance of the filesystem can be probed for its directory marker retention ability/
policy can be probed for through the `org.apache.hadoop.fs.PathCapabilities` interface,
which all FileSystem classes have supported since Hadoop 3.2.


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

* [Object Versioning](https://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html). _Using versioning_

* [Optimizing Performance](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html). _Best Practices Design Patterns: Optimizing Amazon S3 Performance_
