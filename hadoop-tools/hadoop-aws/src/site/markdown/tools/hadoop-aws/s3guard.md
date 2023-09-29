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

# S3Guard: Consistency and Metadata Caching for S3A (retired)

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

*S3Guard* was a feature for the S3A client of the S3 object store,
which used a consistent database as the store of metadata about objects
in an S3 bucket.

It was written been 2016 and 2020, *when Amazon S3 was eventually consistent.*
It compensated for the following S3 inconsistencies: 
* Newly created objects excluded from directory listings.
* Newly deleted objects retained in directory listings.
* Deleted objects still visible in existence probes and opening for reading.
* S3 Load balancer 404 caching when a probe is made for an object before its creation.

It did not compensate for update inconsistency, though by storing the etag
values of objects in the database, it could detect and report problems.

Now that S3 is consistent, _there is no need for S3Guard at all._
Accordingly, it was removed from the source in 2022 in [HADOOP-17409](https://issues.apache.org/jira/browse/HADOOP-17409), _Remove S3Guard_.

Attempting to create an S3A connector instance with S3Guard set to anything but the
null or local metastores will now fail.


### S3Guard History

S3Guard

1. Permitted a consistent view of the object store.

1. Could improve performance on directory listing/scanning operations.
including those which take place during the partitioning period of query
execution, the process where files are listed and the work divided up amongst
processes.

The basic idea was that, for each operation in the Hadoop S3 client (s3a) that
reads or modifies metadata, a shadow copy of that metadata is stored in a
separate MetadataStore implementation. The store was 
1. Updated after mutating operations on the store
1. Updated after list operations against S3 discovered changes
1. Looked up whenever a probe was made for a file/directory existing.
1. Queried for all objects under a path when a directory listing was made; the results were
   merged with the S3 listing in a non-authoritative path, used exclusively in
   authoritative mode.
 

For links to early design documents and related patches, see
[HADOOP-13345](https://issues.apache.org/jira/browse/HADOOP-13345).


## Moving off S3Guard

How to move off S3Guard

1. Unset the option `fs.s3a.metadatastore.impl` globally/for all buckets for which it
   was selected.
1. Restart all applications.

Once you are confident that all applications have been restarted, _Delete the DynamoDB table_.
This is to avoid paying for a database you no longer need.
This can be done from the AWS GUI.

## Removing S3Guard Configurations

The `fs.s3a.metadatastore.impl` option must be one of
* unset
* set to the empty string ""
* set to the "Null" Metadata store `org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore`.

To aid the migration of external components which used the Local store for a consistent
view within the test process, the Local Metadata store option is also recognized:
`org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore`.
When this option is used the S3A connector will warn and continue.


```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value></value>
</property>
```

```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value>org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore</value>
</property>
```

## Issue: Increased number/cost of S3 IO calls.

More AWS S3 calls may be made once S3Guard is disabled, both for LIST and HEAD operations.

While this may seem to increase cost, as the DDB table is no longer needed, users will
save on DDB table storage and use costs.

Some deployments of Apache Hive declared their managed tables to be "authoritative".
The S3 store was no longer checked when listing directories or for updates to
entries. The S3Guard table in DynamoDB was used exclusively.

Without S3Guard, listing performance may be slower. However, Hadoop 3.3.0+ has significantly
improved listing performance ([HADOOP-17400](https://issues.apache.org/jira/browse/HADOOP-17400)
_Optimize S3A for maximum performance in directory listings_) so this should not be apparent.

The S3A [auditing](auditing.html) feature adds information to the S3 server logs
about which jobs, users and filesystem operations have been making S3 requests.
This auditing information can be used to identify opportunities to reduce load.


## S3Guard Command Line Interface (CLI)


### Display information about a bucket, `s3guard bucket-info`

Prints and optionally checks the status of a bucket.

```bash
hadoop s3guard bucket-info [-guarded] [-unguarded] [-auth] [-nonauth] [-magic] [-encryption ENCRYPTION] [-markers MARKER] s3a://BUCKET
```

Options

| argument | meaning |
|-----------|-------------|
| `-guarded` | Require S3Guard to be enabled. This will now always fail |
| `-unguarded` | Require S3Guard to be disabled. This will now always succeed |
| `-auth` | Require the S3Guard mode to be "authoritative". This will now always fail |
| `-nonauth` | Require the S3Guard mode to be "non-authoritative". This will now always fail |
| `-magic` | Require the S3 filesystem to be support the "magic" committer |
| `-markers` | Directory marker status: `aware`, `keep`, `delete`, `authoritative` |
| `-encryption <type>` | Require a specific encryption algorithm  |

The server side encryption options are not directly related to S3Guard, but
it is often convenient to check them at the same time.

Example

```bash
> hadoop s3guard bucket-info -magic -markers keep s3a://test-london/

Filesystem s3a://test-london
Location: eu-west-2

S3A Client
        Signing Algorithm: fs.s3a.signing-algorithm=(unset)
        Endpoint: fs.s3a.endpoint=(unset)
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
        The directory marker policy is "keep"
        Available Policies: delete, keep, authoritative
        Authoritative paths: fs.s3a.authoritative.path=

```


### List or Delete Leftover Multipart Uploads: `s3guard uploads`

Lists or deletes all pending (uncompleted) multipart uploads older than
given age.

```bash
hadoop s3guard uploads (-list | -abort | -expect <num-uploads>) [-verbose] \
    [-days <days>] [-hours <hours>] [-minutes <minutes>] [-seconds <seconds>] \
    [-force] s3a://bucket/prefix
```

The command lists or deletes all multipart uploads which are older than
the given age, and that match the prefix supplied, if any.

For example, to delete all uncompleted multipart uploads older than two
days in the folder at `s3a://my-bucket/path/to/stuff`, use the following
command:

```bash
hadoop s3guard uploads -abort -days 2 s3a://my-bucket/path/to/stuff
```

We recommend running with `-list` first to confirm the parts shown
are those that you wish to delete. Note that the command will prompt
you with a "Are you sure?" prompt unless you specify the `-force`
option. This is to safeguard against accidental deletion of data, which
is especially risky without a long age parameter as it can affect
in-fight uploads.

The `-expect` option is similar to `-list`, except it is silent by
default, and terminates with a success or failure exit code depending
on whether or not the supplied number matches the number of uploads
found that match the given options (path, age).

