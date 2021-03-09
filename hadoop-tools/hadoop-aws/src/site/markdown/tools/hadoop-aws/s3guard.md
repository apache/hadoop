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

# S3Guard: Consistency and Metadata Caching for S3A

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

*S3Guard* is a feature for the S3A client of the S3 object store,
which can use a (consistent) database as the store of metadata about objects
in an S3 bucket.

It was written been 2016 and 2020, *when Amazon S3 was eventually consistent.*
It compensated for the following S3 inconsistencies: 
* Newly created objects excluded from directory listings.
* Newly deleted objects retained in directory listings.
* Deleted objects still visible in existence probes and opening for reading.
* S3 Load balancer 404 caching when a probe is made for an object before its creation.

It did not compensate for update inconsistency, though by storing the etag
values of objects in the database, it could detect and report problems.

Now that S3 is consistent, there is no need for S3Guard at all.

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

*Important*

* While all underlying data is persisted in S3, if, for some reason,
the S3Guard-cached metadata becomes inconsistent with that in S3,
queries on the data may become incorrect.
For example, new datasets may be omitted, objects may be overwritten,
or clients may not be aware that some data has been deleted.
It is essential for all clients writing to an S3Guard-enabled
S3 Repository to use the feature. Clients reading the data may work directly
with the S3A data, in which case the normal S3 consistency guarantees apply.

## Moving off S3Guard

How to move off S3Guard, given it is no longer needed.

1. Unset the option `fs.s3a.metadatastore.impl` globally/for all buckets for which it
   was selected.
1. If the option `org.apache.hadoop.fs.s3a.s3guard.disabled.warn.level` has been changed from
the default (`SILENT`), change it back. You no longer need to be warned that S3Guard is disabled.
1. Restart all applications.

Once you are confident that all applications have been restarted, _Delete the DynamoDB table_.
This is to avoid paying for a database you no longer need.
This is best done from the AWS GUI.

## Setting up S3Guard

### S3A to warn or fail if S3Guard is disabled
A seemingly recurrent problem with S3Guard is that people think S3Guard is
turned on but it isn't.
You can set `org.apache.hadoop.fs.s3a.s3guard.disabled.warn.level`
to avoid this. The property sets what to do when an S3A FS is instantiated
without S3Guard. The following values are available:

* `SILENT`: Do nothing.
* `INFORM`: Log at info level that FS is instantiated without S3Guard.
* `WARN`: Warn that data may be at risk in workflows.
* `FAIL`: S3AFileSystem instantiation will fail.

The default setting is `SILENT`. The setting is case insensitive.
The required level can be set in the `core-site.xml`.

---
The latest configuration parameters are defined in `core-default.xml`.  You
should consult that file for full information, but a summary is provided here.


### 1. Choose the Database

A core concept of S3Guard is that the directory listing data of the object
store, *the metadata* is replicated in a higher-performance, consistent,
database. In S3Guard, this database is called *The Metadata Store*

By default, S3Guard is not enabled.

The Metadata Store to use in production is bonded to Amazon's DynamoDB
database service.  The following setting will enable this Metadata Store:

```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value>org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore</value>
</property>
```

Note that the `NullMetadataStore` store can be explicitly requested if desired.
This offers no metadata storage, and effectively disables S3Guard.

```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value>org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore</value>
</property>
```

### 2. Configure S3Guard Settings

More settings will may be added in the future.
Currently the only Metadata Store-independent setting, besides the
implementation class above, are the *allow authoritative* and *fail-on-error*
flags.

#### <a name="authoritative"></a>  Authoritative S3Guard

Authoritative S3Guard is a complicated configuration which delivers performance
at the expense of being unsafe for other applications to use the same directory
tree/bucket unless configured consistently.

It can also be used to support [directory marker retention](directory_markers.html)
in higher-performance but non-backwards-compatible modes.

Most deployments do not use this setting -it is ony used in deployments where
specific parts of a bucket (e.g. Apache Hive managed tables) are known to
have exclusive access by a single application (Hive) and other tools/applications
from exactly the same Hadoop release.

The _authoritative_ expression in S3Guard is present in two different layers, for
two different reasons:

* Authoritative S3Guard
    * S3Guard can be set as authoritative, which means that an S3A client will
    avoid round-trips to S3 when **getting file metadata**, and **getting
    directory listings** if there is a fully cached version of the directory
    stored in metadata store.
    * This mode can be set as a configuration property
    `fs.s3a.metadatastore.authoritative`
    * It can also be set only on specific directories by setting
    `fs.s3a.authoritative.path` to one or more prefixes, for example
    `s3a://bucket/path` or "/auth1,/auth2".
    * All interactions with the S3 bucket(s) must be through S3A clients sharing
    the same metadata store.
    * This is independent from which metadata store implementation is used.
    * In authoritative mode the metadata TTL metadata expiry is not effective.
    This means that the metadata entries won't expire on authoritative paths.

* Authoritative directory listings (isAuthoritative bit)
    * Tells if the stored directory listing metadata is complete.
    * This is set by the FileSystem client (e.g. s3a) via the `DirListingMetadata`
    class (`org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata`).
    (The MetadataStore only knows what the FS client tells it.)
    * If set to `TRUE`, we know that the directory listing
    (`DirListingMetadata`) is full, and complete.
    * If set to `FALSE` the listing may not be complete.
    * Metadata store may persist the isAuthoritative bit on the metadata store.
    * Currently `org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore` and
    `org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore` implementation
    supports authoritative bit.

More on Authoritative S3Guard:

* This setting is about treating the MetadataStore (e.g. dynamodb) as the source
 of truth in general, and also to short-circuit S3 list objects and serve
 listings from the MetadataStore in some circumstances.
* For S3A to skip S3's get object metadata, and serve it directly from the
MetadataStore, the following things must all be true:
    1. The S3A client is configured to allow MetadataStore to be authoritative
    source of a file metadata (`fs.s3a.metadatastore.authoritative=true`).
    1. The MetadataStore has the file metadata for the path stored in it.
* For S3A to skip S3's list objects on some path, and serve it directly from
the MetadataStore, the following things must all be true:
    1. The MetadataStore implementation persists the bit
    `DirListingMetadata.isAuthorititative` set when calling
    `MetadataStore#put` (`DirListingMetadata`)
    1. The S3A client is configured to allow MetadataStore to be authoritative
    source of a directory listing (`fs.s3a.metadatastore.authoritative=true`).
    1. The MetadataStore has a **full listing for path** stored in it.  This only
    happens if the FS client (s3a) explicitly has stored a full directory
    listing with `DirListingMetadata.isAuthorititative=true` before the said
    listing request happens.

This configuration only enables authoritative mode in the client layer. It is
recommended that you leave the default setting here:

```xml
<property>
    <name>fs.s3a.metadatastore.authoritative</name>
    <value>false</value>
</property>
```

Note that a MetadataStore MAY persist this bit in the directory listings. (Not
MUST).

Note that if this is set to true, it may exacerbate or persist existing race
conditions around multiple concurrent modifications and listings of a given
directory tree.

In particular: **If the Metadata Store is declared as authoritative,
all interactions with the S3 bucket(s) must be through S3A clients sharing
the same Metadata Store**

#### TTL metadata expiry

It can be configured how long an entry is valid in the MetadataStore
**if the authoritative mode is turned off**, or the path is not
configured to be authoritative.
If `((lastUpdated + ttl) <= now)` is false for an entry, the entry will
be expired, so the S3 bucket will be queried for fresh metadata.
The time for expiry of metadata can be set as the following:

```xml
<property>
    <name>fs.s3a.metadatastore.metadata.ttl</name>
    <value>15m</value>
</property>
```

#### Fail on Error

By default, S3AFileSystem write operations will fail when updates to
S3Guard metadata fail. S3AFileSystem first writes the file to S3 and then
updates the metadata in S3Guard. If the metadata write fails,
`MetadataPersistenceException` is thrown.  The file in S3 **is not** rolled
back.

If the write operation cannot be programmatically retried, the S3Guard metadata
for the given file can be corrected with a command like the following:

```bash
hadoop s3guard import [-meta URI] s3a://my-bucket/file-with-bad-metadata
```

Programmatic retries of the original operation would require overwrite=true.
Suppose the original operation was `FileSystem.create(myFile, overwrite=false)`.
If this operation failed with `MetadataPersistenceException` a repeat of the
same operation would result in `FileAlreadyExistsException` since the original
operation successfully created the file in S3 and only failed in writing the
metadata to S3Guard.

Metadata update failures can be downgraded to ERROR logging instead of exception
by setting the following configuration:

```xml
<property>
    <name>fs.s3a.metadatastore.fail.on.write.error</name>
    <value>false</value>
</property>
```

Setting this false is dangerous as it could result in the type of issue S3Guard
is designed to avoid. For example, a reader may see an inconsistent listing
after a recent write since S3Guard may not contain metadata about the recently
written file due to a metadata write error.

As with the default setting, the new/updated file is still in S3 and **is not**
rolled back. The S3Guard metadata is likely to be out of sync.

### 3. Configure the Metadata Store.

Here are the `DynamoDBMetadataStore` settings.  Other Metadata Store
implementations will have their own configuration parameters.


### 4. Name Your Table

First, choose the name of the table you wish to use for the S3Guard metadata
storage in your DynamoDB instance.  If you leave it unset/empty, a
separate table will be created for each S3 bucket you access, and that
bucket's name will be used for the name of the DynamoDB table.  For example,
this sets the table name to `my-ddb-table-name`

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table</name>
  <value>my-ddb-table-name</value>
  <description>
    The DynamoDB table name to operate. Without this property, the respective
    S3 bucket names will be used.
  </description>
</property>
```

It is good to share a table across multiple buckets for multiple reasons,
especially if you are *not* using on-demand DynamoDB tables, and instead
prepaying for provisioned I/O capacity.

1. You are billed for the provisioned I/O capacity allocated to the table,
*even when the table is not used*. Sharing capacity can reduce costs.

1. You can share the "provision burden" across the buckets. That is, rather
than allocating for the peak load on a single bucket, you can allocate for
the peak load *across all the buckets*, which is likely to be significantly
lower.

1. It's easier to measure and tune the load requirements and cost of
S3Guard, because there is only one table to review and configure in the
AWS management console.

1. When you don't grant the permission to create DynamoDB tables to users.
A single pre-created table for all buckets avoids the needs for an administrator
to create one for every bucket.

When wouldn't you want to share a table?

1. When you are using on-demand DynamoDB and want to keep each table isolated.
1. When you do explicitly want to provision I/O capacity to a specific bucket
and table, isolated from others.

1. When you are using separate billing for specific buckets allocated
to specific projects.

1. When different users/roles have different access rights to different buckets.
As S3Guard requires all users to have R/W access to the table, all users will
be able to list the metadata in all buckets, even those to which they lack
read access.

### 5. Locate your Table

You may also wish to specify the region to use for DynamoDB.  If a region
is not configured, S3A will assume that it is in the same region as the S3
bucket. A list of regions for the DynamoDB service can be found in
[Amazon's documentation](http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region).
In this example, to use the US West 2 region:

```xml
<property>
  <name>fs.s3a.s3guard.ddb.region</name>
  <value>us-west-2</value>
</property>
```

When working with S3Guard-managed buckets from EC2 VMs running in AWS
infrastructure, using a local DynamoDB region ensures the lowest latency
and highest reliability, as well as avoiding all long-haul network charges.
The S3Guard tables, and indeed, the S3 buckets, should all be in the same
region as the VMs.

### 6. Optional: Create your Table

Next, you can choose whether or not the table will be automatically created
(if it doesn't already exist).  If you want this feature, set the
`fs.s3a.s3guard.ddb.table.create` option to `true`.

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table.create</name>
  <value>true</value>
  <description>
    If true, the S3A client will create the table if it does not already exist.
  </description>
</property>
```

### 7. If creating a table: Choose your billing mode (and perhaps I/O Capacity)

Next, you need to decide whether to use On-Demand DynamoDB and its
pay-per-request billing (recommended), or to explicitly request a
provisioned IO capacity.

Before AWS offered pay-per-request billing, the sole billing mechanism,
was "provisioned capacity". This mechanism requires you to choose 
the DynamoDB read and write throughput requirements you
expect to need for your expected uses of the S3Guard table.
Setting higher values cost you more money -*even when the table was idle*
  *Note* that these settings only affect table creation when
`fs.s3a.s3guard.ddb.table.create` is enabled.  To change the throughput for
an existing table, use the AWS console or CLI tool.

For more details on DynamoDB capacity units, see the AWS page on [Capacity
Unit Calculations](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#CapacityUnitCalculations).

Provisioned IO capacity is billed per hour for the life of the table, *even when the
table and the underlying S3 buckets are not being used*.

There are also charges incurred for data storage and for data I/O outside of the
region of the DynamoDB instance. S3Guard only stores metadata in DynamoDB: path names
and summary details of objects —the actual data is stored in S3, so billed at S3
rates.

With provisioned I/O capacity, attempting to perform more I/O than the capacity
requested throttles the operation and may result in operations failing.
Larger I/O capacities cost more.

With the introduction of On-Demand DynamoDB, you can now avoid paying for
provisioned capacity by creating an on-demand table.
With an on-demand table you are not throttled if your DynamoDB requests exceed
any pre-provisioned limit, nor do you pay per hour even when a table is idle.

You do, however, pay more per DynamoDB operation.
Even so, the ability to cope with sudden bursts of read or write requests, combined
with the elimination of charges for idle tables, suit the use patterns made of 
S3Guard tables by applications interacting with S3. That is: periods when the table
is rarely used, with intermittent high-load operations when directory trees
are scanned (query planning and similar), or updated (rename and delete operations).


We recommending using On-Demand DynamoDB for maximum performance in operations
such as query planning, and lowest cost when S3 buckets are not being accessed.

This is the default, as configured in the default configuration options.

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table.capacity.read</name>
  <value>0</value>
  <description>
    Provisioned throughput requirements for read operations in terms of capacity
    units for the DynamoDB table. This config value will only be used when
    creating a new DynamoDB table.
    If set to 0 (the default), new tables are created with "per-request" capacity.
    If a positive integer is provided for this and the write capacity, then
    a table with "provisioned capacity" will be created.
    You can change the capacity of an existing provisioned-capacity table
    through the "s3guard set-capacity" command.
  </description>
</property>

<property>
  <name>fs.s3a.s3guard.ddb.table.capacity.write</name>
  <value>0</value>
  <description>
    Provisioned throughput requirements for write operations in terms of
    capacity units for the DynamoDB table.
    If set to 0 (the default), new tables are created with "per-request" capacity.
    Refer to related configuration option fs.s3a.s3guard.ddb.table.capacity.read
  </description>
</property>
```

### 8.  If creating a table: Enable server side encryption (SSE)

Encryption at rest can help you protect sensitive data in your DynamoDB table.
When creating a new table, you can set server side encryption on the table
using the default AWS owned customer master key (CMK), AWS managed CMK, or
customer managed CMK. S3Guard code accessing the table is all the same whether
SSE is enabled or not. For more details on DynamoDB table server side
encryption, see the AWS page on [Encryption at Rest: How It Works](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/encryption.howitworks.html).

These are the default configuration options, as configured in `core-default.xml`.

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table.sse.enabled</name>
  <value>false</value>
  <description>
    Whether server-side encryption (SSE) is enabled or disabled on the table.
    By default it's disabled, meaning SSE is set to AWS owned CMK.
  </description>
</property>

<property>
  <name>fs.s3a.s3guard.ddb.table.sse.cmk</name>
  <value/>
  <description>
    The KMS Customer Master Key (CMK) used for the KMS encryption on the table.
    To specify a CMK, this config value can be its key ID, Amazon Resource Name
    (ARN), alias name, or alias ARN. Users only need to provide this config if
    the key is different from the default DynamoDB KMS Master Key, which is
    alias/aws/dynamodb.
  </description>
</property>
```

## Authenticating with S3Guard

The DynamoDB metadata store takes advantage of the fact that the DynamoDB
service uses the same authentication mechanisms as S3. S3Guard
gets all its credentials from the S3A client that is using it.

All existing S3 authentication mechanisms can be used.

## Per-bucket S3Guard configuration

In production, it is likely only some buckets will have S3Guard enabled;
those which are read-only may have disabled, for example. Equally importantly,
buckets in different regions should have different tables, each
in the relevant region.

These options can be managed through S3A's [per-bucket configuration
mechanism](./index.html#Configuring_different_S3_buckets).
All options with the under `fs.s3a.bucket.BUCKETNAME.KEY` are propagated
to the options `fs.s3a.KEY` *for that bucket only*.

As an example, here is a configuration to use different metadata stores
and tables for different buckets

First, we define shortcuts for the metadata store classnames:


```xml
<property>
  <name>s3guard.null</name>
  <value>org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore</value>
</property>

<property>
  <name>s3guard.dynamo</name>
  <value>org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore</value>
</property>
```

Next, Amazon's public landsat database is configured with no
metadata store:

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.metadatastore.impl</name>
  <value>${s3guard.null}</value>
  <description>The read-only landsat-pds repository isn't
  managed by S3Guard</description>
</property>
```

Next the `ireland-2` and `ireland-offline` buckets are configured with
DynamoDB as the store, and a shared table `production-table`:


```xml
<property>
  <name>fs.s3a.bucket.ireland-2.metadatastore.impl</name>
  <value>${s3guard.dynamo}</value>
</property>

<property>
  <name>fs.s3a.bucket.ireland-offline.metadatastore.impl</name>
  <value>${s3guard.dynamo}</value>
</property>

<property>
  <name>fs.s3a.bucket.ireland-2.s3guard.ddb.table</name>
  <value>production-table</value>
</property>
```

The region of this table is automatically set to be that of the buckets,
here `eu-west-1`; the same table name may actually be used in different
regions.

Together then, this configuration enables the DynamoDB Metadata Store
for two buckets with a shared table, while disabling it for the public
bucket.


### Out-of-band operations with S3Guard

We call an operation out-of-band (OOB) when a bucket is used by a client with
 S3Guard, and another client runs a write (e.g delete, move, rename,
 overwrite) operation on an object in the same bucket without S3Guard.

The definition of behaviour in S3AFileSystem/MetadataStore in case of OOBs:
* A client with S3Guard
* B client without S3Guard (Directly to S3)


* OOB OVERWRITE, authoritative mode:
  * A client creates F1 file
  * B client overwrites F1 file with F2 (Same, or different file size)
  * A client's getFileStatus returns F1 metadata

* OOB OVERWRITE, NOT authoritative mode:
  * A client creates F1 file
  * B client overwrites F1 file with F2 (Same, or different file size)
  * A client's getFileStatus returns F2 metadata. In not authoritative mode we
 check S3 for the file. If the modification time of the file in S3 is greater
 than in S3Guard, we can safely return the S3 file metadata and update the
 cache.

* OOB DELETE, authoritative mode:
  * A client creates F file
  * B client deletes F file
  * A client's getFileStatus returns that the file is still there

* OOB DELETE, NOT authoritative mode:
  * A client creates F file
  * B client deletes F file
  * A client's getFileStatus returns that the file is still there

Note: authoritative and NOT authoritative mode behaves the same at
OOB DELETE case.

The behaviour in case of getting directory listings:
* File status in metadata store gets updated during the listing the same way
as in getFileStatus.


## S3Guard Command Line Interface (CLI)

Note that in some cases an AWS region or `s3a://` URI can be provided.

Metadata store URIs include a scheme that designates the backing store. For
example (e.g. `dynamodb://table_name`;). As documented above, the
AWS region can be inferred if the URI to an existing bucket is provided.


The S3A URI must also be provided for per-bucket configuration options
to be picked up. That is: when an s3a URL is provided on the command line,
all its "resolved" per-bucket settings are used to connect to, authenticate
with and configure the S3Guard table. If no such URL is provided, then
the base settings are picked up.


### Create a table: `s3guard init`

```bash
hadoop s3guard init -meta URI ( -region REGION | s3a://BUCKET )
```

Creates and initializes an empty metadata store.

A DynamoDB metadata store can be initialized with additional parameters
pertaining to capacity. 

If these values are both zero, then an on-demand DynamoDB table is created;
if positive values then they set the
[Provisioned Throughput](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html)
of the table.


```bash
[-write PROVISIONED_WRITES] [-read PROVISIONED_READS]
```

Server side encryption (SSE) can be enabled with AWS managed customer master key
(CMK), or customer managed CMK. By default the DynamoDB table will be encrypted
with AWS owned CMK. To use a customer managed CMK, you can specify its KMS key
ID, ARN, alias name, or alias ARN. If not specified, the default AWS managed CMK
for DynamoDB "alias/aws/dynamodb" will be used.

```bash
[-sse [-cmk KMS_CMK_ID]]
```

Tag argument can be added with a key=value list of tags. The table for the
metadata store will be created with these tags in DynamoDB.

```bash
[-tag key=value;]
```


Example 1

```bash
hadoop s3guard init -meta dynamodb://ireland-team -write 0 -read 0 s3a://ireland-1
```

Creates an on-demand table "ireland-team",
in the same location as the S3 bucket "ireland-1".


Example 2

```bash
hadoop s3guard init -meta dynamodb://ireland-team -region eu-west-1 --read 0 --write 0
```

Creates a table "ireland-team" in the region "eu-west-1.amazonaws.com"


Example 3

```bash
hadoop s3guard init -meta dynamodb://ireland-team -tag tag1=first;tag2=second;
```

Creates a table "ireland-team" with tags "first" and "second". The read and
write capacity will be those of the site configuration's values of
`fs.s3a.s3guard.ddb.table.capacity.read` and `fs.s3a.s3guard.ddb.table.capacity.write`;
if these are both zero then it will be an on-demand table.


Example 4

```bash
hadoop s3guard init -meta dynamodb://ireland-team -sse
```

Creates a table "ireland-team" with server side encryption enabled. The CMK will
be using the default AWS managed "alias/aws/dynamodb".


### Import a bucket: `s3guard import`

```bash
hadoop s3guard import [-meta URI] [-authoritative] [-verbose] s3a://PATH
```

Pre-populates a metadata store according to the current contents of an S3
bucket/path. If the `-meta` option is omitted, the binding information is taken
from the `core-site.xml` configuration.

Usage

```
hadoop s3guard import

import [OPTIONS] [s3a://PATH]
    import metadata from existing S3 data

Common options:
  -authoritative - Mark imported directory data as authoritative.
  -verbose - Verbose Output.
  -meta URL - Metadata repository details (implementation-specific)

Amazon DynamoDB-specific options:
  -region REGION - Service region for connections

  URLs for Amazon DynamoDB are of the form dynamodb://TABLE_NAME.
  Specifying both the -region option and an S3A path
  is not supported.
```

Example

Import all files and directories in a bucket into the S3Guard table.

```bash
hadoop s3guard import s3a://ireland-1
```

Import a directory tree, marking directories as authoritative.

```bash
hadoop s3guard import -authoritative -verbose s3a://ireland-1/fork-0008

2020-01-03 12:05:18,321 [main] INFO - Metadata store DynamoDBMetadataStore{region=eu-west-1,
 tableName=s3guard-metadata, tableArn=arn:aws:dynamodb:eu-west-1:980678866538:table/s3guard-metadata} is initialized.
2020-01-03 12:05:18,324 [main] INFO - Starting: Importing s3a://ireland-1/fork-0008
2020-01-03 12:05:18,324 [main] INFO - Importing directory s3a://ireland-1/fork-0008
2020-01-03 12:05:18,537 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-0-false
2020-01-03 12:05:18,630 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-0-true
2020-01-03 12:05:19,142 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-false/dir-0
2020-01-03 12:05:19,191 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-false/dir-1
2020-01-03 12:05:19,240 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-true/dir-0
2020-01-03 12:05:19,289 [main] INFO - Dir  s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-true/dir-1
2020-01-03 12:05:19,314 [main] INFO - Updated S3Guard with 0 files and 6 directory entries
2020-01-03 12:05:19,315 [main] INFO - Marking directory tree s3a://ireland-1/fork-0008 as authoritative
2020-01-03 12:05:19,342 [main] INFO - Importing s3a://ireland-1/fork-0008: duration 0:01.018s
Inserted 6 items into Metadata Store
```

### Compare a S3Guard table and the S3 Store: `s3guard diff`

```bash
hadoop s3guard diff [-meta URI] s3a://BUCKET
```

Lists discrepancies between a metadata store and bucket. Note that depending on
how S3Guard is used, certain discrepancies are to be expected.

Example

```bash
hadoop s3guard diff s3a://ireland-1
```

### Display information about a bucket, `s3guard bucket-info`

Prints and optionally checks the s3guard and encryption status of a bucket.

```bash
hadoop s3guard bucket-info [-guarded] [-unguarded] [-auth] [-nonauth] [-magic] [-encryption ENCRYPTION] s3a://BUCKET
```

Options

| argument | meaning |
|-----------|-------------|
| `-guarded` | Require S3Guard to be enabled |
| `-unguarded` | Force S3Guard to be disabled |
| `-auth` | Require the S3Guard mode to be "authoritative" |
| `-nonauth` | Require the S3Guard mode to be "non-authoritative" |
| `-magic` | Require the S3 filesystem to be support the "magic" committer |
| `-encryption <type>` | Require a specific server-side encryption algorithm  |

The server side encryption options are not directly related to S3Guard, but
it is often convenient to check them at the same time.

Example

```bash
hadoop s3guard bucket-info -guarded -magic s3a://ireland-1
```

List the details of bucket `s3a://ireland-1`, mandating that it must have S3Guard enabled
("-guarded") and that support for the magic S3A committer is enabled ("-magic")

```
Filesystem s3a://ireland-1
Location: eu-west-1
Filesystem s3a://ireland-1 is using S3Guard with store DynamoDBMetadataStore{region=eu-west-1, tableName=ireland-1}
Authoritative S3Guard: fs.s3a.metadatastore.authoritative=false
Metadata Store Diagnostics:
  ARN=arn:aws:dynamodb:eu-west-1:00000000:table/ireland-1
  billing-mode=provisioned
  description=S3Guard metadata store in DynamoDB
  name=ireland-1
  read-capacity=20
  region=eu-west-1
  retryPolicy=ExponentialBackoffRetry(maxRetries=9, sleepTime=100 MILLISECONDS)
  size=12812
  status=ACTIVE
  table={AttributeDefinitions: [{AttributeName: child,AttributeType: S},
    {AttributeName: parent,AttributeType: S}],TableName: ireland-1,
    KeySchema: [{AttributeName: parent,KeyType: HASH}, {AttributeName: child,KeyType: RANGE}],
    TableStatus: ACTIVE,
    CreationDateTime: Fri Aug 25 19:07:25 BST 2017,
    ProvisionedThroughput: {LastIncreaseDateTime: Tue Aug 29 11:45:18 BST 2017,
    LastDecreaseDateTime: Wed Aug 30 15:37:51 BST 2017,
    NumberOfDecreasesToday: 1,
    ReadCapacityUnits: 20,WriteCapacityUnits: 20},
    TableSizeBytes: 12812,ItemCount: 91,
    TableArn: arn:aws:dynamodb:eu-west-1:00000000:table/ireland-1,}
  write-capacity=20
The "magic" committer is supported

S3A Client
  Signing Algorithm: fs.s3a.signing-algorithm=(unset)
  Endpoint: fs.s3a.endpoint=s3-eu-west-1.amazonaws.com
  Encryption: fs.s3a.server-side-encryption-algorithm=none
  Input seek policy: fs.s3a.experimental.input.fadvise=normal
  Change Detection Source: fs.s3a.change.detection.source=etag
  Change Detection Mode: fs.s3a.change.detection.mode=server
Delegation token support is disabled
```

This listing includes all the information about the table supplied from

```bash
hadoop s3guard bucket-info -unguarded -encryption none s3a://landsat-pds
```

List the S3Guard status of clients of the public `landsat-pds` bucket,
and verifies that the data is neither tracked with S3Guard nor encrypted.


```
Filesystem s3a://landsat-pdsLocation: us-west-2
Filesystem s3a://landsat-pds is not using S3Guard
Endpoint: fs.s3a.endpoints3.amazonaws.com
Encryption: fs.s3a.server-side-encryption-algorithm=none
Input seek policy: fs.s3a.experimental.input.fadvise=normal
```

Note that other clients may have a S3Guard table set up to store metadata
on this bucket; the checks are all done from the perspective of the configuration
settings of the current client.

```bash
hadoop s3guard bucket-info -guarded -auth s3a://landsat-pds
```

Require the bucket to be using S3Guard in authoritative mode. This will normally
fail against this specific bucket.

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


### Delete a table: `s3guard destroy`

Deletes a metadata store. With DynamoDB as the store, this means
the specific DynamoDB table use to store the metadata.

```bash
hadoop s3guard destroy [-meta URI] ( -region REGION | s3a://BUCKET )
```

This *does not* delete the bucket, only the S3Guard table which it is bound
to.


Examples

```bash
hadoop s3guard destroy s3a://ireland-1
```

Deletes the table which the bucket ireland-1 is configured to use
as its MetadataStore.

```bash
hadoop s3guard destroy -meta dynamodb://ireland-team -region eu-west-1
```


### Clean up a table, `s3guard prune`

Delete all file entries in the MetadataStore table whose object "modification
time" is older than the specified age.

```bash
hadoop s3guard prune [-days DAYS] [-hours HOURS] [-minutes MINUTES]
    [-seconds SECONDS] [-tombstone] [-meta URI] ( -region REGION | s3a://BUCKET )
```

A time value of hours, minutes and/or seconds must be supplied.

1. This does not delete the entries in the bucket itself.
1. The modification time is effectively the creation time of the objects
in the S3 Bucket.
1. If an S3A URI is supplied, only the entries in the table specified by the
URI and older than a specific age are deleted.


The `-tombstone` option instructs the operation to only purge "tombstones",
markers of deleted files. These tombstone markers are only used briefly,
to indicate that a recently deleted file should not be found in listings.
As a result, there is no adverse consequences in regularly pruning old
tombstones.

Example

```bash
hadoop s3guard prune -days 7 s3a://ireland-1
```

Deletes all entries in the S3Guard table for files older than seven days from
the table associated with `s3a://ireland-1`.

```bash
hadoop s3guard prune -tombstone -days 7 s3a://ireland-1/path_prefix/
```

Deletes all entries in the S3Guard table for tombstones older than seven days from
the table associated with `s3a://ireland-1` and with the prefix `path_prefix`

```bash
hadoop s3guard prune -hours 1 -minutes 30 -meta dynamodb://ireland-team -region eu-west-1
```

Delete all file entries more than 90 minutes old from the table "`ireland-team"` in
the region `eu-west-1`.


### Audit the "authoritative state of a DynamoDB Table, `s3guard authoritative`

This recursively checks a S3Guard table to verify that all directories
underneath are marked as "authoritative", and/or that the configuration
is set for the S3A client to treat files and directories urnder the path
as authoritative.

```
hadoop s3guard authoritative

authoritative [OPTIONS] [s3a://PATH]
    Audits a DynamoDB S3Guard repository for all the entries being 'authoritative'

Options:
  -required Require directories under the path to be authoritative.
  -check-config Check the configuration for the path to be authoritative
  -verbose Verbose Output.
```

Verify that a path under an object store is declared to be authoritative
in the cluster configuration -and therefore that file entries will not be
validated against S3, and that directories marked as "authoritative" in the
S3Guard table will be treated as complete.

```bash
hadoop s3guard authoritative -check-config s3a:///ireland-1/fork-0003/test/

2020-01-03 11:42:29,147 [main] INFO  Metadata store DynamoDBMetadataStore{
  region=eu-west-1, tableName=s3guard-metadata, tableArn=arn:aws:dynamodb:eu-west-1:980678866538:table/s3guard-metadata} is initialized.
Path /fork-0003/test is not configured to be authoritative
```

Scan a store and report which directories are not marked as authoritative.

```bash
hadoop s3guard authoritative s3a://ireland-1/

2020-01-03 11:51:58,416 [main] INFO  - Metadata store DynamoDBMetadataStore{region=eu-west-1, tableName=s3guard-metadata, tableArn=arn:aws:dynamodb:eu-west-1:980678866538:table/s3guard-metadata} is initialized.
2020-01-03 11:51:58,419 [main] INFO  - Starting: audit s3a://ireland-1/
2020-01-03 11:51:58,422 [main] INFO  - Root directory s3a://ireland-1/
2020-01-03 11:51:58,469 [main] INFO  -   files 4; directories 12
2020-01-03 11:51:58,469 [main] INFO  - Directory s3a://ireland-1/Users
2020-01-03 11:51:58,521 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,522 [main] INFO  - Directory s3a://ireland-1/fork-0007
2020-01-03 11:51:58,573 [main] INFO  - Directory s3a://ireland-1/fork-0001
2020-01-03 11:51:58,626 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,626 [main] INFO  - Directory s3a://ireland-1/fork-0006
2020-01-03 11:51:58,676 [main] INFO  - Directory s3a://ireland-1/path
2020-01-03 11:51:58,734 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,735 [main] INFO  - Directory s3a://ireland-1/fork-0008
2020-01-03 11:51:58,802 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,802 [main] INFO  - Directory s3a://ireland-1/fork-0004
2020-01-03 11:51:58,854 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,855 [main] WARN   - Directory s3a://ireland-1/fork-0003 is not authoritative
2020-01-03 11:51:58,905 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:58,906 [main] INFO  - Directory s3a://ireland-1/fork-0005
2020-01-03 11:51:58,955 [main] INFO  - Directory s3a://ireland-1/customsignerpath2
2020-01-03 11:51:59,006 [main] INFO  - Directory s3a://ireland-1/fork-0002
2020-01-03 11:51:59,063 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:59,064 [main] INFO  - Directory s3a://ireland-1/customsignerpath1
2020-01-03 11:51:59,121 [main] INFO  - Directory s3a://ireland-1/Users/stevel
2020-01-03 11:51:59,170 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:59,171 [main] INFO  - Directory s3a://ireland-1/fork-0001/test
2020-01-03 11:51:59,233 [main] INFO  - Directory s3a://ireland-1/path/style
2020-01-03 11:51:59,282 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:59,282 [main] INFO  - Directory s3a://ireland-1/fork-0008/test
2020-01-03 11:51:59,338 [main] INFO  -   files 15; directories 10
2020-01-03 11:51:59,339 [main] INFO  - Directory s3a://ireland-1/fork-0004/test
2020-01-03 11:51:59,394 [main] WARN   - Directory s3a://ireland-1/fork-0003/test is not authoritative
2020-01-03 11:51:59,451 [main] INFO  -   files 35; directories 1
2020-01-03 11:51:59,451 [main] INFO  - Directory s3a://ireland-1/fork-0002/test
2020-01-03 11:51:59,508 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects
2020-01-03 11:51:59,558 [main] INFO  -   files 0; directories 1
2020-01-03 11:51:59,559 [main] INFO  - Directory s3a://ireland-1/path/style/access
2020-01-03 11:51:59,610 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-2-0-false
2020-01-03 11:51:59,660 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-2-1-false
2020-01-03 11:51:59,719 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-0-true
2020-01-03 11:51:59,773 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-true
2020-01-03 11:51:59,824 [main] INFO  -   files 0; directories 2
2020-01-03 11:51:59,824 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-2-1-true
2020-01-03 11:51:59,879 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-1-false
2020-01-03 11:51:59,939 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-0-false
2020-01-03 11:51:59,990 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-2-0-true
2020-01-03 11:52:00,042 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-false
2020-01-03 11:52:00,094 [main] INFO  -   files 0; directories 2
2020-01-03 11:52:00,094 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-0-0-1-true
2020-01-03 11:52:00,144 [main] WARN   - Directory s3a://ireland-1/fork-0003/test/ancestor is not authoritative
2020-01-03 11:52:00,197 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk
2020-01-03 11:52:00,245 [main] INFO  -   files 0; directories 1
2020-01-03 11:52:00,245 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-true/dir-0
2020-01-03 11:52:00,296 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-true/dir-1
2020-01-03 11:52:00,346 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-false/dir-0
2020-01-03 11:52:00,397 [main] INFO  - Directory s3a://ireland-1/fork-0008/test/doTestListFiles-2-0-0-false/dir-1
2020-01-03 11:52:00,479 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools
2020-01-03 11:52:00,530 [main] INFO  -   files 0; directories 1
2020-01-03 11:52:00,530 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws
2020-01-03 11:52:00,582 [main] INFO  -   files 0; directories 1
2020-01-03 11:52:00,582 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws/target
2020-01-03 11:52:00,636 [main] INFO  -   files 0; directories 1
2020-01-03 11:52:00,637 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws/target/test-dir
2020-01-03 11:52:00,691 [main] INFO  -   files 0; directories 3
2020-01-03 11:52:00,691 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws/target/test-dir/2
2020-01-03 11:52:00,752 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws/target/test-dir/5
2020-01-03 11:52:00,807 [main] INFO  - Directory s3a://ireland-1/Users/stevel/Projects/hadoop-trunk/hadoop-tools/hadoop-aws/target/test-dir/8
2020-01-03 11:52:00,862 [main] INFO   - Scanned 45 directories - 3 were not marked as authoritative
2020-01-03 11:52:00,863 [main] INFO  - audit s3a://ireland-1/: duration 0:02.444s
```

Scan the path/bucket and fail if any entry is non-authoritative.

```bash
hadoop s3guard authoritative -verbose -required s3a://ireland-1/

2020-01-03 11:47:40,288 [main] INFO  - Metadata store DynamoDBMetadataStore{region=eu-west-1, tableName=s3guard-metadata, tableArn=arn:aws:dynamodb:eu-west-1:980678866538:table/s3guard-metadata} is initialized.
2020-01-03 11:47:40,291 [main] INFO  - Starting: audit s3a://ireland-1/
2020-01-03 11:47:40,295 [main] INFO  - Root directory s3a://ireland-1/
2020-01-03 11:47:40,336 [main] INFO  -  files 4; directories 12
2020-01-03 11:47:40,336 [main] INFO  - Directory s3a://ireland-1/Users
2020-01-03 11:47:40,386 [main] INFO  -  files 0; directories 1
2020-01-03 11:47:40,386 [main] INFO  - Directory s3a://ireland-1/fork-0007
2020-01-03 11:47:40,435 [main] INFO  -  files 1; directories 0
2020-01-03 11:47:40,435 [main] INFO  - Directory s3a://ireland-1/fork-0001
2020-01-03 11:47:40,486 [main] INFO  -  files 0; directories 1
2020-01-03 11:47:40,486 [main] INFO  - Directory s3a://ireland-1/fork-0006
2020-01-03 11:47:40,534 [main] INFO  -  files 1; directories 0
2020-01-03 11:47:40,535 [main] INFO  - Directory s3a://ireland-1/path
2020-01-03 11:47:40,587 [main] INFO  -  files 0; directories 1
2020-01-03 11:47:40,588 [main] INFO  - Directory s3a://ireland-1/fork-0008
2020-01-03 11:47:40,641 [main] INFO  -  files 0; directories 1
2020-01-03 11:47:40,642 [main] INFO  - Directory s3a://ireland-1/fork-0004
2020-01-03 11:47:40,692 [main] INFO  -  files 0; directories 1
2020-01-03 11:47:40,693 [main] WARN  - Directory s3a://ireland-1/fork-0003 is not authoritative
2020-01-03 11:47:40,693 [main] INFO  - audit s3a://ireland-1/: duration 0:00.402s
2020-01-03 11:47:40,698 [main] INFO  - Exiting with status 46: `s3a://ireland-1/fork-0003': Directory is not marked as authoritative in the S3Guard store
```

This command is primarily for testing.

### Tune the I/O capacity of the DynamoDB Table, `s3guard set-capacity`

Alter the read and/or write capacity of a s3guard table created with provisioned
I/O capacity.

```bash
hadoop s3guard set-capacity [--read UNIT] [--write UNIT] ( -region REGION | s3a://BUCKET )
```

The `--read` and `--write` units are those of `s3guard init`.

It cannot be used to change the I/O capacity of an on demand table (there is
no need), and nor can it be used to convert an existing table to being
on-demand. For that the AWS console must be used.

Example

```
hadoop s3guard set-capacity  -read 20 -write 20 s3a://ireland-1
```

Set the capacity of the table used by bucket `s3a://ireland-1` to 20 read
and 20 write. (This is a low number, incidentally)

```
2017-08-30 16:21:26,343 [main] INFO  s3guard.S3GuardTool (S3GuardTool.java:initMetadataStore(229)) - Metadata store DynamoDBMetadataStore{region=eu-west-1, tableName=ireland-1} is initialized.
2017-08-30 16:21:26,344 [main] INFO  s3guard.DynamoDBMetadataStore (DynamoDBMetadataStore.java:updateParameters(1084)) - Current table capacity is read: 25, write: 25
2017-08-30 16:21:26,344 [main] INFO  s3guard.DynamoDBMetadataStore (DynamoDBMetadataStore.java:updateParameters(1086)) - Changing capacity of table to read: 20, write: 20
Metadata Store Diagnostics:
  ARN=arn:aws:dynamodb:eu-west-1:00000000000:table/ireland-1
  billing-mode=provisioned
  description=S3Guard metadata store in DynamoDB
  name=ireland-1
  read-capacity=25
  region=eu-west-1
  retryPolicy=ExponentialBackoffRetry(maxRetries=9, sleepTime=100 MILLISECONDS)
  size=12812
  status=UPDATING
  table={ ... }
  write-capacity=25
```

After the update, the table status changes to `UPDATING`; this is a sign that
the capacity has been changed.

Repeating the same command will not change the capacity, as both read and
write values match that already in use.

```
2017-08-30 16:24:35,337 [main] INFO  s3guard.DynamoDBMetadataStore (DynamoDBMetadataStore.java:updateParameters(1090)) - Table capacity unchanged at read: 20, write: 20
Metadata Store Diagnostics:
  ARN=arn:aws:dynamodb:eu-west-1:00000000000:table/ireland-1
  billing-mode=provisioned
  description=S3Guard metadata store in DynamoDB
  name=ireland-1
  read-capacity=20
  region=eu-west-1
  retryPolicy=ExponentialBackoffRetry(maxRetries=9, sleepTime=100 MILLISECONDS)
  size=12812
  status=ACTIVE
  table={ ... }
  write-capacity=20
```
*Note*: There is a limit to how many times in a 24 hour period the capacity
of a bucket can be changed, either through this command or the AWS console.

### Check the consistency of the metadata store, `s3guard fsck`

Compares S3 with MetadataStore, and returns a failure status if any
rules or invariants are violated. Only works with DynamoDB metadata stores.

```bash
hadoop s3guard fsck [-check | -internal] [-fix] (s3a://BUCKET | s3a://PATH_PREFIX)
```

`-check` operation checks the metadata store from the S3 perspective, but
does not fix any issues.
The consistency issues will be logged in ERROR loglevel.

`-internal` operation checks the internal consistency of the metadata store,
but does not fix any issues.

`-fix` operation fixes consistency issues between the metadatastore and the S3
bucket. This parameter is optional, and can be used together with check or
internal parameters, but not alone.
The following fix is implemented:
- Remove orphan entries from DDB

The errors found will be logged at the ERROR log level.

*Note*: `-check` and `-internal` operations can be used only as separate
commands. Running `fsck` with both will result in an error.

Example

```bash
hadoop s3guard fsck -check s3a://ireland-1/path_prefix/
```

Checks the metadata store while iterating through the S3 bucket.
The path_prefix will be used as the root element of the check.

```bash
hadoop s3guard fsck -internal s3a://ireland-1/path_prefix/
```

Checks the metadata store internal consistency.
The path_prefix will be used as the root element of the check.


## Debugging and Error Handling

If you run into network connectivity issues, or have a machine failure in the
middle of an operation, you may end up with your metadata store having state
that differs from S3.  The S3Guard CLI commands, covered in the CLI section
above, can be used to diagnose and repair these issues.

There are some logs whose log level can be increased to provide more
information.

```properties
# Log S3Guard classes
log4j.logger.org.apache.hadoop.fs.s3a.s3guard=DEBUG

# Log all S3A classes
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG

# Enable debug logging of AWS DynamoDB client
log4j.logger.com.amazonaws.services.dynamodbv2.AmazonDynamoDB

# Log all HTTP requests made; includes S3 interaction. This may
# include sensitive information such as account IDs in HTTP headers.
log4j.logger.com.amazonaws.request=DEBUG
```

If all else fails, S3Guard is designed to allow for easy recovery by deleting
the metadata store data. In DynamoDB, this can be accomplished by simply
deleting the table, and allowing S3Guard to recreate it from scratch.  Note
that S3Guard tracks recent changes to file metadata to implement consistency.
Deleting the metadata store table will simply result in a period of eventual
consistency for any file modifications that were made right before the table
was deleted.

### Enabling a log message whenever S3Guard is *disabled*

When dealing with support calls related to the S3A connector, "is S3Guard on?"
is the usual opening question. This can be determined by looking at the application logs for
messages about S3Guard starting -the absence of S3Guard can only be inferred by the absence
of such messages.

There is a another strategy: have the S3A Connector log whenever *S3Guard is not enabled*

This can be done in the configuration option `fs.s3a.s3guard.disabled.warn.level`

```xml
<property>
 <name>fs.s3a.s3guard.disabled.warn.level</name>
 <value>silent</value>
 <description>
   Level to print a message when S3Guard is disabled.
   Values: 
   "warn": log at WARN level
   "inform": log at INFO level
   "silent": log at DEBUG level
   "fail": raise an exception
 </description>
</property>
```

The `fail` option is clearly more than logging; it exists as an extreme debugging
tool. Use with care.

### Failure Semantics

Operations which modify metadata will make changes to S3 first. If, and only
if, those operations succeed, the equivalent changes will be made to the
Metadata Store.

These changes to S3 and Metadata Store are not fully-transactional:  If the S3
operations succeed, and the subsequent Metadata Store updates fail, the S3
changes will *not* be rolled back.  In this case, an error message will be
logged.

### Versioning

S3Guard tables are created with a version marker entry and table tag.
The entry is created with the primary key and child entry of `../VERSION`; 
the use of a relative path guarantees that it will not be resolved.
Table tag key is named `s3guard_version`.

When the table is initialized by S3Guard, the table will be tagged during the 
creating and the version marker entry will be created in the table.
If the table lacks the version marker entry or tag, S3Guard will try to create
it according to the following rules:

1. If the table lacks both version markers AND it's empty, both markers will be added. 
If the table is not empty the check throws IOException
1. If there's no version marker ITEM, the compatibility with the TAG
will be checked, and the version marker ITEM will be added if the
TAG version is compatible.
If the TAG version is not compatible, the check throws OException
1. If there's no version marker TAG, the compatibility with the ITEM
version marker will be checked, and the version marker ITEM will be
added if the ITEM version is compatible.
If the ITEM version is not compatible, the check throws IOException
1. If the TAG and ITEM versions are both present then both will be checked
for compatibility. If the ITEM or TAG version marker is not compatible,
the check throws IOException

*Note*: If the user does not have sufficient rights to tag the table the 
initialization of S3Guard will not fail, but there will be no version marker tag
on the dynamo table.

*Versioning policy*

1. The version number of an S3Guard table will only be incremented when
an incompatible change is made to the table structure —that is, the structure
has changed so that it is no longer readable by older versions, or because
it has added new mandatory fields which older versions do not create.
1. The version number of S3Guard tables will only be changed by incrementing
the value.
1. Updated versions of S3Guard MAY continue to support older version tables.
1. If an incompatible change is made such that existing tables are not compatible,
then a means shall be provided to update existing tables. For example:
an option in the Command Line Interface, or an option to upgrade tables
during S3Guard initialization.

*Note*: this policy does not indicate any intent to upgrade table structures
in an incompatible manner. The version marker in tables exists to support
such an option if it ever becomes necessary, by ensuring that all S3Guard
client can recognise any version mismatch.

## Security

All users of the DynamoDB table must have write access to it. This
effectively means they must have write access to the entire object store.

There's not been much testing of using a S3Guard Metadata Store
with a read-only S3 Bucket. It *should* work, provided all users
have write access to the DynamoDB table. And, as updates to the Metadata Store
are only made after successful file creation, deletion and rename, the
store is *unlikely* to get out of sync, it is still something which
merits more testing before it could be considered reliable.

## Managing DynamoDB I/O Capacity

Historically, DynamoDB has been not only billed on use (data and I/O requests)
-but on provisioned I/O Capacity.

With Provisioned IO, when an application makes more requests than
the allocated capacity permits, the request is rejected; it is up to
the calling application to detect when it is being so throttled and
react. S3Guard does this, but as a result: when the client is being
throttled, operations are slower. This capacity throttling is averaged
over a few minutes: a briefly overloaded table will not be throttled,
but the rate cannot be sustained.

The load on a table is visible in the AWS console: go to the
DynamoDB page for the table and select the "metrics" tab.
If the graphs of throttled read or write
requests show that a lot of throttling has taken place, then there is not
enough allocated capacity for the applications making use of the table.

Similarly, if the capacity graphs show that the read or write loads are
low compared to the allocated capacities, then the table *may* be overprovisioned
for the current workload.

The S3Guard connector to DynamoDB can be configured to make
multiple attempts to repeat a throttled request, with an exponential
backoff between them.

The relevant settings for managing retries in the connector are:

```xml

<property>
  <name>fs.s3a.s3guard.ddb.max.retries</name>
  <value>9</value>
    <description>
      Max retries on throttled/incompleted DynamoDB operations
      before giving up and throwing an IOException.
      Each retry is delayed with an exponential
      backoff timer which starts at 100 milliseconds and approximately
      doubles each time.  The minimum wait before throwing an exception is
      sum(100, 200, 400, 800, .. 100*2^N-1 ) == 100 * ((2^N)-1)
    </description>
</property>

<property>
  <name>fs.s3a.s3guard.ddb.throttle.retry.interval</name>
  <value>100ms</value>
    <description>
      Initial interval to retry after a request is throttled events;
      the back-off policy is exponential until the number of retries of
      fs.s3a.s3guard.ddb.max.retries is reached.
    </description>
</property>

<property>
  <name>fs.s3a.s3guard.ddb.background.sleep</name>
  <value>25ms</value>
  <description>
    Length (in milliseconds) of pause between each batch of deletes when
    pruning metadata.  Prevents prune operations (which can typically be low
    priority background operations) from overly interfering with other I/O
    operations.
  </description>
</property>
```

Having a large value for `fs.s3a.s3guard.ddb.max.retries` will ensure
that clients of an overloaded table will not fail immediately. However
queries may be unexpectedly slow.

If operations, especially directory operations, are slow, check the AWS
console. It is also possible to set up AWS alerts for capacity limits
being exceeded.

### <a name="on-demand"></a> On-Demand Dynamo Capacity

[Amazon DynamoDB On-Demand](https://aws.amazon.com/blogs/aws/amazon-dynamodb-on-demand-no-capacity-planning-and-pay-per-request-pricing/)
removes the need to pre-allocate I/O capacity for S3Guard tables.
Instead the caller is _only_ charged per I/O Operation.

* There are no SLA capacity guarantees. This is generally not an issue
for S3Guard applications.
* There's no explicit limit on I/O capacity, so operations which make
heavy use of S3Guard tables (for example: SQL query planning) do not
get throttled.
* You are charged more per DynamoDB API call, in exchange for paying nothing
when you are not interacting with DynamoDB.
* There's no way put a limit on the I/O; you may unintentionally run up
large bills through sustained heavy load.
* The `s3guard set-capacity` command fails: it does not make sense any more.

When idle, S3Guard tables are only billed for the data stored, not for
any unused capacity. For this reason, there is no performance benefit
from sharing a single S3Guard table across multiple buckets.

*Creating a S3Guard Table with On-Demand Tables*

The default settings for S3Guard are to create on-demand tables; this
can also be done explicitly in the `s3guard init` command by setting the
read and write capacities to zero.


```bash
hadoop s3guard init -meta dynamodb://ireland-team -write 0 -read 0 s3a://ireland-1
```

*Enabling DynamoDB On-Demand for an existing S3Guard table*

You cannot currently convert an existing S3Guard table to being an on-demand
table through the `s3guard` command.

It can be done through the AWS console or [the CLI](https://docs.aws.amazon.com/cli/latest/reference/dynamodb/update-table.html).
From the Web console or the command line, switch the billing to pay-per-request.

Once enabled, the read and write capacities of the table listed in the
`hadoop s3guard bucket-info` command become "0", and the "billing-mode"
attribute changes to "per-request":

```
> hadoop s3guard bucket-info s3a://example-bucket/

Filesystem s3a://example-bucket
Location: eu-west-1
Filesystem s3a://example-bucket is using S3Guard with store
  DynamoDBMetadataStore{region=eu-west-1, tableName=example-bucket,
  tableArn=arn:aws:dynamodb:eu-west-1:11111122223333:table/example-bucket}
Authoritative S3Guard: fs.s3a.metadatastore.authoritative=false
Metadata Store Diagnostics:
  ARN=arn:aws:dynamodb:eu-west-1:11111122223333:table/example-bucket
  billing-mode=per-request
  description=S3Guard metadata store in DynamoDB
  name=example-bucket
  persist.authoritative.bit=true
  read-capacity=0
  region=eu-west-1
  retryPolicy=ExponentialBackoffRetry(maxRetries=9, sleepTime=250 MILLISECONDS)
  size=66797
  status=ACTIVE
  table={AttributeDefinitions:
    [{AttributeName: child,AttributeType: S},
     {AttributeName: parent,AttributeType: S}],
     TableName: example-bucket,
     KeySchema: [{
       AttributeName: parent,KeyType: HASH},
       {AttributeName: child,KeyType: RANGE}],
     TableStatus: ACTIVE,
     CreationDateTime: Thu Oct 11 18:51:14 BST 2018,
     ProvisionedThroughput: {
       LastIncreaseDateTime: Tue Oct 30 16:48:45 GMT 2018,
       LastDecreaseDateTime: Tue Oct 30 18:00:03 GMT 2018,
       NumberOfDecreasesToday: 0,
       ReadCapacityUnits: 0,
       WriteCapacityUnits: 0},
     TableSizeBytes: 66797,
     ItemCount: 415,
     TableArn: arn:aws:dynamodb:eu-west-1:11111122223333:table/example-bucket,
     TableId: a7b0728a-f008-4260-b2a0-aaaaabbbbb,}
  write-capacity=0
The "magic" committer is supported
```

### <a name="autoscaling"></a> Autoscaling (Provisioned Capacity) S3Guard tables.

[DynamoDB Auto Scaling](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.html)
can automatically increase and decrease the allocated capacity.

Before DynamoDB On-Demand was introduced, autoscaling was the sole form
of dynamic scaling. 

Experiments with S3Guard and DynamoDB Auto Scaling have shown that any Auto Scaling
operation will only take place after callers have been throttled for a period of
time. The clients will still need to be configured to retry when overloaded
until any extra capacity is allocated. Furthermore, as this retrying will
block the threads from performing other operations -including more I/O, the
the autoscale may not scale fast enough.

This is why the DynamoDB On-Demand appears is a better option for
workloads with Hadoop, Spark, Hive and other applications.

If autoscaling is to be used, we recommend experimenting with the option,
based on usage information collected from previous days, and choosing a
combination of retry counts and an interval which allow for the clients to cope with
some throttling, but not to time-out other applications.

## Read-After-Overwrite Consistency

S3Guard provides read-after-overwrite consistency through ETags (default) or
object versioning checked either on the server (default) or client. This works
such that a reader reading a file after an overwrite either sees the new version
of the file or an error. Without S3Guard, new readers may see the original
version. Once S3 reaches eventual consistency, new readers will see the new
version.

Readers using S3Guard will usually see the new file version, but may
in rare cases see `RemoteFileChangedException` instead. This would occur if
an S3 object read cannot provide the version tracked in S3Guard metadata.

S3Guard achieves this behavior by storing ETags and object version IDs in the
S3Guard metadata store (e.g. DynamoDB). On opening a file, S3AFileSystem
will look in S3 for the version of the file indicated by the ETag or object
version ID stored in the metadata store. If that version is unavailable,
`RemoteFileChangedException` is thrown. Whether ETag or version ID and
server or client mode is used is determed by the
[fs.s3a.change.detection configuration options](./index.html#Handling_Read-During-Overwrite).

### No Versioning Metadata Available

When the first S3AFileSystem clients are upgraded to a version of
`S3AFileSystem` that contains these change tracking features, any existing
S3Guard metadata will not contain ETags or object version IDs. Reads of files
tracked in such S3Guard metadata will access whatever version of the file is
available in S3 at the time of read. Only if the file is subsequently updated
will S3Guard start tracking ETag and object version ID and as such generating
`RemoteFileChangedException` if an inconsistency is detected.

Similarly, when S3Guard metadata is pruned, S3Guard will no longer be able to
detect an inconsistent read. S3Guard metadata should be retained for at least
as long as the perceived possible read-after-overwrite temporary inconsistency
window. That window is expected to be short, but there are no guarantees so it
is at the administrator's discretion to weigh the risk.

### Known Limitations

#### S3 Select

S3 Select does not provide a capability for server-side ETag or object
version ID qualification. Whether `fs.s3a.change.detection.mode` is "client" or
"server", S3Guard will cause a client-side check of the file version before
opening the file with S3 Select. If the current version does not match the
version tracked in S3Guard, `RemoteFileChangedException` is thrown.

It is still possible that the S3 Select read will access a different version of
the file, if the visible file version changes between the version check and
the opening of the file. This can happen due to eventual consistency or
an overwrite of the file between the version check and the open of the file.

#### Rename

Rename is implemented via copy in S3. With `fs.s3a.change.detection.mode` set
to "client", a fully reliable mechansim for ensuring the copied content is the expected
content is not possible. This is the case since there isn't necessarily a way
to know the expected ETag or version ID to appear on the object resulting from
the copy.

Furthermore, if `fs.s3a.change.detection.mode` is "server" and a third-party S3
implementation is used that doesn't honor the provided ETag or version ID,
S3AFileSystem and S3Guard cannot detect it.

When `fs.s3.change.detection.mode` is "client", a client-side check
will be performed before the copy to ensure the current version of the file
matches S3Guard metadata. If not, `RemoteFileChangedException` is thrown.
Similar to as discussed with regard to S3 Select, this is not sufficient to
guarantee that same version is the version copied.

When `fs.s3.change.detection.mode` server, the expected version is also specified
in the underlying S3 `CopyObjectRequest`. As long as the server honors it, the
copied object will be correct.

All this said, with the defaults of `fs.s3.change.detection.mode` of "server" and
`fs.s3.change.detection.source` of "etag", when working with Amazon's S3, copy should in fact
either copy the expected file version or, in the case of an eventual consistency
anomaly, generate `RemoteFileChangedException`. The same should be true when
`fs.s3.change.detection.source` = "versionid".

#### Out of Sync Metadata

The S3Guard version tracking metadata (ETag or object version ID) could become
out of sync with the true current object metadata in S3.  For example, S3Guard
is still tracking v1 of some file after v2 has been written.  This could occur
for reasons such as a writer writing without utilizing S3Guard and/or
S3AFileSystem or simply due to a write with S3AFileSystem and S3Guard that wrote
successfully to S3, but failed in communication with S3Guard's metadata store
(e.g. DynamoDB).

If this happens, reads of the affected file(s) will result in
`RemoteFileChangedException` until one of:

* the S3Guard metadata is corrected out-of-band
* the file is overwritten (causing an S3Guard metadata update)
* the S3Guard metadata is pruned

The S3Guard metadata for a file can be corrected with the `s3guard import`
command as discussed above. The command can take a file URI instead of a
bucket URI to correct the metadata for a single file. For example:

```bash
hadoop s3guard import [-meta URI] s3a://my-bucket/file-with-bad-metadata
```

## Troubleshooting

### Error: `S3Guard table lacks version marker.`

The table which was intended to be used as a S3guard metadata store
does not have any version marker indicating that it is a S3Guard table.

It may be that this is not a S3Guard table.

* Make sure that this is the correct table name.
* Delete the table, so it can be rebuilt.

### Error: `Database table is from an incompatible S3Guard version`

This indicates that the version of S3Guard which created (or possibly updated)
the database table is from a different version that that expected by the S3A
client.

This error will also include the expected and actual version numbers.

If the expected version is lower than the actual version, then the version
of the S3A client library is too old to interact with this S3Guard-managed
bucket. Upgrade the application/library.

If the expected version is higher than the actual version, then the table
itself will need upgrading.

### Error `"DynamoDB table TABLE does not exist in region REGION; auto-creation is turned off"`

S3Guard could not find the DynamoDB table for the Metadata Store,
and it was not configured to create it. Either the table was missing,
or the configuration is preventing S3Guard from finding the table.

1. Verify that the value of `fs.s3a.s3guard.ddb.table` is correct.
1. If the region for an existing table has been set in
`fs.s3a.s3guard.ddb.region`, verify that the value is correct.
1. If the region is not set, verify that the table exists in the same
region as the bucket being used.
1. Create the table if necessary.


### Error `"The level of configured provisioned throughput for the table was exceeded"`

```
org.apache.hadoop.fs.s3a.AWSServiceThrottledException: listFiles on s3a://bucket/10/d1/d2/d3:
com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException:
The level of configured provisioned throughput for the table was exceeded.
Consider increasing your provisioning level with the UpdateTable API.
(Service: AmazonDynamoDBv2; Status Code: 400;
Error Code: ProvisionedThroughputExceededException;
```
The I/O load of clients of the (shared) DynamoDB table was exceeded.

1. Switch to On-Demand Dynamo DB tables (AWS console)
1. Increase the capacity of the DynamoDB table (AWS console or `s3guard set-capacity`)/
1. Increase the retry count and/or sleep time of S3Guard on throttle events (Hadoop configuration).

### Error `Max retries exceeded`

The I/O load of clients of the (shared) DynamoDB table was exceeded, and
the number of attempts to retry the operation exceeded the configured amount.

1. Switch to On-Demand Dynamo DB tables (AWS console).
1. Increase the capacity of the DynamoDB table.
1. Increase the retry count and/or sleep time of S3Guard on throttle events.


### Error when running `set-capacity`: `org.apache.hadoop.fs.s3a.AWSServiceThrottledException: ProvisionTable`

```
org.apache.hadoop.fs.s3a.AWSServiceThrottledException: ProvisionTable on s3guard-example:
com.amazonaws.services.dynamodbv2.model.LimitExceededException:
Subscriber limit exceeded: Provisioned throughput decreases are limited within a given UTC day.
After the first 4 decreases, each subsequent decrease in the same UTC day can be performed at most once every 3600 seconds.
Number of decreases today: 6.
Last decrease at Wednesday, July 25, 2018 8:48:14 PM UTC.
Next decrease can be made at Wednesday, July 25, 2018 9:48:14 PM UTC
```

There's are limit on how often you can change the capacity of an DynamoDB table;
if you call `set-capacity` too often, it fails. Wait until the after the time indicated
and try again.

### Error `Invalid region specified`

```
java.io.IOException: Invalid region specified "iceland-2":
  Region can be configured with fs.s3a.s3guard.ddb.region:
  us-gov-west-1, us-east-1, us-east-2, us-west-1, us-west-2,
  eu-west-1, eu-west-2, eu-west-3, eu-central-1, ap-south-1,
  ap-southeast-1, ap-southeast-2, ap-northeast-1, ap-northeast-2,
  sa-east-1, cn-north-1, cn-northwest-1, ca-central-1
  at org.apache.hadoop.fs.s3a.s3guard.DynamoDBClientFactory$DefaultDynamoDBClientFactory.getRegion
  at org.apache.hadoop.fs.s3a.s3guard.DynamoDBClientFactory$DefaultDynamoDBClientFactory.createDynamoDBClient
```

The region specified in `fs.s3a.s3guard.ddb.region` is invalid.

### "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"

```
ValidationException; One or more parameter values were invalid:
  Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when
  BillingMode is PAY_PER_REQUEST
  (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ValidationException)
```

On-Demand DynamoDB tables do not have any fixed capacity -it is an error
to try to change it with the `set-capacity` command.

### `MetadataPersistenceException`

A filesystem write operation failed to persist metadata to S3Guard. The file was
successfully written to S3 and now the S3Guard metadata is likely to be out of
sync.

See [Fail on Error](#fail-on-error) for more detail.

### Error `RemoteFileChangedException`

An exception like the following could occur for a couple of reasons:

* the S3Guard metadata is out of sync with the true S3 metadata.  For
example, the S3Guard DynamoDB table is tracking a different ETag than the ETag
shown in the exception.  This may suggest the object was updated in S3 without
involvement from S3Guard or there was a transient failure when S3Guard tried to
write to DynamoDB.

* S3 is exhibiting read-after-overwrite temporary inconsistency.  The S3Guard
metadata was updated with a new ETag during a recent write, but the current read
is not seeing that ETag due to S3 eventual consistency.  This exception prevents
the reader from an inconsistent read where the reader sees an older version of
the file.

```
org.apache.hadoop.fs.s3a.RemoteFileChangedException: open 's3a://my-bucket/test/file.txt':
  Change reported by S3 while reading at position 0.
  ETag 4e886e26c072fef250cfaf8037675405 was unavailable
  at org.apache.hadoop.fs.s3a.impl.ChangeTracker.processResponse(ChangeTracker.java:167)
  at org.apache.hadoop.fs.s3a.S3AInputStream.reopen(S3AInputStream.java:207)
  at org.apache.hadoop.fs.s3a.S3AInputStream.lambda$lazySeek$1(S3AInputStream.java:355)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$2(Invoker.java:195)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:109)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:265)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:322)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:261)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:193)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:215)
  at org.apache.hadoop.fs.s3a.S3AInputStream.lazySeek(S3AInputStream.java:348)
  at org.apache.hadoop.fs.s3a.S3AInputStream.read(S3AInputStream.java:381)
  at java.io.FilterInputStream.read(FilterInputStream.java:83)
```

### Error `AWSClientIOException: copyFile` caused by `NullPointerException`

The AWS SDK has an [issue](https://github.com/aws/aws-sdk-java/issues/1644)
where it will throw a relatively generic `AmazonClientException` caused by
`NullPointerException` when copying a file and specifying a precondition
that cannot be met.  This can bubble up from `S3AFileSystem.rename()`. It
suggests that the file in S3 is inconsistent with the metadata in S3Guard.

```
org.apache.hadoop.fs.s3a.AWSClientIOException: copyFile(test/rename-eventually2.dat, test/dest2.dat) on test/rename-eventually2.dat: com.amazonaws.AmazonClientException: Unable to complete transfer: null: Unable to complete transfer: null
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:201)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:314)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:406)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:310)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:285)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.copyFile(S3AFileSystem.java:3034)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerRename(S3AFileSystem.java:1258)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.rename(S3AFileSystem.java:1119)
  at org.apache.hadoop.fs.s3a.ITestS3ARemoteFileChanged.lambda$testRenameEventuallyConsistentFile2$6(ITestS3ARemoteFileChanged.java:556)
  at org.apache.hadoop.test.LambdaTestUtils.intercept(LambdaTestUtils.java:498)
  at org.apache.hadoop.fs.s3a.ITestS3ARemoteFileChanged.testRenameEventuallyConsistentFile2(ITestS3ARemoteFileChanged.java:554)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
  at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
  at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
  at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
  at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
  at org.junit.internal.runners.statements.FailOnTimeout$CallableStatement.call(FailOnTimeout.java:298)
  at org.junit.internal.runners.statements.FailOnTimeout$CallableStatement.call(FailOnTimeout.java:292)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.lang.Thread.run(Thread.java:748)
Caused by: com.amazonaws.AmazonClientException: Unable to complete transfer: null
  at com.amazonaws.services.s3.transfer.internal.AbstractTransfer.unwrapExecutionException(AbstractTransfer.java:286)
  at com.amazonaws.services.s3.transfer.internal.AbstractTransfer.rethrowExecutionException(AbstractTransfer.java:265)
  at com.amazonaws.services.s3.transfer.internal.CopyImpl.waitForCopyResult(CopyImpl.java:67)
  at org.apache.hadoop.fs.s3a.impl.CopyOutcome.waitForCopy(CopyOutcome.java:72)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$copyFile$14(S3AFileSystem.java:3047)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:109)
  ... 25 more
Caused by: java.lang.NullPointerException
  at com.amazonaws.services.s3.transfer.internal.CopyCallable.copyInOneChunk(CopyCallable.java:154)
  at com.amazonaws.services.s3.transfer.internal.CopyCallable.call(CopyCallable.java:134)
  at com.amazonaws.services.s3.transfer.internal.CopyMonitor.call(CopyMonitor.java:132)
  at com.amazonaws.services.s3.transfer.internal.CopyMonitor.call(CopyMonitor.java:43)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
  ... 1 more
```

### Error `Attempt to change a resource which is still in use: Table is being deleted`

```
com.amazonaws.services.dynamodbv2.model.ResourceInUseException:
  Attempt to change a resource which is still in use: Table is being deleted:
   s3guard.test.testDynamoDBInitDestroy351245027
    (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ResourceInUseException;)
```

You have attempted to call `hadoop s3guard destroy` on a table which is already
being destroyed.

## Other Topics

For details on how to test S3Guard, see [Testing S3Guard](./testing.html#s3guard)
