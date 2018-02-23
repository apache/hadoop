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

**Experimental Feature**

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

*S3Guard* is an experimental feature for the S3A client of the S3 object store,
which can use a (consistent) database as the store of metadata about objects
in an S3 bucket.

S3Guard

1. May improve performance on directory listing/scanning operations,
including those which take place during the partitioning period of query
execution, the process where files are listed and the work divided up amongst
processes.

1. Permits a consistent view of the object store. Without this, changes in
objects may not be immediately visible, especially in listing operations.

1. Offers a platform for future performance improvements for running Hadoop
workloads on top of object stores

The basic idea is that, for each operation in the Hadoop S3 client (s3a) that
reads or modifies metadata, a shadow copy of that metadata is stored in a
separate MetadataStore implementation.  Each MetadataStore implementation
offers HDFS-like consistency for the metadata, and may also provide faster
lookups for things like file status or directory listings.

For links to early design documents and related patches, see
[HADOOP-13345](https://issues.apache.org/jira/browse/HADOOP-13345).

*Important*

* S3Guard is experimental and should be considered unstable.

* While all underlying data is persisted in S3, if, for some reason,
the S3Guard-cached metadata becomes inconsistent with that in S3,
queries on the data may become incorrect.
For example, new datasets may be omitted, objects may be overwritten,
or clients may not be aware that some data has been deleted.
It is essential for all clients writing to an S3Guard-enabled
S3 Repository to use the feature. Clients reading the data may work directly
with the S3A data, in which case the normal S3 consistency guarantees apply.


## Setting up S3Guard

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
implementation class above, is the *allow authoritative* flag.

It is recommended that you leave the default setting here:

```xml
<property>
    <name>fs.s3a.metadatastore.authoritative</name>
    <value>false</value>
</property>
```

Setting this to `true` is currently an experimental feature.  When true, the
S3A client will avoid round-trips to S3 when getting directory listings, if
there is a fully-cached version of the directory stored in the Metadata Store.

Note that if this is set to true, it may exacerbate or persist existing race
conditions around multiple concurrent modifications and listings of a given
directory tree.

In particular: **If the Metadata Store is declared as authoritative,
all interactions with the S3 bucket(s) must be through S3A clients sharing
the same Metadata Store**


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

It is good to share a table across multiple buckets for multiple reasons.

1. You are billed for the I/O capacity allocated to the table,
*even when the table is not used*. Sharing capacity can reduce costs.

1. You can share the "provision burden" across the buckets. That is, rather
than allocating for the peak load on a single bucket, you can allocate for
the peak load *across all the buckets*, which is likely to be significantly
lower.

1. It's easier to measure and tune the load requirements and cost of
S3Guard, because there is only one table to review and configure in the
AWS management console.

When wouldn't you want to share a table?

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

### 7. If creating a table: Set your DynamoDB IO Capacity

Next, you need to set the DynamoDB read and write throughput requirements you
expect to need for your cluster.  Setting higher values will cost you more
money.  *Note* that these settings only affect table creation when
`fs.s3a.s3guard.ddb.table.create` is enabled.  To change the throughput for
an existing table, use the AWS console or CLI tool.

For more details on DynamoDB capacity units, see the AWS page on [Capacity
Unit Calculations](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#CapacityUnitCalculations).

The charges are incurred per hour for the life of the table, *even when the
table and the underlying S3 buckets are not being used*.

There are also charges incurred for data storage and for data IO outside of the
region of the DynamoDB instance. S3Guard only stores metadata in DynamoDB: path names
and summary details of objects —the actual data is stored in S3, so billed at S3
rates.

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table.capacity.read</name>
  <value>500</value>
  <description>
    Provisioned throughput requirements for read operations in terms of capacity
    units for the DynamoDB table.  This config value will only be used when
    creating a new DynamoDB table, though later you can manually provision by
    increasing or decreasing read capacity as needed for existing tables.
    See DynamoDB documents for more information.
  </description>
</property>

<property>
  <name>fs.s3a.s3guard.ddb.table.capacity.write</name>
  <value>100</value>
  <description>
    Provisioned throughput requirements for write operations in terms of
    capacity units for the DynamoDB table.  Refer to related config
    fs.s3a.s3guard.ddb.table.capacity.read before usage.
  </description>
</property>
```

Attempting to perform more IO than the capacity requested simply throttles the
IO; small capacity numbers are recommended when initially experimenting
with S3Guard.

## Authenticating with S3Guard

The DynamoDB metadata store takes advantage of the fact that the DynamoDB
service uses the same authentication mechanisms as S3. S3Guard
gets all its credentials from the S3A client that is using it.

All existing S3 authentication mechanisms can be used, except for one
exception. Credentials placed in URIs are not supported for S3Guard, for security
reasons.

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

First, we define shortcuts for the metadata store classnames


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
metadata store

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.metadatastore.impl</name>
  <value>${s3guard.null}</value>
  <description>The read-only landsat-pds repository isn't
  managed by S3Guard</description>
</property>
```

Next the `ireland-2` and `ireland-offline` buckets are configured with
DynamoDB as the store, and a shared table `production-table`


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
pertaining to [Provisioned Throughput](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html):

```bash
[-write PROVISIONED_WRITES] [-read PROVISIONED_READS]
```

Example 1

```bash
hadoop s3guard init -meta dynamodb://ireland-team -write 5 -read 10 s3a://ireland-1
```

Creates a table "ireland-team" with a capacity of 5 for writes, 10 for reads,
in the same location as the bucket "ireland-1".


Example 2

```bash
hadoop s3guard init -meta dynamodb://ireland-team -region eu-west-1
```

Creates a table "ireland-team" in the region "eu-west-1.amazonaws.com"


### Import a bucket: `s3guard import`

```bash
hadoop s3guard import [-meta URI] s3a://BUCKET
```

Pre-populates a metadata store according to the current contents of an S3
bucket. If the `-meta` option is omitted, the binding information is taken
from the `core-site.xml` configuration.

Example

```bash
hadoop s3guard import s3a://ireland-1
```

### Audit a table: `s3guard diff`

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
hadoop s3guard bucket-info [ -guarded ] [-unguarded] [-auth] [-nonauth] [-magic] [-encryption ENCRYPTION] s3a://BUCKET
```

Options

| argument | meaning |
|-----------|-------------|
| `-guarded` | Require S3Guard to be enabled |
| `-unguarded` | Require S3Guard to be disabled |
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
  Endpoint: fs.s3a.endpoint=s3-eu-west-1.amazonaws.com
  Encryption: fs.s3a.server-side-encryption-algorithm=none
  Input seek policy: fs.s3a.experimental.input.fadvise=normal
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
setttings of the current client.

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
    [-seconds SECONDS] [-m URI] ( -region REGION | s3a://BUCKET )
```

A time value of hours, minutes and/or seconds must be supplied.

1. This does not delete the entries in the bucket itself.
1. The modification time is effectively the creation time of the objects
in the S3 Bucket.
1. Even when an S3A URI is supplied, all entries in the table older than
a specific age are deleted &mdash; even those from other buckets.

Example

```bash
hadoop s3guard prune -days 7 s3a://ireland-1
```

Deletes all entries in the S3Guard table for files older than seven days from
the table associated with `s3a://ireland-1`.

```bash
hadoop s3guard prune -hours 1 -minutes 30 -meta dynamodb://ireland-team -region eu-west-1
```

Delete all entries more than 90 minutes old from the table "ireland-team" in
the region "eu-west-1".


### Tune the IO capacity of the DynamoDB Table, `s3guard set-capacity`

Alter the read and/or write capacity of a s3guard table.

```bash
hadoop s3guard set-capacity [--read UNIT] [--write UNIT] ( -region REGION | s3a://BUCKET )
```

The `--read` and `--write` units are those of `s3guard init`.


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
the capacity has been changed

Repeating the same command will not change the capacity, as both read and
write values match that already in use

```
2017-08-30 16:24:35,337 [main] INFO  s3guard.DynamoDBMetadataStore (DynamoDBMetadataStore.java:updateParameters(1090)) - Table capacity unchanged at read: 20, write: 20
Metadata Store Diagnostics:
  ARN=arn:aws:dynamodb:eu-west-1:00000000000:table/ireland-1
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

### Failure Semantics

Operations which modify metadata will make changes to S3 first. If, and only
if, those operations succeed, the equivalent changes will be made to the
Metadata Store.

These changes to S3 and Metadata Store are not fully-transactional:  If the S3
operations succeed, and the subsequent Metadata Store updates fail, the S3
changes will *not* be rolled back.  In this case, an error message will be
logged.

### Versioning

S3Guard tables are created with a version marker, an entry with the primary
key and child entry of `../VERSION`; the use of a relative path guarantees
that it will not be resolved.

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

The IO load of clients of the (shared) DynamoDB table was exceeded.

Currently S3Guard doesn't do any throttling and retries here; the way to address
this is to increase capacity via the AWS console or the `set-capacity` command.

## Other Topis

For details on how to test S3Guard, see [Testing S3Guard](./testing.html#s3guard)
