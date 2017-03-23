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

*S3Guard* is an experimental feature for the S3A client of the S3 Filesystem,
which can use a (consistent) database as the store of metadata about objects
in an S3 bucket.

S3Guard

1. Increases performances on all directory listing/scanning operations, including
those which take place during the partitioning period of query execution, the
process where files are listed and the work divided up amongst processes.

1. Permits a consistent view of the object store. Without this, changes in
objects may not be immediately visible, especially in listing operations.

1. Create a platform for future performance improvements for running Hadoop
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


## Configuring S3Guard

The latest configuration parameters are defined in `core-default.xml`.  You
should consult that file for full information, but a summary is provided here.


### 1. Choose your MetadataStore implementation.

By default, S3Guard is not enabled.  S3A uses "`NullMetadataStore`", which is a
MetadataStore that does nothing.

The funtional MetadataStore back-end uses Amazon's DynamoDB database service.  The
 following setting will enable this MetadataStore:

```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value>org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore</value>
</property>
```


Note that the Null Metadata store can be explicitly requested if desired.

```xml
<property>
    <name>fs.s3a.metadatastore.impl</name>
    <value>org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore</value>
</property>
```

### 2. Configure S3Guard settings

More settings will be added here in the future as we add to S3Guard.
Currently the only MetadataStore-independent setting, besides the
implementation class above, is the *allow authoritative* flag.

It is recommended that you leave the default setting here:

```xml
<property>
    <name>fs.s3a.metadatastore.authoritative</name>
    <value>false</value>
</property>

```

Setting this to true is currently an experimental feature.  When true, the
S3A client will avoid round-trips to S3 when getting directory listings, if
there is a fully-cached version of the directory stored in the MetadataStore.

Note that if this is set to true, it may exacerbate or persist existing race
conditions around multiple concurrent modifications and listings of a given
directory tree.


### 3. Configure the MetadataStore.

Here are the `DynamoDBMetadataStore` settings.  Other MetadataStore
 implementations will have their own configuration parameters.

First, choose the name of the table you wish to use for the S3Guard metadata
storage in your DynamoDB instance.  If you leave the default blank value, a
separate table will be created for each S3 bucket you access, and that
bucket's name will be used for the name of the DynamoDB table.  Here we
choose our own table name:

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table</name>
  <value>my-ddb-table-name</value>
  <description>
    The DynamoDB table name to operate. Without this property, the respective
    S3 bucket name will be used.
  </description>
</property>
```

You may also wish to specify the endpoint to use for DynamoDB. In AWS, the
endpoint should be matched to the region the table lives in. If an endpoint
is not configured, it will be assume that it is in the same region as the S3
bucket. A list of regions and endpoints for the DynamoDB service can be found in
[Amazon's documentation](http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region).

```xml
<property>
  <name>fs.s3a.s3guard.ddb.endpoint</name>
  <value>dynamodb.us-west-1.amazonaws.com</value>
  <description>
    Endpoint to use for DynamoDB requests. The endpoint should be matched to the
    region the metastore database will be in.
  </description>
</property>
```

Next, you can choose whether or not the table will be automatically created
(if it doesn't already exist).  If we want this feature, we can set the
following parameter to true.

```xml
<property>
  <name>fs.s3a.s3guard.ddb.table.create</name>
  <value>true</value>
  <description>
    If true, the S3A client will create the table if it does not already exist.
  </description>
</property>
```

We can also explicitly set the DynamoDB service endpoint we will connect to.
This makes sure we always access the region our table lives in, even if we
access S3 buckets in other regions.  In this example we use the endpoint for
US West (Oregon):

```xml
<property>
  <name>fs.s3a.s3guard.ddb.endpoint</name>
  <value>dynamodb.us-west-2.amazonaws.com</value>
</property>
```
Next, you need to set the DynamoDB read and write throughput requirements you
expect to need for your cluster.  Setting higher values will cost you more
money.
For more details on DynamoDB capacity units, see the AWS page on [Capacity
Unit Calculations](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#CapacityUnitCalculations).

The charges are incurred per hour for the life of the table, even when the
table and the underlying S3 bucket are not being used.

There are also charges incurred for data storage and  for data IO outside of the
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

## Credentials

The DynamoDB metadata store takes advantage of the fact that the DynamoDB
service uses uses the same authentication mechanisms as S3. With S3Guard,
DynamoDB doesn't have any dedicated authentication configuration; it gets its
credentials from the S3A client that is using it.

The existing S3 authentication mechanisms can be used, except for one
exception. Credentials placed in URIs are not supported for S3Guard.  The
reason is that providing login details in filesystem URIs is considered
unsafe and thus deprecated.

## S3Guard Command Line Interface (CLI)

Note that in some cases an endpoint or a s3a:// URI can be provided.

Metadata store URIs include a scheme that designates the backing store. For
example (e.g. dynamodb://&lt;table_name&gt;). As documented above, endpoints can
be inferred if the URI to an existing bucket is provided.

### Init

```
hadoop s3a init -meta URI ( -endpoint ENDPOINT | s3a://BUCKET )
```

Creates and initializes an empty metadata store.

A DynamoDB metadata store can be initialized with additional parameters
pertaining to [Provisioned Throughput](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html):

```
[-write PROVISIONED_WRITES] [-read PROVISIONED_READS]
```

### Import

```
hadoop s3a import [-meta URI] s3a://BUCKET
```

Pre-populates a metadata store according to the current contents of an S3
bucket.

### Diff

```
hadoop s3a diff [-meta URI] s3a://BUCKET
```

Lists discrepancies between a metadata store and bucket. Note that depending on
how S3Guard is used, certain discrepancies are to be expected.

### Destroy

```
hadoop s3a destroy [-meta URI] ( -endpoint ENDPOINT | s3a://BUCKET )
```

Deletes a metadata store.

### Prune

```
hadoop s3a prune [-days DAYS] [-hours HOURS] [-minutes MINUTES]
    [-seconds SECONDS] [-m URI] ( -endpoint ENDPOINT | s3a://BUCKET )
```

Trims metadata for files that are older than the time given. Must supply at least length of time.

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

# Enable debug logging of AWS Dynamo client
log4j.logger.com.amazonaws.services.dynamodbv2.AmazonDynamoDB

# Log all HTTP requests made; includes S3 interaction. This may
# include sensitive information such as account IDs in HTTP headers.
log4j.logger.com.amazonaws.request=DEBUG

```

If all else fails, S3Guard is designed to allow for easy recovery by deleting
your metadata store data.  In DynamoDB, this can be accomplished by simply
deleting the table, and allowing S3Guard to recreate it from scratch.  Note
that S3Guard tracks recent changes to file metadata to implement consistency.
Deleting the metadata store table will simply result in a period of eventual
consistency for any file modifications that were made right before the table
was deleted.

### Failure Semantics

Operations which modify metadata will make changes to S3 first. If, and only
if, those operations succeed, the equivalent changes will be made to the
MetadataStore.

These changes to S3 and MetadataStore are not fully-transactional:  If the S3
operations succeed, and the subsequent MetadataStore updates fail, the S3
changes will *not* be rolled back.  In this case, an error message will be
logged.

### Versioning

S3Guard tables are created with a version marker, an entry with the primary
key and child entry of `../VERSION`; the use of a relative path guarantees
that it will not be resolved.

#### Versioning policy.

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

#### Error: `S3Guard table lacks version marker.`

The table which was intended to be used as a S3guard metadata store
does not have any version marker indicating that it is a S3Guard table.

It may be that this is not a S3Guard table.

* Make sure that this is the correct table name.
* Delete the table, so it can be rebuilt.

#### Error: `Database table is from an incompatible S3Guard version`

This indicates that the version of S3Guard which created (or possibly updated)
the database table is from a different version that that expected by the S3A
client.

This error will also include the expected and actual version numbers.

If the expected version is lower than the actual version, then the version
of the S3A client library is too old to interact with this S3Guard-managed
bucket. Upgrade the application/library.

If the expected version is higher than the actual version, then the table
itself will need upgrading.

## Testing S3Guard

The basic strategy for testing S3Guard correctness consists of:

1. MetadataStore Contract tests.

    The MetadataStore contract tests are inspired by the Hadoop FileSystem and
    FileContext contract tests.  Each implementation of the MetadataStore interface
    subclasses the `MetadataStoreTestBase` class and customizes it to initialize
    their MetadataStore.  This test ensures that the different implementations
    all satisfy the semantics of the MetadataStore API.

2. Running existing S3A unit and integration tests with S3Guard enabled.

    You can run the S3A integration tests on top of S3Guard by configuring your
    MetadataStore (as documented above) in your
    `hadoop-tools/hadoop-aws/src/test/resources/core-site.xml` or
    `hadoop-tools/hadoop-aws/src/test/resources/auth-keys.xml` files.
    Next run the S3A integration tests as outlined in the *Running the Tests* section
    of the [S3A documentation](./index.html)

3. Running fault-injection tests that test S3Guard's consistency features.

    The `ITestS3GuardListConsistency` uses failure injection to ensure
    that list consistency logic is correct even when the underlying storage is
    eventually consistent.

    The integration test adds a shim above the Amazon S3 Client layer that injects
    delays in object visibility.

    All of these tests will be run if you follow the steps listed in step 2 above.

    No charges are incurred for using this store, and its consistency
    guarantees are that of the underlying object store instance. <!-- :) -->

## Testing S3 with S3Guard Enabled

All the S3A tests which work with a private repository can be configured to
run with S3Guard by using the `s3guard` profile. When set, this will run
all the tests with local memory for the metadata set to "non-authoritative" mode.

```bash
mvn -T 1C verify -Dparallel-tests -DtestsThreadCount=6 -Ds3guard 
```

When the `s3guard` profile is enabled, following profiles can be specified:

* `dynamo`: use an AWS-hosted DynamoDB table; creating the table if it does
  not exist. You will have to pay the bills for DynamoDB web service.
* `dynamodblocal`: use an in-memory DynamoDBLocal server instead of real AWS
  DynamoDB web service; launch the server if it is not yet started; creating the
  table if it does not exist. You won't be charged bills for using DynamoDB in
  test. However, the DynamoDBLocal is a simulator of real AWS DynamoDB and is
  maintained separately, so it may be stale.
* `non-auth`: treat the s3guard metadata as authorative

```bash
mvn -T 1C verify -Dparallel-tests -DtestsThreadCount=6 -Ds3guard -Ddynamo -Dauth 
```

When experimenting with options, it is usually best to run a single test suite
at a time until the operations appear to be working.

```bash
mvn -T 1C verify -Dtest=skip -Dit.test=ITestS3AMiscOperations -Ds3guard -Ddynamo
```

Notes

1. If the `s3guard` profile is not set, then the s3guard properties are those
of the test configuration set in `contract-test-options.xml` or `auth-keys.xml`

If the `s3guard` profile *is* set, 
1. The s3guard options from maven (the dynamo and authoritative flags)
  overwrite any previously set. in the configuration files.
1. Dynamo will be configured to create any missing tables.
1. 


### Scale Testing MetadataStore Directly

We also have some scale tests that exercise MetadataStore implementations
directly.  These allow us to ensure were are robust to things like DynamoDB
throttling, and compare performance for different implementations. See the
main [S3A documentation](./index.html) for more details on how to enable the
S3A scale tests.

The two scale tests here are `ITestDynamoDBMetadataStoreScale` and
`ITestLocalMetadataStoreScale`.  To run the DynamoDB test, you will need to
define your table name and endpoint in your test configuration.  For example,
the following settings allow us to run `ITestDynamoDBMetadataStoreScale` with
artificially low read and write capacity provisioned, so we can judge the
effects of being throttled by the DynamoDB service:

```
<property>
    <name>scale.test.operation.count</name>
    <value>10</value>
</property>
<property>
    <name>scale.test.directory.count</name>
    <value>3</value>
</property>
<property>
    <name>fs.s3a.scale.test.enabled</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.s3guard.ddb.table</name>
    <value>my-scale-test</value>
</property>
<property>
    <name>fs.s3a.s3guard.ddb.endpoint</name>
    <value>dynamodb.us-west-2.amazonaws.com</value>
</property>
<property>
    <name>fs.s3a.s3guard.ddb.table.create</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.s3guard.ddb.table.capacity.read</name>
    <value>10</value>
</property>
<property>
    <name>fs.s3a.s3guard.ddb.table.capacity.write</name>
    <value>10</value>
</property>
```

### Testing only: Local Metadata Store

There is an in-memory metadata store for testing.

```xml
<property>
  <name>fs.s3a.metadatastore.impl</name>
  <value>org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore</value>
</property>
```
