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

# Testing the S3A filesystem client and its features, including S3Guard

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

This module includes both unit tests, which can run in isolation without
connecting to the S3 service, and integration tests, which require a working
connection to S3 to interact with a bucket.  Unit test suites follow the naming
convention `Test*.java`.  Integration tests follow the naming convention
`ITest*.java`.

Due to eventual consistency, integration tests may fail without reason.
Transient failures, which no longer occur upon rerunning the test, should thus
be ignored.

## <a name="policy"></a> Policy for submitting patches which affect the `hadoop-aws` module.

The Apache Jenkins infrastructure does not run any S3 integration tests,
due to the need to keep credentials secure.

### The submitter of any patch is required to run all the integration tests and declare which S3 region/implementation they used.

This is important: **patches which do not include this declaration will be ignored**

This policy has proven to be the only mechanism to guarantee full regression
testing of code changes. Why the declaration of region? Two reasons

1. It helps us identify regressions which only surface against specific endpoints
or third-party implementations of the S3 protocol.
1. It forces the submitters to be more honest about their testing. It's easy
to lie, "yes, I tested this". To say "yes, I tested this against S3 US-west"
is a more specific lie and harder to make. And, if you get caught out: you
lose all credibility with the project.

You don't need to test from a VM within the AWS infrastructure; with the
`-Dparallel=tests` option the non-scale tests complete in under ten minutes.
Because the tests clean up after themselves, they are also designed to be low
cost. It's neither hard nor expensive to run the tests; if you can't,
there's no guarantee your patch works. The reviewers have enough to do, and
don't have the time to do these tests, especially as every failure will simply
make for a slow iterative development.

Please: run the tests. And if you don't, we are sorry for declining your
patch, but we have to.


### What if there's an intermittent failure of a test?

Some of the tests do fail intermittently, especially in parallel runs.
If this happens, try to run the test on its own to see if the test succeeds.

If it still fails, include this fact in your declaration. We know some tests
are intermittently unreliable.

### What if the tests are timing out or failing over my network connection?

The tests and the S3A client are designed to be configurable for different
timeouts. If you are seeing problems and this configuration isn't working,
that's a sign of the configuration mechanism isn't complete. If it's happening
in the production code, that could be a sign of a problem which may surface
over long-haul connections. Please help us identify and fix these problems
&mdash; especially as you are the one best placed to verify the fixes work.

## <a name="setting-up"></a> Setting up the tests

To integration test the S3* filesystem clients, you need to provide
`auth-keys.xml` which passes in authentication details to the test runner.

It is a Hadoop XML configuration file, which must be placed into
`hadoop-tools/hadoop-aws/src/test/resources`.

### File `core-site.xml`

This file pre-exists and sources the configurations created
under `auth-keys.xml`.

For most purposes you will not need to edit this file unless you
need to apply a specific, non-default property change during the tests.

### File `auth-keys.xml`

The presence of this file triggers the testing of the S3 classes.

Without this file, *none of the integration tests in this module will be
executed*.

The XML file must contain all the ID/key information needed to connect
each of the filesystem clients to the object stores, and a URL for
each filesystem for its testing.

1. `test.fs.s3a.name` : the URL of the bucket for S3a tests
1. `fs.contract.test.fs.s3a` : the URL of the bucket for S3a filesystem contract tests


The contents of the bucket will be destroyed during the test process:
do not use the bucket for any purpose other than testing. Furthermore, for
s3a, all in-progress multi-part uploads to the bucket will be aborted at the
start of a test (by forcing `fs.s3a.multipart.purge=true`) to clean up the
temporary state of previously failed tests.

Example:

```xml
<configuration>

  <property>
    <name>test.fs.s3a.name</name>
    <value>s3a://test-aws-s3a/</value>
  </property>

  <property>
    <name>fs.contract.test.fs.s3a</name>
    <value>${test.fs.s3a.name}</value>
  </property>

  <property>
    <name>fs.s3a.access.key</name>
    <description>AWS access key ID. Omit for IAM role-based authentication.</description>
    <value>DONOTCOMMITTHISKEYTOSCM</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <description>AWS secret key. Omit for IAM role-based authentication.</description>
    <value>DONOTEVERSHARETHISSECRETKEY!</value>
  </property>

  <property>
    <name>test.sts.endpoint</name>
    <description>Specific endpoint to use for STS requests.</description>
    <value>sts.amazonaws.com</value>
  </property>

</configuration>
```

### <a name="encryption"></a> Configuring S3a Encryption

For S3a encryption tests to run correctly, the
`fs.s3a.server-side-encryption.key` must be configured in the s3a contract xml
file with a AWS KMS encryption key arn as this value is different for each AWS
KMS.

Example:

```xml
<property>
  <name>fs.s3a.server-side-encryption.key</name>
  <value>arn:aws:kms:us-west-2:360379543683:key/071a86ff-8881-4ba0-9230-95af6d01ca01</value>
</property>
```

You can also force all the tests to run with a specific SSE encryption method
by configuring the property `fs.s3a.server-side-encryption-algorithm` in the s3a
contract file.

## <a name="running"></a> Running the Tests

After completing the configuration, execute the test run through Maven.

```bash
mvn clean verify
```

It's also possible to execute multiple test suites in parallel by passing the
`parallel-tests` property on the command line.  The tests spend most of their
time blocked on network I/O with the S3 service, so running in parallel tends to
complete full test runs faster.

```bash
mvn -Dparallel-tests clean verify
```

Some tests must run with exclusive access to the S3 bucket, so even with the
`parallel-tests` property, several test suites will run in serial in a separate
Maven execution step after the parallel tests.

By default, `parallel-tests` runs 4 test suites concurrently.  This can be tuned
by passing the `testsThreadCount` property.

```bash
mvn -Dparallel-tests -DtestsThreadCount=8 clean verify
```

To run just unit tests, which do not require S3 connectivity or AWS credentials,
use any of the above invocations, but switch the goal to `test` instead of
`verify`.

```bash
mvn clean test

mvn -Dparallel-tests clean test

mvn -Dparallel-tests -DtestsThreadCount=8 clean test
```

To run only a specific named subset of tests, pass the `test` property for unit
tests or the `it.test` property for integration tests.

```bash
mvn clean test -Dtest=TestS3AInputPolicies

mvn clean verify -Dit.test=ITestS3AFileContextStatistics -Dtest=none

mvn clean verify -Dtest=TestS3A* -Dit.test=ITestS3A*
```

Note that when running a specific subset of tests, the patterns passed in `test`
and `it.test` override the configuration of which tests need to run in isolation
in a separate serial phase (mentioned above).  This can cause unpredictable
results, so the recommendation is to avoid passing `parallel-tests` in
combination with `test` or `it.test`.  If you know that you are specifying only
tests that can run safely in parallel, then it will work.  For wide patterns,
like `ITestS3A*` shown above, it may cause unpredictable test failures.

### <a name="regions"></a> Testing against different regions

S3A can connect to different regions —the tests support this. Simply
define the target region in `auth-keys.xml`.

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>
```
This is used for all tests expect for scale tests using a Public CSV.gz file
(see below)

### <a name="csv"></a> CSV Data Tests

The `TestS3AInputStreamPerformance` tests require read access to a multi-MB
text file. The default file for these tests is one published by amazon,
[s3a://landsat-pds.s3.amazonaws.com/scene_list.gz](http://landsat-pds.s3.amazonaws.com/scene_list.gz).
This is a gzipped CSV index of other files which amazon serves for open use.

The path to this object is set in the option `fs.s3a.scale.test.csvfile`,

```xml
<property>
  <name>fs.s3a.scale.test.csvfile</name>
  <value>s3a://landsat-pds/scene_list.gz</value>
</property>
```

1. If the option is not overridden, the default value is used. This
is hosted in Amazon's US-east datacenter.
1. If `fs.s3a.scale.test.csvfile` is empty, tests which require it will be skipped.
1. If the data cannot be read for any reason then the test will fail.
1. If the property is set to a different path, then that data must be readable
and "sufficiently" large.

(the reason the space or newline is needed is to add "an empty entry"; an empty
`<value/>` would be considered undefined and pick up the default)

Of using a test file in an S3 region requiring a different endpoint value
set in `fs.s3a.endpoint`, a bucket-specific endpoint must be defined.
For the default test dataset, hosted in the `landsat-pds` bucket, this is:

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.endpoint</name>
  <value>s3.amazonaws.com</value>
  <description>The endpoint for s3a://landsat-pds URLs</description>
</property>
```

## <a name="reporting"></a> Viewing Integration Test Reports


Integration test results and logs are stored in `target/failsafe-reports/`.
An HTML report can be generated during site generation, or with the `surefire-report`
plugin:

```bash
mvn surefire-report:failsafe-report-only
```
## <a name="scale"></a> Scale Tests

There are a set of tests designed to measure the scalability and performance
at scale of the S3A tests, *Scale Tests*. Tests include: creating
and traversing directory trees, uploading large files, renaming them,
deleting them, seeking through the files, performing random IO, and others.
This makes them a foundational part of the benchmarking.

By their very nature they are slow. And, as their execution time is often
limited by bandwidth between the computer running the tests and the S3 endpoint,
parallel execution does not speed these tests up.

***Note: Running scale tests with -Ds3guard and -Ddynamo requires that
you use a private, testing-only DynamoDB table.*** The tests do disruptive
things such as deleting metadata and setting the provisioned throughput
to very low values.

### <a name="enabling-scale"></a> Enabling the Scale Tests

The tests are enabled if the `scale` property is set in the maven build
this can be done regardless of whether or not the parallel test profile
is used

```bash
mvn verify -Dscale

mvn verify -Dparallel-tests -Dscale -DtestsThreadCount=8
```

The most bandwidth intensive tests (those which upload data) always run
sequentially; those which are slow due to HTTPS setup costs or server-side
actions are included in the set of parallelized tests.


### <a name="tuning_scale"></a> Tuning scale options from Maven


Some of the tests can be tuned from the maven build or from the
configuration file used to run the tests.

```bash
mvn verify -Dparallel-tests -Dscale -DtestsThreadCount=8 -Dfs.s3a.scale.test.huge.filesize=128M
```

The algorithm is

1. The value is queried from the configuration file, using a default value if
it is not set.
1. The value is queried from the JVM System Properties, where it is passed
down by maven.
1. If the system property is null, an empty string, or it has the value `unset`,
then the configuration value is used. The `unset` option is used to
[work round a quirk in maven property propagation](http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven).

Only a few properties can be set this way; more will be added.

| Property | Meaning |
|-----------|-------------|
| `fs.s3a.scale.test.timeout`| Timeout in seconds for scale tests |
| `fs.s3a.scale.test.huge.filesize`| Size for huge file uploads |
| `fs.s3a.scale.test.huge.huge.partitionsize`| Size for partitions in huge file uploads |

The file and partition sizes are numeric values with a k/m/g/t/p suffix depending
on the desired size. For example: 128M, 128m, 2G, 2G, 4T or even 1P.

### <a name="scale-config"></a> Scale test configuration options

Some scale tests perform multiple operations (such as creating many directories).

The exact number of operations to perform is configurable in the option
`scale.test.operation.count`

```xml
<property>
  <name>scale.test.operation.count</name>
  <value>10</value>
</property>
```

Larger values generate more load, and are recommended when testing locally,
or in batch runs.

Smaller values results in faster test runs, especially when the object
store is a long way away.

Operations which work on directories have a separate option: this controls
the width and depth of tests creating recursive directories. Larger
values create exponentially more directories, with consequent performance
impact.

```xml
<property>
  <name>scale.test.directory.count</name>
  <value>2</value>
</property>
```

DistCp tests targeting S3A support a configurable file size.  The default is
10 MB, but the configuration value is expressed in KB so that it can be tuned
smaller to achieve faster test runs.

```xml
<property>
  <name>scale.test.distcp.file.size.kb</name>
  <value>10240</value>
</property>
```

S3A specific scale test properties are

*`fs.s3a.scale.test.huge.filesize`: size in MB for "Huge file tests".*

The Huge File tests validate S3A's ability to handle large files —the property
`fs.s3a.scale.test.huge.filesize` declares the file size to use.

```xml
<property>
  <name>fs.s3a.scale.test.huge.filesize</name>
  <value>200M</value>
</property>
```

Amazon S3 handles files larger than 5GB differently than smaller ones.
Setting the huge filesize to a number greater than that) validates support
for huge files.

```xml
<property>
  <name>fs.s3a.scale.test.huge.filesize</name>
  <value>6G</value>
</property>
```

Tests at this scale are slow: they are best executed from hosts running in
the cloud infrastructure where the S3 endpoint is based.
Otherwise, set a large timeout in `fs.s3a.scale.test.timeout`

```xml
<property>
  <name>fs.s3a.scale.test.timeout</name>
  <value>432000</value>
</property>
```

The tests are executed in an order to only clean up created files after
the end of all the tests. If the tests are interrupted, the test data will remain.


## <a name="alternate_s3"></a> Testing against non AWS S3 endpoints.

The S3A filesystem is designed to work with storage endpoints which implement
the S3 protocols to the extent that the amazon S3 SDK is capable of talking
to it. We encourage testing against other filesystems and submissions of patches
which address issues. In particular, we encourage testing of Hadoop release
candidates, as these third-party endpoints get even less testing than the
S3 endpoint itself.


### Disabling the encryption tests

If the endpoint doesn't support server-side-encryption, these will fail. They
can be turned off.

```xml
<property>
  <name>test.fs.s3a.encryption.enabled</name>
  <value>false</value>
</property>
```

Encryption is only used for those specific test suites with `Encryption` in
their classname.

### Configuring the CSV file read tests**

To test on alternate infrastructures supporting
the same APIs, the option `fs.s3a.scale.test.csvfile` must either be
set to " ", or an object of at least 10MB is uploaded to the object store, and
the `fs.s3a.scale.test.csvfile` option set to its path.

```xml
<property>
  <name>fs.s3a.scale.test.csvfile</name>
  <value> </value>
</property>
```

(yes, the space is necessary. The Hadoop `Configuration` class treats an empty
value as "do not override the default").


### Testing Session Credentials

The test `TestS3ATemporaryCredentials` requests a set of temporary
credentials from the STS service, then uses them to authenticate with S3.

If an S3 implementation does not support STS, then the functional test
cases must be disabled:

```xml
<property>
  <name>test.fs.s3a.sts.enabled</name>
  <value>false</value>
</property>
```
These tests request a temporary set of credentials from the STS service endpoint.
An alternate endpoint may be defined in `test.fs.s3a.sts.endpoint`.

```xml
<property>
  <name>test.fs.s3a.sts.endpoint</name>
  <value>https://sts.example.org/</value>
</property>
```
The default is ""; meaning "use the amazon default value".


## <a name="debugging"></a> Debugging Test failures

Logging at debug level is the standard way to provide more diagnostics output;
after setting this rerun the tests

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
```

There are also some logging options for debug logging of the AWS client
```properties
log4j.logger.com.amazonaws=DEBUG
log4j.logger.com.amazonaws.http.conn.ssl=INFO
log4j.logger.com.amazonaws.internal=INFO
```

There is also the option of enabling logging on a bucket; this could perhaps
be used to diagnose problems from that end. This isn't something actively
used, but remains an option. If you are forced to debug this way, consider
setting the `fs.s3a.user.agent.prefix` to a unique prefix for a specific
test run, which will enable the specific log entries to be more easily
located.

## <a name="new_tests"></a> Adding new tests

New tests are always welcome. Bear in mind that we need to keep costs
and test time down, which is done by

* Not duplicating tests.
* Being efficient in your use of Hadoop API calls.
* Isolating large/slow tests into the "scale" test group.
* Designing all tests to execute in parallel (where possible).
* Adding new probes and predicates into existing tests, albeit carefully.

*No duplication*: if an operation is tested elsewhere, don't repeat it. This
applies as much for metadata operations as it does for bulk IO. If a new
test case is added which completely obsoletes an existing test, it is OK
to cut the previous one —after showing that coverage is not worsened.

*Efficient*: prefer the `getFileStatus()` and examining the results, rather than
call to `exists()`, `isFile()`, etc.

*Isolating Scale tests*. Any S3A test doing large amounts of IO MUST extend the
class `S3AScaleTestBase`, so only running if `scale` is defined on a build,
supporting test timeouts configurable by the user. Scale tests should also
support configurability as to the actual size of objects/number of operations,
so that behavior at different scale can be verified.

*Designed for parallel execution*. A key need here is for each test suite to work
on isolated parts of the filesystem. Subclasses of `AbstractS3ATestBase`
SHOULD use the `path()` method, with a base path of the test suite name, to
build isolated paths. Tests MUST NOT assume that they have exclusive access
to a bucket.

*Extending existing tests where appropriate*. This recommendation goes
against normal testing best practise of "test one thing per method".
Because it is so slow to create directory trees or upload large files, we do
not have that luxury. All the tests against real S3 endpoints are integration
tests where sharing test setup and teardown saves time and money.

A standard way to do this is to extend existing tests with some extra predicates,
rather than write new tests. When doing this, make sure that the new predicates
fail with meaningful diagnostics, so any new problems can be easily debugged
from test logs.


## <a name="requirements"></a> Requirements of new Tests


This is what we expect from new tests; they're an extension of the normal
Hadoop requirements, based on the need to work with remote servers whose
use requires the presence of secret credentials, where tests may be slow,
and where finding out why something failed from nothing but the test output
is critical.

### Subclasses Existing Shared Base Classes

Extend `AbstractS3ATestBase` or `AbstractSTestS3AHugeFiles` unless justifiable.
These set things up for testing against the object stores, provide good threadnames,
help generate isolated paths, and for `AbstractSTestS3AHugeFiles` subclasses,
only run if `-Dscale` is set.

Key features of `AbstractS3ATestBase`

* `getFileSystem()` returns the S3A Filesystem bonded to the contract test Filesystem
defined in `fs.s3a.contract.test`
* will automatically skip all tests if that URL is unset.
* Extends  `AbstractFSContractTestBase` and `Assert` for all their methods.

Having shared base classes may help reduce future maintenance too. Please
use them/

### Secure

Don't ever log credentials. The credential tests go out of their way to
not provide meaningful logs or assertion messages precisely to avoid this.

### Efficient of Time and Money

This means efficient in test setup/teardown, and, ideally, making use of
existing public datasets to save setup time and tester cost.

Strategies of particular note are:

1. `ITestS3ADirectoryPerformance`: a single test case sets up the directory
tree then performs different list operations, measuring the time taken.
1. `AbstractSTestS3AHugeFiles`: marks the test suite as
`@FixMethodOrder(MethodSorters.NAME_ASCENDING)` then orders the test cases such
that each test case expects the previous test to have completed (here: uploaded a file,
renamed a file, ...). This provides for independent tests in the reports, yet still
permits an ordered sequence of operations. Do note the use of `Assume.assume()`
to detect when the preconditions for a single test case are not met, hence,
the tests become skipped, rather than fail with a trace which is really a false alarm.

The ordered test case mechanism of `AbstractSTestS3AHugeFiles` is probably
the most elegant way of chaining test setup/teardown.

Regarding reusing existing data, we tend to use the landsat archive of
AWS US-East for our testing of input stream operations. This doesn't work
against other regions, or with third party S3 implementations. Thus the
URL can be overridden for testing elsewhere.


### Works With Other S3 Endpoints

Don't assume AWS S3 US-East only, do allow for working with external S3 implementations.
Those may be behind the latest S3 API features, not support encryption, session
APIs, etc.

They won't have the same CSV test files as some of the input tests rely on.
Look at `ITestS3AInputStreamPerformance` to see how tests can be written
to support the declaration of a specific large test file on alternate filesystems.


### Works Over Long-haul Links

As well as making file size and operation counts scalable, this includes
making test timeouts adequate. The Scale tests make this configurable; it's
hard coded to ten minutes in `AbstractS3ATestBase()`; subclasses can
change this by overriding `getTestTimeoutMillis()`.

Equally importantly: support proxies, as some testers need them.


### Provides Diagnostics and timing information

1. Give threads useful names.
1. Create logs, log things. Know that the `S3AFileSystem` and its input
and output streams *all* provide useful statistics in their {{toString()}}
calls; logging them is useful on its own.
1. you can use `AbstractS3ATestBase.describe(format-stringm, args)` here.; it
adds some newlines so as to be easier to spot.
1. Use `ContractTestUtils.NanoTimer` to measure the duration of operations,
and log the output.

### Fails Meaningfully

The `ContractTestUtils` class contains a whole set of assertions for making
statements about the expected state of a filesystem, e.g.
`assertPathExists(FS, path)`, `assertPathDoesNotExists(FS, path)`, and others.
These do their best to provide meaningful diagnostics on failures (e.g. directory
listings, file status, ...), so help make failures easier to understand.

At the very least, do not use `assertTrue()` or `assertFalse()` without
including error messages.

### Sets up its filesystem and checks for those settings

Tests can overrun `createConfiguration()` to add new options to the configuration
file for the S3A Filesystem instance used in their tests.

However, filesystem caching may mean that a test suite may get a cached
instance created with an different configuration. For tests which don't need
specific configurations caching is good: it reduces test setup time.

For those tests which do need unique options (encryption, magic files),
things can break, and they will do so in hard-to-replicate ways.

Use `S3ATestUtils.disableFilesystemCaching(conf)` to disable caching when
modifying the config. As an example from `AbstractTestS3AEncryption`:

```java
@Override
protected Configuration createConfiguration() {
  Configuration conf = super.createConfiguration();
  S3ATestUtils.disableFilesystemCaching(conf);
  conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
          getSSEAlgorithm().getMethod());
  return conf;
}
```

Then verify in the setup method or test cases that their filesystem actually has
the desired feature (`fs.getConf().getProperty(...)`). This not only
catches filesystem reuse problems, it catches the situation where the
filesystem configuration in `auth-keys.xml` has explicit per-bucket settings
which override the test suite's general option settings.

### Cleans Up Afterwards

Keeps costs down.

1. Do not only cleanup if a test case completes successfully; test suite
teardown must do it.
1. That teardown code must check for the filesystem and other fields being
null before the cleanup. Why? If test setup fails, the teardown methods still
get called.

### Works Reliably

We really appreciate this &mdash; you will too.

### Runs in parallel unless this is unworkable.

Tests must be designed to run in parallel with other tests, all working
with the same shared S3 bucket. This means

* Uses relative and JVM-fork-unique paths provided by the method
  `AbstractFSContractTestBase.path(String filepath)`.
* Doesn't manipulate the root directory or make assertions about its contents
(for example: delete its contents and assert that it is now empty).
* Doesn't have a specific requirement of all active clients of the bucket
(example: SSE-C tests which require all files, even directory markers,
to be encrypted with the same key).
* Doesn't use so much bandwidth that all other tests will be starved of IO and
start timing out (e.g. the scale tests).

Tests such as these can only be run as sequential tests. When adding one,
exclude it in the POM file. from the parallel failsafe run and add to the
sequential one afterwards. The IO heavy ones must also be subclasses of
`S3AScaleTestBase` and so only run if the system/maven property
`fs.s3a.scale.test.enabled` is true.

## Individual test cases can be run in an IDE

This is invaluable for debugging test failures.


## <a name="tips"></a> Tips

### How to keep your credentials really safe

Although the `auth-keys.xml` file is marked as ignored in git and subversion,
it is still in your source tree, and there's always that risk that it may
creep out.

You can avoid this by keeping your keys outside the source tree and
using an absolute XInclude reference to it.

```xml
<configuration>

  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///users/ubuntu/.auth-keys.xml" />

</configuration>
```

#  <a name="failure-injection"></a>Failure Injection

**Warning do not enable any type of failure injection in production.  The
following settings are for testing only.**

One of the challenges with S3A integration tests is the fact that S3 is an
eventually-consistent storage system.  In practice, we rarely see delays in
visibility of recently created objects both in listings (`listStatus()`) and
when getting a single file's metadata (`getFileStatus()`). Since this behavior
is rare and non-deterministic, thorough integration testing is challenging.

To address this, S3A supports a shim layer on top of the `AmazonS3Client`
class which artificially delays certain paths from appearing in listings.
This is implemented in the class `InconsistentAmazonS3Client`.

## Simulating List Inconsistencies

### Enabling the InconsistentAmazonS3CClient

There are two ways of enabling the `InconsistentAmazonS3Client`: at
config-time, or programmatically. For an example of programmatic test usage,
see `ITestS3GuardListConsistency`.

To enable the fault-injecting client via configuration, switch the
S3A client to use the "Inconsistent S3 Client Factory" when connecting to
S3:

```xml
<property>
  <name>fs.s3a.s3.client.factory.impl</name>
  <value>org.apache.hadoop.fs.s3a.InconsistentS3ClientFactory</value>
</property>
```

The inconsistent client works by:

1. Choosing which objects will be "inconsistent" at the time the object is
created or deleted.
2. When `listObjects()` is called, any keys that we have marked as
inconsistent above will not be returned in the results (until the
configured delay has elapsed). Similarly, deleted items may be *added* to
missing results to delay the visibility of the delete.

There are two ways of choosing which keys (filenames) will be affected: By
substring, and by random probability.

```xml
<property>
  <name>fs.s3a.failinject.inconsistency.key.substring</name>
  <value>DELAY_LISTING_ME</value>
</property>

<property>
  <name>fs.s3a.failinject.inconsistency.probability</name>
  <value>1.0</value>
</property>
```

By default, any object which has the substring "DELAY_LISTING_ME" in its key
will subject to delayed visibility. For example, the path
`s3a://my-bucket/test/DELAY_LISTING_ME/file.txt` would match this condition.
To match all keys use the value "\*" (a single asterisk). This is a special
value: *We don't support arbitrary wildcards.*

The default probability of delaying an object is 1.0. This means that *all*
keys that match the substring will get delayed visibility. Note that we take
the logical *and* of the two conditions (substring matches *and* probability
random chance occurs). Here are some example configurations:

```
| substring | probability |  behavior                                  |
|-----------|-------------|--------------------------------------------|
|           | 0.001       | An empty <value> tag in .xml config will   |
|           |             | be interpreted as unset and revert to the  |
|           |             | default value, "DELAY_LISTING_ME"          |
|           |             |                                            |
| *         | 0.001       | 1/1000 chance of *any* key being delayed.  |
|           |             |                                            |
| delay     | 0.01        | 1/100 chance of any key containing "delay" |
|           |             |                                            |
| delay     | 1.0         | All keys containing substring "delay" ..   |
```

You can also configure how long you want the delay in visibility to last.
The default is 5000 milliseconds (five seconds).

```xml
<property>
  <name>fs.s3a.failinject.inconsistency.msec</name>
  <value>5000</value>
</property>
```

Future versions of this client will introduce new failure modes,
with simulation of S3 throttling exceptions the next feature under
development.

### Limitations of Inconsistency Injection

Although `InconsistentAmazonS3Client` can delay the visibility of an object
or parent directory, it does not prevent the key of that object from
appearing in all prefix searches. For example, if we create the following
object with the default configuration above, in an otherwise empty bucket:

```
s3a://bucket/a/b/c/DELAY_LISTING_ME
```

Then the following paths will still be visible as directories (ignoring
possible real-world inconsistencies):

```
s3a://bucket/a
s3a://bucket/a/b
```

Whereas `getFileStatus()` on the following *will* be subject to delayed
visibility (`FileNotFoundException` until delay has elapsed):

```
s3a://bucket/a/b/c
s3a://bucket/a/b/c/DELAY_LISTING_ME
```

In real-life S3 inconsistency, however, we expect that all the above paths
(including `a` and `b`) will be subject to delayed visibility.

### Using the `InconsistentAmazonS3CClient` in downstream integration tests

The inconsistent client is shipped in the `hadoop-aws` JAR, so it can
be used in applications which work with S3 to see how they handle
inconsistent directory listings.

##<a name="s3guard"></a> Testing S3Guard

[S3Guard](./s3guard.html) is an extension to S3A which adds consistent metadata
listings to the S3A client. As it is part of S3A, it also needs to be tested.

The basic strategy for testing S3Guard correctness consists of:

1. MetadataStore Contract tests.

    The MetadataStore contract tests are inspired by the Hadoop FileSystem and
    `FileContext` contract tests.  Each implementation of the `MetadataStore` interface
    subclasses the `MetadataStoreTestBase` class and customizes it to initialize
    their MetadataStore.  This test ensures that the different implementations
    all satisfy the semantics of the MetadataStore API.

2. Running existing S3A unit and integration tests with S3Guard enabled.

    You can run the S3A integration tests on top of S3Guard by configuring your
    `MetadataStore` in your
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

## Testing S3A with S3Guard Enabled

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
  DynamoDB web service; launch the server and creating the table.
  You won't be charged bills for using DynamoDB in test. As it runs in-JVM,
  the table isn't shared across other tests running in parallel.
* `non-auth`: treat the S3Guard metadata as authoritative.

```bash
mvn -T 1C verify -Dparallel-tests -DtestsThreadCount=6 -Ds3guard -Ddynamo -Dauth
```

When experimenting with options, it is usually best to run a single test suite
at a time until the operations appear to be working.

```bash
mvn -T 1C verify -Dtest=skip -Dit.test=ITestS3AMiscOperations -Ds3guard -Ddynamo
```

### Notes

1. If the `s3guard` profile is not set, then the S3Guard properties are those
of the test configuration set in `contract-test-options.xml` or `auth-keys.xml`

If the `s3guard` profile *is* set,
1. The S3Guard options from maven (the dynamo and authoritative flags)
  overwrite any previously set in the configuration files.
1. DynamoDB will be configured to create any missing tables.


### Scale Testing MetadataStore Directly

There are some scale tests that exercise Metadata Store implementations
directly. These ensure that S3Guard is are robust to things like DynamoDB
throttling, and compare performance for different implementations. These
are included in the scale tests executed when `-Dscale` is passed to
the maven command line.

The two S3Guard scale tests are `ITestDynamoDBMetadataStoreScale` and
`ITestLocalMetadataStoreScale`.  To run the DynamoDB test, you will need to
define your table name and region in your test configuration.  For example,
the following settings allow us to run `ITestDynamoDBMetadataStoreScale` with
artificially low read and write capacity provisioned, so we can judge the
effects of being throttled by the DynamoDB service:

```xml
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
  <name>fs.s3a.s3guard.ddb.region</name>
  <value>us-west-2</value>
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

There is an in-memory Metadata Store for testing.

```xml
<property>
  <name>fs.s3a.metadatastore.impl</name>
  <value>org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore</value>
</property>
```

This is not for use in production.

##<a name="assumed_roles"></a> Testing Assumed Roles

Tests for the AWS Assumed Role credential provider require an assumed
role to request.

If this role is not set, the tests which require it will be skipped.

To run the tests in `ITestAssumeRole`, you need:

1. A role in your AWS account will full read and write access rights to
the S3 bucket used in the tests, and ideally DynamoDB, for S3Guard.
If your bucket is set up by default to use S3Guard, the role must have access
to that service.

1.  Your IAM User to have the permissions to adopt that role.

1. The role ARN must be set in `fs.s3a.assumed.role.arn`.

```xml
<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value>arn:aws:iam::9878543210123:role/role-s3-restricted</value>
</property>
```

The tests assume the role with different subsets of permissions and verify
that the S3A client (mostly) works when the caller has only write access
to part of the directory tree.

You can also run the entire test suite in an assumed role, a more
thorough test, by switching to the credentials provider.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider</value>
</property>
```

The usual credentials needed to log in to the bucket will be used, but now
the credentials used to interact with S3 and DynamoDB will be temporary
role credentials, rather than the full credentials.
