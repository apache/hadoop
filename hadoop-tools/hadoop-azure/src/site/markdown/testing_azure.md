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

# Testing the Azure WASB client

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

This module includes both unit tests, which can run in isolation without
connecting to the Azure Storage service, and integration tests, which require a working
connection to interact with a container.  Unit test suites follow the naming
convention `Test*.java`.  Integration tests follow the naming convention
`ITest*.java`.

## Policy for submitting patches which affect the `hadoop-azure` module.

The Apache Jenkins infrastucture does not run any cloud integration tests,
due to the need to keep credentials secure.

### The submitter of any patch is required to run all the integration tests and declare which Azure region they used.

This is important: **patches which do not include this declaration will be ignored**

This policy has proven to be the only mechanism to guarantee full regression
testing of code changes. Why the declaration of region? Two reasons

1. It helps us identify regressions which only surface against specific endpoints.
1. It forces the submitters to be more honest about their testing. It's easy
to lie, "yes, I tested this". To say "yes, I tested this against Azure US-west"
is a more specific lie and harder to make. And, if you get caught out: you
lose all credibility with the project.

You don't need to test from a VM within the Azure infrastructure, all you need
are credentials.

It's neither hard nor expensive to run the tests; if you can't,
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

The tests are designed to be configurable for different
timeouts. If you are seeing problems and this configuration isn't working,
that's a sign of the configuration mechanism isn't complete. If it's happening
in the production code, that could be a sign of a problem which may surface
over long-haul connections. Please help us identify and fix these problems
&mdash; especially as you are the one best placed to verify the fixes work.

## Setting up the tests

## Testing the `hadoop-azure` Module

The `hadoop-azure` module includes a full suite of unit tests.  Many of the tests
will run without additional configuration by running `mvn test`.  This includes
tests against mocked storage, which is an in-memory emulation of Azure Storage.

The integration tests are designed to test directly against an Azure storage
service, and require an account and credentials in order to run.

This is done by creating the file to `src/test/resources/azure-auth-keys.xml`
and setting the name of the storage account and its access key.

For example:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.azure.test.account.name</name>
    <value>{ACCOUNTNAME}.blob.core.windows.net</value>
  </property>
  <property>
    <name>fs.azure.account.key.{ACCOUNTNAME}.blob.core.windows.net</name>
    <value>{ACCOUNT ACCESS KEY}</value>
  </property>
</configuration>
```

To run contract tests, set the WASB file system URI in `src/test/resources/azure-auth-keys.xml`
and the account access key. For example:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.contract.test.fs.wasb</name>
    <value>wasb://{CONTAINERNAME}@{ACCOUNTNAME}.blob.core.windows.net</value>
    <description>The name of the azure file system for testing.</description>
  </property>
  <property>
    <name>fs.azure.account.key.{ACCOUNTNAME}.blob.core.windows.net</name>
    <value>{ACCOUNT ACCESS KEY}</value>
  </property>
</configuration>
```

Overall, to run all the tests using `mvn test`,  a sample `azure-auth-keys.xml` is like following:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.azure.test.account.name</name>
    <value>{ACCOUNTNAME}.blob.core.windows.net</value>
  </property>
  <property>
    <name>fs.azure.account.key.{ACCOUNTNAME}.blob.core.windows.net</name>
    <value>{ACCOUNT ACCESS KEY}</value>
  </property>
  <property>
    <name>fs.contract.test.fs.wasb</name>
    <value>wasb://{CONTAINERNAME}@{ACCOUNTNAME}.blob.core.windows.net</value>
  </property>
</configuration>
```

DO NOT ADD `azure-auth-keys.xml` TO REVISION CONTROL.  The keys to your Azure
Storage account are a secret and must not be shared.


## Running the Tests

After completing the configuration, execute the test run through Maven.

```bash
mvn -T 1C clean verify
```

It's also possible to execute multiple test suites in parallel by passing the
`parallel-tests` property on the command line.  The tests spend most of their
time blocked on network I/O, so running in parallel tends to
complete full test runs faster.

```bash
mvn -T 1C -Dparallel-tests clean verify
```

Some tests must run with exclusive access to the storage container, so even with the
`parallel-tests` property, several test suites will run in serial in a separate
Maven execution step after the parallel tests.

By default, `parallel-tests` runs 4 test suites concurrently.  This can be tuned
by passing the `testsThreadCount` property.

```bash
mvn -T 1C -Dparallel-tests -DtestsThreadCount=8 clean verify
```

<!---
To run just unit tests, which do not require Azure connectivity or credentials,
use any of the above invocations, but switch the goal to `test` instead of
`verify`.
-->

```bash
mvn -T 1C clean test

mvn -T 1C -Dparallel-tests clean test

mvn -T 1C -Dparallel-tests -DtestsThreadCount=8 clean test
```

To run only a specific named subset of tests, pass the `test` property for unit
tests or the `it.test` property for integration tests.

```bash
mvn -T 1C clean test -Dtest=TestRollingWindowAverage

mvn -T 1C clean verify -Dscale -Dit.test=ITestFileSystemOperationExceptionMessage -Dtest=none

mvn -T 1C clean verify -Dtest=none -Dit.test=ITest*

```

Note

1. When running a specific subset of tests, the patterns passed in `test`
and `it.test` override the configuration of which tests need to run in isolation
in a separate serial phase (mentioned above).  This can cause unpredictable
results, so the recommendation is to avoid passing `parallel-tests` in
combination with `test` or `it.test`.  If you know that you are specifying only
tests that can run safely in parallel, then it will work.  For wide patterns,
like `ITest*` shown above, it may cause unpredictable test failures.

2. The command line shell may try to expand the "*" and sometimes the "#" symbols
in test patterns. In such situations, escape the character it with a "\\" prefix.
Example:

          mvn -T 1C clean verify -Dtest=none -Dit.test=ITest\*


## Viewing the results

Integration test results and logs are stored in `target/failsafe-reports/`.
An HTML report can be generated during site generation, or with the `surefire-report`
plugin:

```bash

# for the unit tests
mvn -T 1C surefire-report:report-only

# for the integration tests
mvn -T 1C surefire-report:failsafe-report-only

# all reports for this module
mvn -T 1C site:site
```

## Scale Tests

There are a set of tests designed to measure the scalability and performance
at scale of the filesystem client, *Scale Tests*. Tests include: creating
and traversing directory trees, uploading large files, renaming them,
deleting them, seeking through the files, performing random IO, and others.
This makes them a foundational part of the benchmarking.

By their very nature they are slow. And, as their execution time is often
limited by bandwidth between the computer running the tests and the Azure endpoint,
parallel execution does not speed these tests up.

### Enabling the Scale Tests

The tests are enabled if the `scale` property is set in the maven build
this can be done regardless of whether or not the parallel test profile
is used

```bash
mvn -T 1C verify -Dscale

mvn -T 1C verify -Dparallel-tests -Dscale -DtestsThreadCount=8
```

The most bandwidth intensive tests (those which upload data) always run
sequentially; those which are slow due to HTTPS setup costs or server-side
actions are included in the set of parallelized tests.


### Scale test tuning options


Some of the tests can be tuned from the maven build or from the
configuration file used to run the tests.

```bash
mvn -T 1C verify -Dparallel-tests -Dscale -DtestsThreadCount=8 -Dfs.azure.scale.test.huge.filesize=128M
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

| Property | Meaninging |
|-----------|-------------|
| `fs.azure.scale.test.huge.filesize`| Size for huge file uploads |
| `fs.azure.scale.test.huge.huge.partitionsize`| Size for partitions in huge file uploads |

The file and partition sizes are numeric values with a k/m/g/t/p suffix depending
on the desired size. For example: 128M, 128m, 2G, 2G, 4T or even 1P.

#### Scale test configuration options

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

DistCp tests targeting Azure support a configurable file size.  The default is
10 MB, but the configuration value is expressed in KB so that it can be tuned
smaller to achieve faster test runs.

```xml
<property>
  <name>scale.test.distcp.file.size.kb</name>
  <value>10240</value>
</property>
```

Azure-specific scale test properties are

##### `fs.azure.scale.test.huge.filesize`: size in MB for "Huge file tests".

The Huge File tests validate Azure storages's ability to handle large files —the property
`fs.azure.scale.test.huge.filesize` declares the file size to use.

```xml
<property>
  <name>fs.azure.scale.test.huge.filesize</name>
  <value>200M</value>
</property>
```

Tests at this scale are slow: they are best executed from hosts running in
the cloud infrastructure where the storage endpoint is based.

## Using the emulator

A selection of tests can run against the
[Azure Storage Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx)
which is a high-fidelity emulation of live Azure Storage.  The emulator is
sufficient for high-confidence testing.  The emulator is a Windows executable
that runs on a local machine.

To use the emulator, install Azure SDK 2.3 and start the storage emulator.  Then,
edit `src/test/resources/azure-test.xml` and add the following property:

```xml
<property>
  <name>fs.azure.test.emulator</name>
  <value>true</value>
</property>
```

There is a known issue when running tests with the emulator.  You may see the
following failure message:

    com.microsoft.windowsazure.storage.StorageException: The value for one of the HTTP headers is not in the correct format.

To resolve this, restart the Azure Emulator.  Ensure it is v3.2 or later.


## Debugging Test failures

Logging at debug level is the standard way to provide more diagnostics output;
after setting this rerun the tests

```properties
log4j.logger.org.apache.hadoop.fs.azure=DEBUG
```

## Adding new tests

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

*Fail with useful information:* provide as much diagnostics as possible
on a failure. Using `org.apache.hadoop.fs.contract.ContractTestUtils` to make
assertions about the state of a filesystem helps here.

*Isolating Scale tests*. Any test doing large amounts of IO MUST extend the
class `AbstractAzureScaleTest`, so only running if `scale` is defined on a build,
supporting test timeouts configurable by the user. Scale tests should also
support configurability as to the actual size of objects/number of operations,
so that behavior at different scale can be verified.

*Designed for parallel execution*. A key need here is for each test suite to work
on isolated parts of the filesystem. Subclasses of `AbstractWasbTestBase`
SHOULD use the `path()`, `methodpath()` and `blobpath()` methods,
to build isolated paths. Tests MUST NOT assume that they have exclusive access
to a bucket.

*Extending existing tests where appropriate*. This recommendation goes
against normal testing best practise of "test one thing per method".
Because it is so slow to create directory trees or upload large files, we do
not have that luxury. All the tests against real endpoints are integration
tests where sharing test setup and teardown saves time and money.

A standard way to do this is to extend existing tests with some extra predicates,
rather than write new tests. When doing this, make sure that the new predicates
fail with meaningful diagnostics, so any new problems can be easily debugged
from test logs.


### Requirements of new Tests


This is what we expect from new tests; they're an extension of the normal
Hadoop requirements, based on the need to work with remote servers whose
use requires the presence of secret credentials, where tests may be slow,
and where finding out why something failed from nothing but the test output
is critical.

#### Subclasses Existing Shared Base Blasses

There are a set of base classes which should be extended for Azure tests and
integration tests.

##### `org.apache.hadoop.fs.azure.AbstractWasbTestWithTimeout`

This extends the junit `Assert` class with thread names and timeouts,
the default timeout being set in `AzureTestConstants.AZURE_TEST_TIMEOUT` to
ten minutes. The thread names are set to aid analyzing the stack trace of
a test: a `jstack` call can be used to

##### `org.apache.hadoop.fs.azure.AbstractWasbTestBase`

The base class for tests which use `AzureBlobStorageTestAccount` to create
mock or live Azure clients; in test teardown it tries to clean up store state.

1. This class requires subclasses to implement `createTestAccount()` to create
a mock or real test account.

1. The configuration used to create a test account *should* be that from
`createConfiguration()`; this can be extended in subclasses to tune the settings.


##### `org.apache.hadoop.fs.azure.integration.AbstractAzureScaleTest`

This extends `AbstractWasbTestBase` for scale tests; those test which
only run when `-Dscale` is used to select the "scale" profile.
These tests have a timeout of 30 minutes, so as to support slow test runs.

Having shared base classes help reduces future maintenance. Please
use them.

#### Secure

Don't ever log credentials. The credential tests go out of their way to
not provide meaningful logs or assertion messages precisely to avoid this.

#### Efficient of Time and Money

This means efficient in test setup/teardown, and, ideally, making use of
existing public datasets to save setup time and tester cost.


The reference example is `ITestAzureHugeFiles`:. This marks the test suite as
`@FixMethodOrder(MethodSorters.NAME_ASCENDING)` then orders the test cases such
that each test case expects the previous test to have completed (here: uploaded a file,
renamed a file, ...). This provides for independent tests in the reports, yet still
permits an ordered sequence of operations. Do note the use of `Assume.assume()`
to detect when the preconditions for a single test case are not met, hence,
the tests become skipped, rather than fail with a trace which is really a false alarm.


### Works Over Long-haul Links

As well as making file size and operation counts scaleable, this includes
making test timeouts adequate. The Scale tests make this configurable; it's
hard coded to ten minutes in `AbstractAzureIntegrationTest()`; subclasses can
change this by overriding `getTestTimeoutMillis()`.

Equally importantly: support proxies, as some testers need them.


### Provides Diagnostics and timing information

1. Create logs, log things.
1. you can use `AbstractWasbTestBase.describe(format-string, args)` here; it
adds some newlines so as to be easier to spot.
1. Use `ContractTestUtils.NanoTimer` to measure the duration of operations,
and log the output.

#### Fails Meaningfully

The `ContractTestUtils` class contains a whole set of assertions for making
statements about the expected state of a filesystem, e.g.
`assertPathExists(FS, path)`, `assertPathDoesNotExists(FS, path)`, and others.
These do their best to provide meaningful diagnostics on failures (e.g. directory
listings, file status, ...), so help make failures easier to understand.

At the very least, *do not use `assertTrue()` or `assertFalse()` without
including error messages*.


### Cleans Up Afterwards

Keeps costs down.

1. Do not only cleanup if a test case completes successfully; test suite
teardown must do it.
1. That teardown code must check for the filesystem and other fields being
null before the cleanup. Why? If test setup fails, the teardown methods still
get called.

### Works Reliably

We really appreciate this &mdash; you will too.


## Tips

### How to keep your credentials really safe

Although the `auth-keys.xml` file is marged as ignored in git and subversion,
it is still in your source tree, and there's always that risk that it may
creep out.

You can avoid this by keeping your keys outside the source tree and
using an absolute XInclude reference to it.

```xml
<configuration>

  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///users/qe/.auth-keys.xml" />

</configuration>
```

### Cleaning up Containers

The Azure tests create containers with the prefix `"wasbtests-"` and delete
them after the test runs. If a test run is interrupted, these containers
may not get deleted. There is a special test case which can be manually invoked
to list and delete these, `CleanupTestContainers`

```bash
mvn test -Dtest=CleanupTestContainers
```

This will delete the containers; the output log of the test run will
provide the details and summary of the operation.


## Testing ABFS

The ABFS Connector tests share the same account as the wasb tests; this is
needed for cross-connector compatibility tests.

This makes for a somewhat complex set of configuration options.

Here are the settings for an account `ACCOUNTNAME`

```xml
<property>
  <name>abfs.account.name</name>
  <value>ACCOUNTNAME</value>
</property>

<property>
  <name>abfs.account.full.name</name>
  <value>${abfs.account.name}.dfs.core.windows.net</value>
</property>

<property>
  <name>abfs.account.key</name>
  <value>SECRETKEY==</value>
</property>

<property>
  <name>fs.azure.account.key.ACCOUNTNAME.dfs.core.windows.net</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.account.key.ACCOUNTNAME.blob.core.windows.net</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.test.account.key.ACCOUNTNAME.dfs.core.windows.net</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.test.account.key.ACCOUNTNAME.blob.core.windows.net</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.account.key.ACCOUNTNAME</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.test.account.key.ACCOUNTNAME</name>
  <value>${abfs.account.key}</value>
</property>

<property>
  <name>fs.azure.test.account.name</name>
  <value>${abfs.account.full.name}</value>
</property>

<property>
  <name>fs.contract.test.fs.abfs</name>
  <value>abfs://TESTCONTAINER@ACCOUNTNAME.dfs.core.windows.net</value>
  <description>Container for contract tests</description>
</property>

<property>
  <name>fs.contract.test.fs.abfss</name>
  <value>abfss://TESTCONTAINER@ACCOUNTNAME.dfs.core.windows.net</value>
  <description>Container for contract tests</description>
</property>


```
