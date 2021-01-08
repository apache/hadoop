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
    <name>fs.azure.wasb.account.name</name>
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
    <name>fs.azure.wasb.account.name</name>
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
`parallel-tests=wasb|abfs|both` property on the command line.  The tests spend most of their
time blocked on network I/O, so running in parallel tends to
complete full test runs faster.

```bash
mvn -T 1C -Dparallel-tests=both clean verify
mvn -T 1C -Dparallel-tests=wasb clean verify
mvn -T 1C -Dparallel-tests=abfs clean verify
```
`-Dparallel-tests=wasb` runs the WASB related integration tests from azure directory<br/>
`-Dparallel-tests=abfs` runs the ABFS related integration tests from azurebfs directory<br/>
`-Dparallel-tests=both` runs all the integration tests from both azure and azurebfs directory<br/>

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


## Testing the Azure ABFS Client

Azure Data Lake Storage Gen 2 (ADLS Gen 2) is a set of capabilities dedicated to
big data analytics, built on top of Azure Blob Storage. The ABFS and ABFSS
schemes target the ADLS Gen 2 REST API, and the WASB and WASBS schemes target
the Azure Blob Storage REST API.  ADLS Gen 2 offers better performance and
scalability.  ADLS Gen 2 also offers authentication and authorization compatible
with the Hadoop Distributed File System permissions model when hierarchical
namespace is enabled for the storage account.  Furthermore, the metadata and data
produced by ADLS Gen 2 REST API can be consumed by Blob REST API, and vice versa.

## Generating test run configurations and test triggers over various config combinations

To simplify the testing across various authentication and features combinations
that are mandatory for a PR, script `dev-support/testrun-scripts/runtests.sh`
should be used. Once the script is updated with relevant config settings for
various test combinations, it will:
1. Auto-generate configs specific to each test combinations
2. Run tests for all combinations
3. Summarize results across all the test combination runs.

As a pre-requiste step, fill config values for test accounts and credentials
needed for authentication in `src/test/resources/azure-auth-keys.xml.template`
and rename as `src/test/resources/azure-auth-keys.xml`.

**To add a new test combination:** Templates for mandatory test combinations
for PR validation are present in `dev-support/testrun-scripts/runtests.sh`.
If a new one needs to be added, add a combination set within
`dev-support/testrun-scripts/runtests.sh` similar to the ones already defined
and
1. Provide a new combination name
2. Update properties and values array which need to be effective for the test
combination
3. Call generateconfigs

**To run PR validation:** Running command
* `dev-support/testrun-scripts/runtests.sh` will generate configurations for
each of the combinations defined and run tests for all the combinations.
* `dev-support/testrun-scripts/runtests.sh -c {combinationname}` Specific
combinations can be provided with -c option. If combinations are provided
with -c option, tests for only those combinations will be run.

**Test logs:** Test runs will create a folder within dev-support/testlogs to
save the test logs. Folder name will be the test start timestamp. The mvn verify
command line logs for each combination will be saved into a file as
Test-Logs-$combination.txt into this folder. In case of any failures, this file
will have the failure exception stack. At the end of the test run, the
consolidated results of all the combination runs will be saved into a file as
Test-Results.log in the same folder. When run for PR validation, the
consolidated test results needs to be pasted into the PR comment section.

**To generate config for use in IDE:** Running command with -a (activate) option
`dev-support/testrun-scripts/runtests.sh -a {combination name}` will update
the effective config relevant for the specific test combination. Hence the same
config files used by the mvn test runs can be used for IDE without any manual
updates needed within config file.

**Other command line options:**
* -a <COMBINATION_NAME> Specify the combination name which needs to be
activated. This is to be used to generate config for use in IDE.
* -c <COMBINATION_NAME> Specify the combination name for test runs. If this
config is specified, tests for only the specified combinations will run. All
combinations of tests will be running if this config is not specified.
* -t <THREAD_COUNT> ABFS mvn tests are run in parallel mode. Tests by default
are run with 8 thread count. It can be changed by providing -t <THREAD_COUNT>

In order to test ABFS, please add the following configuration to your
`src/test/resources/azure-auth-keys.xml` file. Note that the ABFS tests include
compatibility tests which require WASB credentials, in addition to the ABFS
credentials.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
  <property>
    <name>fs.azure.abfs.account.name</name>
    <value>{ACCOUNT_NAME}.dfs.core.windows.net</value>
  </property>

  <property>
    <name>fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net</name>
    <value>{ACCOUNT_ACCESS_KEY}</value>
  </property>

  <property>
    <name>fs.azure.wasb.account.name</name>
    <value>{ACCOUNT_NAME}.blob.core.windows.net</value>
  </property>

  <property>
    <name>fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net</name>
    <value>{ACCOUNT_ACCESS_KEY}</value>
  </property>

  <property>
    <name>fs.contract.test.fs.abfs</name>
    <value>abfs://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net</value>
    <description>A file system URI to be used by the contract tests.</description>
  </property>

  <property>
    <name>fs.contract.test.fs.wasb</name>
    <value>wasb://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net</value>
    <description>A file system URI to be used by the contract tests.</description>
  </property>
</configuration>
```

To run OAuth and ACL test cases you must use a storage account with the
hierarchical namespace enabled, and set the following configuration settings:

```xml
<!--=========================== AUTHENTICATION  OPTIONS ===================-->
<!--ATTENTION:
      TO RUN ABFS & WASB COMPATIBILITY TESTS, YOU MUST SET AUTH TYPE AS SharedKey.
      OAUTH IS INTRODUCED TO ABFS ONLY.-->
<property>
  <name>fs.azure.account.auth.type.{YOUR_ABFS_ACCOUNT_NAME}</name>
  <value>{AUTH TYPE}</value>
  <description>The authorization type can be SharedKey, OAuth, Custom or SAS. The
  default is SharedKey.</description>
</property>

<!--=============================   FOR OAUTH   ===========================-->
<!--IF AUTH TYPE IS SET AS OAUTH, FOLLOW THE STEPS BELOW-->
<!--NOTICE: AAD client and tenant related properties can be obtained through Azure Portal-->

  <!--1. UNCOMMENT BELOW AND CHOOSE YOUR OAUTH PROVIDER TYPE -->

  <!--
  <property>
    <name>fs.azure.account.oauth.provider.type.{ABFS_ACCOUNT_NAME}</name>
    <value>org.apache.hadoop.fs.azurebfs.oauth2.{Token Provider Class name}</value>
    <description>The full name of token provider class name.</description>
  </property>
 -->

  <!--2. UNCOMMENT BELOW AND SET CREDENTIALS ACCORDING TO THE PROVIDER TYPE-->

  <!--2.1. If "ClientCredsTokenProvider" is set as key provider, uncomment below and
           set auth endpoint, client id and secret below-->
  <!--
   <property>
    <name>fs.azure.account.oauth2.client.endpoint.{ABFS_ACCOUNT_NAME}</name>
    <value>https://login.microsoftonline.com/{TENANTID}/oauth2/token</value>
    <description>Token end point, this can be found through Azure portal</description>
  </property>

   <property>
     <name>fs.azure.account.oauth2.client.id.{ABFS_ACCOUNT_NAME}</name>
     <value>{client id}</value>
     <description>AAD client id.</description>
   </property>

   <property>
     <name>fs.azure.account.oauth2.client.secret.{ABFS_ACCOUNT_NAME}</name>
     <value>{client secret}</value>
   </property>
 -->

  <!--2.2. If "UserPasswordTokenProvider" is set as key provider, uncomment below and
           set auth endpoint, use name and password-->
  <!--
   <property>
    <name>fs.azure.account.oauth2.client.endpoint.{ABFS_ACCOUNT_NAME}</name>
    <value>https://login.microsoftonline.com/{TENANTID}/oauth2/token</value>
    <description>Token end point, this can be found through Azure portal</description>
  </property>

   <property>
     <name>fs.azure.account.oauth2.user.name.{ABFS_ACCOUNT_NAME}</name>
     <value>{user name}</value>
   </property>

   <property>
     <name>fs.azure.account.oauth2.user.password.{ABFS_ACCOUNT_NAME}</name>
     <value>{user password}</value>
   </property>
 -->

  <!--2.3. If "MsiTokenProvider" is set as key provider, uncomment below and
           set tenantGuid and client id.-->
  <!--
   <property>
     <name>fs.azure.account.oauth2.msi.tenant.{ABFS_ACCOUNT_NAME}</name>
     <value>{tenantGuid}</value>
     <description>msi tenantGuid.</description>
   </property>

   <property>
     <name>fs.azure.account.oauth2.client.id.{ABFS_ACCOUNT_NAME}</name>
     <value>{client id}</value>
     <description>AAD client id.</description>
   </property>
  -->

  <!--2.4. If "RefreshTokenBasedTokenProvider" is set as key provider, uncomment below and
           set refresh token and client id.-->
  <!--
   <property>
     <name>fs.azure.account.oauth2.refresh.token.{ABFS_ACCOUNT_NAME}</name>
     <value>{refresh token}</value>
     <description>refresh token.</description>
   </property>

   <property>
     <name>fs.azure.account.oauth2.client.id.{ABFS_ACCOUNT_NAME}</name>
     <value>{client id}</value>
     <description>AAD client id.</description>
   </property>
  -->

  <!--
    <property>
        <name>fs.azure.identity.transformer.enable.short.name</name>
        <value>true/false</value>
        <description>
          User principal names (UPNs) have the format “{alias}@{domain}”.
          If true, only {alias} is included when a UPN would otherwise appear in the output
          of APIs like getFileStatus, getOwner, getAclStatus, etc, default is false.
        </description>
    </property>

    <property>
        <name>fs.azure.identity.transformer.domain.name</name>
        <value>domain name of the user's upn</value>
        <description>
          If the domain name is specified and “fs.azure.identity.transformer.enable.short.name”
          is true, then the {alias} part of a UPN can be specified as input to APIs like setOwner,
          setAcl, modifyAclEntries, or removeAclEntries, and it will be transformed to a UPN by appending @ and the domain specified by
          this configuration property.
        </description>
    </property>

    <property>
        <name>fs.azure.identity.transformer.service.principal.id</name>
        <value>service principal object id</value>
        <description>
          An Azure Active Directory object ID (oid) used as the replacement for names contained
          in the list specified by “fs.azure.identity.transformer.service.principal.substitution.list”.
          Notice that instead of setting oid, you can also set $superuser here.
        </description>
    </property>

    <property>
        <name>fs.azure.identity.transformer.skip.superuser.replacement</name>
        <value>true/false</value>
        <description>
          If false, “$superuser” is replaced with the current user when it appears as the owner
          or owning group of a file or directory. The default is false.
        </description>
    </property>

    <property>
        <name>fs.azure.identity.transformer.service.principal.substitution.list</name>
        <value>mapred,hdfs,yarn,hive,tez</value>
        <description>
           A comma separated list of names to be replaced with the service principal ID specified by
           “fs.azure.identity.transformer.service.principal.id”.  This substitution occurs
           when setOwner, setAcl, modifyAclEntries, or removeAclEntries are invoked with identities
           contained in the substitution list. Notice that when in non-secure cluster, asterisk symbol *
           can be used to match all user/group.
        </description>
    </property>
   -->

```
To run Delegation SAS test cases you must use a storage account with the
hierarchical namespace enabled and set the following configuration settings:

```xml
<!--=========================== AUTHENTICATION  OPTIONS ===================-->
<!--=============================   FOR SAS   ===========================-->
<!-- To run ABFS Delegation SAS tests, you must register an app, create the
     necessary role assignments, and set the configuration discussed below:

    1) Register an app:
      a) Login to https://portal.azure.com, select your AAD directory and search for app registrations.
      b) Click "New registration".
      c) Provide a display name, such as "abfs-app".
      d) Set the account type to "Accounts in this organizational directory only ({YOUR_Tenant} only - Single tenant)".
      e) For Redirect URI select Web and enter "http://localhost".
      f) Click Register.

    2)  Create necessary role assignments:
      a) Login to https://portal.azure.com and find the Storage account with hierarchical namespace enabled
         that you plan to run the tests against.
      b) Select "Access Control (IAM)".
      c) Select Role Assignments
      d) Click Add and select "Add role assignments"
      e) For Role and enter "Storage Blob Data Owner".
      f) Under Select enter the name of the app you registered in step 1 and select it.
      g) Click Save.
      h) Repeat above steps to create a second role assignment for the app but this time for
         the "Storage Blob Delegator" role.

    3) Generate a new client secret for the application:
      a) Login to https://portal.azure.com and find the app registered in step 1.
      b) Select "Certificates and secrets".
      c) Click "New client secret".
      d) Enter a description (eg. Secret1)
      e) Set expiration period.  Expires in 1 year is good.
      f) Click Add
      g) Copy the secret and expiration to a safe location.

    4) Set the following configuration values:
-->

  <property>
    <name>fs.azure.sas.token.provider.type</name>
    <value>org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider</value>
    <description>The fully qualified class name of the SAS token provider implementation.</description>
  </property>

  <property>
    <name>fs.azure.test.app.service.principal.tenant.id</name>
    <value>{TID}</value>
    <description>Tenant ID for the application's service principal.</description>
  </property>

  <property>
    <name>fs.azure.test.app.service.principal.object.id</name>
    <value>{OID}</value>
    <description>Object ID for the application's service principal.</description>
  </property>

  <property>
    <name>fs.azure.test.app.id</name>
    <value>{app id}</value>
    <description>The application's ID, also known as the client id.</description>
  </property>

   <property>
     <name>fs.azure.test.app.secret</name>
     <value>{client secret}</value>
     <description>The application's secret, also known as the client secret.</description>
   </property>


```

To run CheckAccess test cases you must register an app with no RBAC and set
the following configurations.
```xml
<!--===========================   FOR CheckAccess =========================-->
<!-- To run ABFS CheckAccess SAS tests, you must register an app, with no role
 assignments, and set the configuration discussed below:

    1) Register a new app with no RBAC
    2) As part of the test configs you need to provide the guid for the above
created app. Please follow the below steps to fetch the guid.
      a) Get an access token with the above created app. Please refer the
 following documentation for the same. https://docs.microsoft
.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#get-a-token
      b) Decode the token fetched with the above step. You may use https
://jwt.ms/ to decode the token
      d) The oid field in the decoded string is the guid.
    3) Set the following configurations:
-->

  <property>
    <name>fs.azure.enable.check.access</name>
    <value>true</value>
    <description>By default the check access will be on. Checkaccess can
    be turned off by changing this flag to false.</description>
  </property>
  <property>
    <name>fs.azure.account.test.oauth2.client.id</name>
    <value>{client id}</value>
    <description>The client id(app id) for the app created on step 1
    </description>
  </property>
  <property>
    <name>fs.azure.account.test.oauth2.client.secret</name>
    <value>{client secret}</value>
    <description>
The client secret(application's secret) for the app created on step 1
    </description>
  </property>
  <property>
    <name>fs.azure.check.access.testuser.guid</name>
    <value>{guid}</value>
    <description>The guid fetched on step 2</description>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.endpoint.{account name}.dfs.core
.windows.net</name>
    <value>https://login.microsoftonline.com/{TENANTID}/oauth2/token</value>
    <description>
Token end point. This can be found through Azure portal. As part of CheckAccess
test cases. The access will be tested for an FS instance created with the
above mentioned client credentials. So this configuration is necessary to
create the test FS instance.
    </description>
  </property>

```

If running tests against an endpoint that uses the URL format
http[s]://[ip]:[port]/[account]/[filesystem] instead of
http[s]://[account][domain-suffix]/[filesystem], please use the following:

```xml
<property>
  <name>fs.azure.abfs.endpoint</name>
  <value>{IP}:{PORT}</value>
</property>
```
