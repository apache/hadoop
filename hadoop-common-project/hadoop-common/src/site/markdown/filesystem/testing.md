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

# Testing the Filesystem Contract

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Running the tests

A normal Hadoop test run will test those FileSystems that can be tested locally
via the local filesystem. This typically means `file://` and its underlying `LocalFileSystem`, and
`hdfs://` via the HDFS MiniCluster.

Other filesystems are skipped unless there is a specific configuration to the
remote server providing the filesystem.


These filesystem bindings must be defined in an XML configuration file, usually
`hadoop-common-project/hadoop-common/src/test/resources/contract-test-options.xml`.
This file is excluded and should not be checked in.

### ftp://


In `contract-test-options.xml`, the filesystem name must be defined in
the property `fs.contract.test.fs.ftp`. The specific login options to
connect to the FTP Server must then be provided.

A path to a test directory must also be provided in the option
`fs.contract.test.ftp.testdir`. This is the directory under which
operations take place.

Example:


    <configuration>
      <property>
        <name>fs.contract.test.fs.ftp</name>
        <value>ftp://server1/</value>
      </property>

      <property>
        <name>fs.ftp.user.server1</name>
        <value>testuser</value>
      </property>

      <property>
        <name>fs.contract.test.ftp.testdir</name>
        <value>/home/testuser/test</value>
      </property>

      <property>
        <name>fs.ftp.password.server1</name>
        <value>secret-login</value>
      </property>
    </configuration>


### swift://

The OpenStack Swift login details must be defined in the file
`/hadoop-tools/hadoop-openstack/src/test/resources/contract-test-options.xml`.
The standard hadoop-common `contract-test-options.xml` resource file cannot be
used, as that file does not get included in `hadoop-common-test.jar`.


In `/hadoop-tools/hadoop-openstack/src/test/resources/contract-test-options.xml`
the Swift bucket name must be defined in the property `fs.contract.test.fs.swift`,
along with the login details for the specific Swift service provider in which the
bucket is posted.

    <configuration>
      <property>
        <name>fs.contract.test.fs.swift</name>
        <value>swift://swiftbucket.rackspace/</value>
      </property>

      <property>
        <name>fs.swift.service.rackspace.auth.url</name>
        <value>https://auth.api.rackspacecloud.com/v2.0/tokens</value>
        <description>Rackspace US (multiregion)</description>
      </property>

      <property>
        <name>fs.swift.service.rackspace.username</name>
        <value>this-is-your-username</value>
      </property>

      <property>
        <name>fs.swift.service.rackspace.region</name>
        <value>DFW</value>
      </property>

      <property>
        <name>fs.swift.service.rackspace.apikey</name>
        <value>ab0bceyoursecretapikeyffef</value>
      </property>

    </configuration>

1. Often the different public cloud Swift infrastructures exhibit different behaviors
(authentication and throttling in particular). We recommand that testers create
accounts on as many of these providers as possible and test against each of them.
1. They can be slow, especially remotely. Remote links are also the most likely
to make eventual-consistency behaviors visible, which is a mixed benefit.

## Testing a new filesystem

The core of adding a new FileSystem to the contract tests is adding a
new contract class, then creating a new non-abstract test class for every test
suite that you wish to test.

1. Do not try and add these tests into Hadoop itself. They won't be added to
the source tree. The tests must live with your own filesystem source.
1. Create a package in your own test source tree (usually) under `contract`,
for the files and tests.
1. Subclass `AbstractFSContract` for your own contract implementation.
1. For every test suite you plan to support create a non-abstract subclass,
 with the name starting with `Test` and the name of the filesystem.
 Example: `TestHDFSRenameContract`.
1. These non-abstract classes must implement the abstract method
 `createContract()`.
1. Identify and document any filesystem bindings that must be defined in a
 `src/test/resources/contract-test-options.xml` file of the specific project.
1. Run the tests until they work.


As an example, here is the implementation of the test of the `create()` tests for the local filesystem.

    package org.apache.hadoop.fs.contract.localfs;

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.contract.AbstractCreateContractTest;
    import org.apache.hadoop.fs.contract.AbstractFSContract;

    public class TestLocalCreateContract extends AbstractCreateContractTest {
      @Override
      protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
      }
    }

The standard implementation technique for subclasses of `AbstractFSContract` is to be driven entirely by a Hadoop XML configuration file stored in the test resource tree. The best practise is to store it under `/contract` with the name of the FileSystem, such as `contract/localfs.xml`. Having the XML file define all FileSystem options makes the listing of FileSystem behaviors immediately visible.

The `LocalFSContract` is a special case of this, as it must adjust its case sensitivity policy based on the OS on which it is running: for both Windows and OS/X, the filesystem is case insensitive, so the `ContractOptions.IS_CASE_SENSITIVE` option must be set to false. Furthermore, the Windows filesystem does not support Unix file and directory permissions, so the relevant flag must also be set. This is done *after* loading the XML contract file from the resource tree, simply by updating the now-loaded configuration options:

      getConf().setBoolean(getConfKey(ContractOptions.SUPPORTS_UNIX_PERMISSIONS), false);



### Handling test failures

If your new `FileSystem` test cases fails one of the contract tests, what you can you do?

It depends on the cause of the problem

1. Case: custom `FileSystem` subclass class doesn't correctly implement specification. Fix.
1. Case: Underlying filesystem doesn't behave in a way that matches Hadoop's expectations. Ideally, fix. Or try to make your `FileSystem` subclass hide the differences, e.g. by translating exceptions.
1. Case: fundamental architectural differences between your filesystem and Hadoop. Example: different concurrency and consistency model. Recommendation: document and make clear that the filesystem is not compatible with HDFS.
1. Case: test does not match the specification. Fix: patch test, submit the patch to Hadoop.
1. Case: specification incorrect. The underlying specification is (with a few exceptions) HDFS. If the specification does not match HDFS, HDFS should normally be assumed to be the real definition of what a FileSystem should do. If there's a mismatch, please raise it on the `hdfs-dev` mailing list. Note that while FileSystem tests live in the core Hadoop codebase, it is the HDFS team who owns the FileSystem specification and the tests that accompany it.

If a test needs to be skipped because a feature is not supported, look for a existing configuration option in the `ContractOptions` class. If there is no method, the short term fix is to override the method and use the `ContractTestUtils.skip()` message to log the fact that a test is skipped. Using this method prints the message to the logs, then tells the test runner that the test was skipped. This highlights the problem.

A recommended strategy is to call the superclass, catch the exception, and verify that the exception class and part of the error string matches that raised by the current implementation. It should also `fail()` if superclass actually succeeded -that is it failed the way that the implemention does not currently do.  This will ensure that the test path is still executed, any other failure of the test -possibly a regression- is picked up. And, if the feature does become implemented, that the change is picked up.

A long-term solution is to enhance the base test to add a new optional feature key. This will require collaboration with the developers on the `hdfs-dev` mailing list.



### 'Lax vs Strict' exceptions

The contract tests include the notion of strict vs lax exceptions. *Strict* exception reporting means: reports failures using specific subclasses of `IOException`, such as `FileNotFoundException`, `EOFException` and so on. *Lax* reporting means throws `IOException`.

While FileSystems SHOULD raise stricter exceptions, there may be reasons why they cannot. Raising lax exceptions is still allowed, it merely hampers diagnostics of failures in user applications. To declare that a FileSystem does not support the stricter exceptions, set the option `fs.contract.supports-strict-exceptions` to false.

### Supporting FileSystems with login and authentication parameters

Tests against remote FileSystems will require the URL to the FileSystem to be specified;
tests against remote FileSystems that require login details require usernames/IDs and passwords.

All these details MUST be required to be placed in the file `src/test/resources/contract-test-options.xml`, and your SCM tools configured to never commit this file to subversion, git or
equivalent. Furthermore, the build MUST be configured to never bundle this file in any `-test` artifacts generated. The Hadoop build does this, excluding `src/test/**/*.xml` from the JAR files.
In addition, `src/test/resources/auth-keys.xml` will need to be created.  It can be a copy of `contract-test-options.xml`.
The `AbstractFSContract` class automatically loads this resource file if present; specific keys for specific test cases can be added.

As an example, here are what S3N test keys look like:

    <configuration>
      <property>
        <name>fs.contract.test.fs.s3n</name>
        <value>s3n://tests3contract</value>
      </property>

      <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>
    </configuration>

The `AbstractBondedFSContract` automatically skips a test suite if the FileSystem URL is not defined in the property `fs.contract.test.fs.%s`, where `%s` matches the schema name of the FileSystem.

When running the tests `maven.test.skip` will need to be turned off since it is true by default on these tests.  This can be done with a command like `mvn test -Ptests-on`.

### Important: passing the tests does not guarantee compatibility

Passing all the FileSystem contract tests does not mean that a filesystem can be described as "compatible with HDFS". The tests try to look at the isolated functionality of each operation, and focus on the preconditions and postconditions of each action. Core areas not covered are concurrency and aspects of failure across a distributed system.

* Consistency: are all changes immediately visible?
* Atomicity: are operations which HDFS guarantees to be atomic equally so on the new filesystem.
* Idempotency: if the filesystem implements any retry policy, is idempotent even while other clients manipulate the filesystem?
* Scalability: does it support files as large as HDFS, or as many in a single directory?
* Durability: do files actually last -and how long for?

Proof that this is is true is the fact that the Amazon S3 and OpenStack Swift object stores are eventually consistent object stores with non-atomic rename and delete operations. Single threaded test cases are unlikely to see some of the concurrency issues, while consistency is very often only visible in tests that span a datacenter.

There are also some specific aspects of the use of the FileSystem API:

* Compatibility with the `hadoop -fs` CLI.
* Whether the blocksize policy produces file splits that are suitable for analytics workss. (as an example, a blocksize of 1 matches the specification, but as it tells MapReduce jobs to work a byte at a time, unusable).

Tests that verify these behaviors are of course welcome.



## Adding a new test suite

1. New tests should be split up with a test class per operation, as is done for `seek()`, `rename()`, `create()`, and so on. This is to match up the way that the FileSystem contract specification is split up by operation. It also makes it easier for FileSystem implementors to work on one test suite at a time.
2. Subclass `AbstractFSContractTestBase` with a new abstract test suite class. Again, use `Abstract` in the title.
3. Look at `org.apache.hadoop.fs.contract.ContractTestUtils` for utility classes to aid testing, with lots of filesystem-centric assertions. Use these to make assertions about the filesystem state, and to incude diagnostics information such as directory listings and dumps of mismatched files when an assertion actually fails.
4. Write tests for the local, raw local and HDFS filesystems -if one of these fails the tests then there is a sign of a problem -though be aware that they do have differnces
5. Test on the object stores once the core filesystems are passing the tests.
4. Try and log failures with as much detail as you can -the people debugging the failures will appreciate it.


### Root manipulation tests

Some tests work directly against the root filesystem, attempting to do things like rename "/" and similar actions. The root directory is "special", and it's important to test this, especially on non-POSIX filesystems such as object stores. These tests are potentially very destructive to native filesystems, so use care.

1. Add the tests under `AbstractRootDirectoryContractTest` or create a new test with (a) `Root` in the title and (b) a check in the setup method to skip the test if root tests are disabled:

          skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);

1. Don't provide an implementation of this test suite to run against the local FS.

### Scalability tests

Tests designed to generate scalable load -and that includes a large number of small files, as well as fewer larger files, should be designed to be configurable, so that users of the test
suite can configure the number and size of files.

Be aware that on object stores, the directory rename operation is usually `O(files)*O(data)` while the delete operation is `O(files)`. The latter means even any directory cleanup operations may take time and can potentially timeout. It is important to design tests that work against remote filesystems with possible delays in all operations.

## Extending the specification

The specification is incomplete. It doesn't have complete coverage of the FileSystem classes, and there may be bits of the existing specified classes that are not covered.

1. Look at the implementations of a class/interface/method to see what they do, especially HDFS and local. These are the documentation of what is done today.
2. Look at the POSIX API specification.
3. Search through the HDFS JIRAs for discussions on FileSystem topics, and try to understand what was meant to happen, as well as what does happen.
4. Use an IDE to find out how methods are used in Hadoop, HBase and other parts of the stack. Although this assumes that these are representative Hadoop applications, it will at least show how applications *expect* a FileSystem to behave.
5. Look in the java.io source to see how the bunded FileSystem classes are expected to behave -and read their javadocs carefully.
5. If something is unclear -as on the hdfs-dev list.
6. Don't be afraid to write tests to act as experiments and clarify what actually happens. Use the HDFS behaviours as the normative guide.
