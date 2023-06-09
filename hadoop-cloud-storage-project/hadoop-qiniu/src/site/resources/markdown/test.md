# Mock Test the hadoop-qiniu Module

The project also supports offline testing based mock, which simulates the Kodo service based on the `HashMap` data
structure in memory, and does not require any authentication information.

But in order to prevent `maven` from skipping the test, you still need to create
an `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/auth-keys.xml` file, but no
authentication information is required. You can create a file with the following content:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
</configuration>
```

We also need to create the
file `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/contract-test-options.xml`, the content of this file
is as follows:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.qiniu.test.useMock</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.contract.test.fs.mockkodo</name>
    <value>mockkodo://hadoop-java</value>
  </property>

  <property>
    <name>fs.mockkodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.MockQiniuKodoFileSystem</value>
  </property>

  <property>
    <name>fs.AbstractFileSystem.mockkodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.MockQiniuKodo</value>
  </property>
</configuration>
```

## Use maven command

To run the contract tests, you need to use the `mvn` command in the `hadoop-cloud-storage-project/hadoop-qiniu/` folder,
and specify all the test classes that need to be run using the `-Dtest` parameter, for example:

```shell
cd hadoop-cloud-storage-project/hadoop-qiniu/
mvn test -Dtest=ITestQiniuKodoFileSystemContractBase,ITestQiniuKodoContractCreate,ITestQiniuKodoContractDelete,ITestQiniuKodoContractDistCp,ITestQiniuKodoContractGetFileStatus,ITestQiniuKodoContractMkdir,ITestQiniuKodoContractOpen,ITestQiniuKodoContractRename,ITestQiniuKodoContractRootDir,ITestQiniuKodoContractSeek
```

> PS: If you run the `ITestQiniuKodoContractDistCp` test class, some test cases report an error
> `java.lang.NoSuchMethodError`, please first in the outermost `hadoop` repository, execute
> the `mvn install -DskipTests` command

# Living Test the hadoop-qiniu Module

To test `kodo://` filesystem client，two files in `hadoop-qiniu/src/test/resources` which pass in
authentication details to the test runner are needed.

1. `auth-keys.xml`

2. `core-site.xml`

## `core-site.xml`

This file is pre-exists. For most cases, no modification is needed, unless a specific, non-default property needs to be
set during the testing.

## `auth-keys.xml`

This file will trigger the testing of Qiniu Kodo module. Without the file,
*none of the tests in this module will be executed*

It contains the accessKey, secretKey and optional proxy configuration information that are needed to connect to Qiniu
Kodo. If you test in private-cloud environment, the host related region should also be configured.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
    <property>
        <name>fs.qiniu.auth.accessKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>
    <property>
        <name>fs.qiniu.auth.secretKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
    </property>
</configuration>
```

## Run Hadoop Contract Test

Create file `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/contract-test-options.xml`, If a
specific file `fs.contract.test.fs.kodo` test path is not defined, those tests will be skipped.
Credentials are also needed to run any of those tests, they can be copied
from `hadoop-cloud-storage-project/hadoop-qiniu/src/test/resources/auth-keys.xml` and through direct
XInclude inclusion. Here is an example of `contract-test-options.xml`:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <include xmlns="http://www.w3.org/2001/XInclude"
             href="auth-keys.xml"/>

    <property>
        <name>fs.qiniu.useHttps</name>
        <value>false</value>
    </property>

  <property>
    <name>fs.qiniu.download.useHttps</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.qiniu.test.useMock</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.contract.test.fs.kodo</name>
    <value>kodo://your-test-bucket</value>
  </property>

  <property>
    <name>fs.kodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem</value>
  </property>

  <property>
    <name>fs.AbstractFileSystem.kodo.impl</name>
    <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodo</value>
  </property>
    
  <property>
    <name>fs.qiniu.download.useNoCacheHeader</name>
    <value>true</value>
  </property>
</configuration>
```

### Use maven command

To run the contract tests, you need to use the `mvn` command in the `hadoop-cloud-storage-project/hadoop-qiniu/` folder,
and specify all the test classes that need to be run using the `-Dtest` parameter, for example:

```shell
cd hadoop-cloud-storage-project/hadoop-qiniu/
mvn test -Dtest=ITestQiniuKodoFileSystemContractBase,ITestQiniuKodoContractCreate,ITestQiniuKodoContractDelete,ITestQiniuKodoContractDistCp,ITestQiniuKodoContractGetFileStatus,ITestQiniuKodoContractMkdir,ITestQiniuKodoContractOpen,ITestQiniuKodoContractRename,ITestQiniuKodoContractRootDir,ITestQiniuKodoContractSeek
```

> PS: If you run the `ITestQiniuKodoContractDistCp` test class, some test cases report an error
> `java.lang.NoSuchMethodError`, please first in the outermost `hadoop` repository, execute
> the `mvn install -DskipTests` command