# Testing the hadoop-qiniu Module

To test `kodo://` filesystem client，two files in `hadoop-tols/hadoop-qiniu/src/test/resources` which pass in
authentication details to the test runner are needed.

1. `auth-keys.xml`

2. `core-site.xml`

## `core-site.xml`

This file is pre-exists. For most cases, no modification is needed, unless a specific, non-default property needs to be
set during the testing.

## `auth-keys.xml`

This file will trigger the testing of Qiniu Kodo module. Without the file,
*none of the tests in this module will be executed*
这个文件将触发 Qiniu Kodo 模块，如果没有这个文件，这个模块将不会执行任何测试

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

## 运行 Hadoop 契约测试

Create file `contract-test-options.xml` in directory `hadoop-tols/hadoop-qiniu/src/test/resources`, If a
specific file `fs.contract.test.fs.kodo` test path is not defined, those tests will be skipped.
Credentials are also needed to run any of those tests, they can be copied from `auth-keys.xml` or through direct
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
</configuration>
```

