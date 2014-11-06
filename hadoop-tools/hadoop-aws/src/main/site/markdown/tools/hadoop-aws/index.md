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

# Hadoop-AWS module: Integration with Amazon Web Services

The `hadoop-aws` module provides support for AWS integration. The generated
JAR file, `hadoop-aws.jar` also declares a transitive dependency on all
external artifacts which are needed for this support —enabling downstream
applications to easily use this support.

Features

1. The "classic" `s3:` filesystem for storing objects in Amazon S3 Storage
1. The second-generation, `s3n:` filesystem, making it easy to share
data between hadoop and other applications via the S3 object store
1. The third generation, `s3a:` filesystem. Designed to be a switch in
replacement for `s3n:`, this filesystem binding supports larger files and promises
higher performance.

The specifics of using these filesystems are documented below.

## Warning: Object Stores are not filesystems.

Amazon S3 is an example of "an object store". In order to achieve scalalablity
and especially high availability, S3 has —as many other cloud object stores have
done— relaxed some of the constraints which classic "POSIX" filesystems promise.

Specifically

1. Files that are newly created from the Hadoop Filesystem APIs may not be
immediately visible.
2. File delete and update operations may not immediately propagate. Old
copies of the file may exist for an indeterminate time period.
3. Directory operations: `delete()` and `rename()` are implemented by
recursive file-by-file operations. They take time at least proportional to
the number of files, during which time partial updates may be visible. If
the operations are interrupted, the filesystem is left in an intermediate state.

For further discussion on these topics, please consult
[/filesystem](The Hadoop FileSystem API Definition).

## Warning #2: your AWS credentials are valuable

Your AWS credentials not only pay for services, they offer read and write
access to the data. Anyone with the credentials can not only read your datasets
—they can delete them.

Do not inadvertently share these credentials through means such as
1. Checking in Hadoop configuration files containing the credentials.
1. Logging them to a console, as they invariably end up being seen.

If you do any of these: change your credentials immediately!


## S3

### Authentication properties

    <property>
      <name>fs.s3.awsAccessKeyId</name>
      <description>AWS access key ID</description>
    </property>

    <property>
      <name>fs.s3.awsSecretAccessKey</name>
      <description>AWS secret key</description>
    </property>


## S3N

### Authentication properties

    <property>
      <name>fs.s3n.awsAccessKeyId</name>
      <description>AWS access key ID</description>
    </property>

    <property>
      <name>fs.s3n.awsSecretAccessKey</name>
      <description>AWS secret key</description>
    </property>

### Other properties


    <property>
      <name>fs.s3n.block.size</name>
      <value>67108864</value>
      <description>Block size to use when reading files using the native S3
      filesystem (s3n: URIs).</description>
    </property>

    <property>
      <name>fs.s3n.multipart.uploads.enabled</name>
      <value>false</value>
      <description>Setting this property to true enables multiple uploads to
      native S3 filesystem. When uploading a file, it is split into blocks
      if the size is larger than fs.s3n.multipart.uploads.block.size.
      </description>
    </property>

    <property>
      <name>fs.s3n.multipart.uploads.block.size</name>
      <value>67108864</value>
      <description>The block size for multipart uploads to native S3 filesystem.
      Default size is 64MB.
      </description>
    </property>

    <property>
      <name>fs.s3n.multipart.copy.block.size</name>
      <value>5368709120</value>
      <description>The block size for multipart copy in native S3 filesystem.
      Default size is 5GB.
      </description>
    </property>

    <property>
      <name>fs.s3n.server-side-encryption-algorithm</name>
      <value></value>
      <description>Specify a server-side encryption algorithm for S3.
      The default is NULL, and the only other currently allowable value is AES256.
      </description>
    </property>

## S3A


### Authentication properties

    <property>
      <name>fs.s3a.awsAccessKeyId</name>
      <description>AWS access key ID. Omit for Role-based authentication.</description>
    </property>

    <property>
      <name>fs.s3a.awsSecretAccessKey</name>
      <description>AWS secret key. Omit for Role-based authentication.</description>
    </property>

### Other properties

    <property>
      <name>fs.s3a.connection.maximum</name>
      <value>15</value>
      <description>Controls the maximum number of simultaneous connections to S3.</description>
    </property>

    <property>
      <name>fs.s3a.connection.ssl.enabled</name>
      <value>true</value>
      <description>Enables or disables SSL connections to S3.</description>
    </property>

    <property>
      <name>fs.s3a.attempts.maximum</name>
      <value>10</value>
      <description>How many times we should retry commands on transient errors.</description>
    </property>

    <property>
      <name>fs.s3a.connection.timeout</name>
      <value>5000</value>
      <description>Socket connection timeout in seconds.</description>
    </property>

    <property>
      <name>fs.s3a.paging.maximum</name>
      <value>5000</value>
      <description>How many keys to request from S3 when doing
         directory listings at a time.</description>
    </property>

    <property>
      <name>fs.s3a.multipart.size</name>
      <value>104857600</value>
      <description>How big (in bytes) to split upload or copy operations up into.</description>
    </property>

    <property>
      <name>fs.s3a.multipart.threshold</name>
      <value>2147483647</value>
      <description>Threshold before uploads or copies use parallel multipart operations.</description>
    </property>

    <property>
      <name>fs.s3a.acl.default</name>
      <description>Set a canned ACL for newly created and copied objects. Value may be private,
         public-read, public-read-write, authenticated-read, log-delivery-write,
         bucket-owner-read, or bucket-owner-full-control.</description>
    </property>

    <property>
      <name>fs.s3a.multipart.purge</name>
      <value>false</value>
      <description>True if you want to purge existing multipart uploads that may not have been
         completed/aborted correctly</description>
    </property>

    <property>
      <name>fs.s3a.multipart.purge.age</name>
      <value>86400</value>
      <description>Minimum age in seconds of multipart uploads to purge</description>
    </property>

    <property>
      <name>fs.s3a.buffer.dir</name>
      <value>${hadoop.tmp.dir}/s3a</value>
      <description>Comma separated list of directories that will be used to buffer file
        uploads to.</description>
    </property>

    <property>
      <name>fs.s3a.impl</name>
      <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
      <description>The implementation class of the S3A Filesystem</description>
    </property>


## Testing the S3 filesystem clients

To test the S3* filesystem clients, you need to provide two files
which pass in authentication details to the test runner

1. `auth-keys.xml`
1. `core-site.xml`

These are both Hadoop XML configuration files, which must be placed into
`hadoop-tools/hadoop-aws/src/test/resources`.


### `auth-keys.xml`

The presence of this file triggers the testing of the S3 classes.

Without this file, *none of the tests in this module will be executed*

The XML file must contain all the ID/key information needed to connect
each of the filesystem clients to the object stores, and a URL for
each filesystem for its testing.

1. `test.fs.s3n.name` : the URL of the bucket for S3n tests
1. `test.fs.s3a.name` : the URL of the bucket for S3a tests
2. `test.fs.s3.name` : the URL of the bucket for "S3"  tests

The contents of each bucket will be destroyed during the test process:
do not use the bucket for any purpose other than testing.

Example:

    <configuration>
      
      <property>
        <name>test.fs.s3n.name</name>
        <value>s3n://test-aws-s3n/</value>
      </property>
    
      <property>
        <name>test.fs.s3a.name</name>
        <value>s3a://test-aws-s3a/</value>
      </property>
    
      <property>
        <name>test.fs.s3.name</name>
        <value>s3a://test-aws-s3/</value>
      </property>
  
      <property>
        <name>fs.s3.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>

      <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>

      <property>
        <name>fs.s3a.awsAccessKeyId</name>
        <description>AWS access key ID. Omit for Role-based authentication.</description>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>
  
      <property>
        <name>fs.s3a.awsSecretAccessKey</name>
        <description>AWS secret key. Omit for Role-based authentication.</description>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>
    </configuration>

## File `contract-test-options.xml`

The file `hadoop-tools/hadoop-aws/src/test/resources/contract-test-options.xml`
must be created and configured for the test fileystems.

If a specific file `fs.contract.test.fs.*` test path is not defined for
any of the filesystems, those tests will be skipped.

The standard S3 authentication details must also be provided. This can be
through copy-and-paste of the `auth-keys.xml` credentials, or it can be
through direct XInclude inclustion.

#### s3://

The filesystem name must be defined in the property `fs.contract.test.fs.s3`. 


Example:

      <property>
        <name>fs.contract.test.fs.s3</name>
        <value>s3://test-aws-s3/</value>
      </property>

### s3n://


In the file `src/test/resources/contract-test-options.xml`, the filesystem
name must be defined in the property `fs.contract.test.fs.s3n`.
The standard configuration options to define the S3N authentication details
must also be provided.

Example:

      <property>
        <name>fs.contract.test.fs.s3n</name>
        <value>s3n://test-aws-s3n/</value>
      </property>

### s3a://


In the file `src/test/resources/contract-test-options.xml`, the filesystem
name must be defined in the property `fs.contract.test.fs.s3a`.
The standard configuration options to define the S3N authentication details
must also be provided.

Example:

    <property>
      <name>fs.contract.test.fs.s3a</name>
      <value>s3a://test-aws-s3a/</value>
    </property>

### Complete example of `contract-test-options.xml`



    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <!--
      ~ Licensed to the Apache Software Foundation (ASF) under one
      ~  or more contributor license agreements.  See the NOTICE file
      ~  distributed with this work for additional information
      ~  regarding copyright ownership.  The ASF licenses this file
      ~  to you under the Apache License, Version 2.0 (the
      ~  "License"); you may not use this file except in compliance
      ~  with the License.  You may obtain a copy of the License at
      ~
      ~       http://www.apache.org/licenses/LICENSE-2.0
      ~
      ~  Unless required by applicable law or agreed to in writing, software
      ~  distributed under the License is distributed on an "AS IS" BASIS,
      ~  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      ~  See the License for the specific language governing permissions and
      ~  limitations under the License.
      -->
    
    <configuration>
    
      <include xmlns="http://www.w3.org/2001/XInclude"
        href="auth-keys.xml"/>
    
      <property>
        <name>fs.contract.test.fs.s3</name>
        <value>s3://test-aws-s3/</value>
      </property>


      <property>
        <name>fs.contract.test.fs.s3a</name>
        <value>s3a://test-aws-s3a/</value>
      </property>

      <property>
        <name>fs.contract.test.fs.s3n</name>
        <value>s3n://test-aws-s3n/</value>
      </property>

    </configuration>

This example pulls in the `auth-keys.xml` file for the credentials. 
This provides one single place to keep the keys up to date —and means
that the file `contract-test-options.xml` does not contain any
secret credentials itself.