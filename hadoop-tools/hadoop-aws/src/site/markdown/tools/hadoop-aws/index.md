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

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

The `hadoop-aws` module provides support for AWS integration. The generated
JAR file, `hadoop-aws.jar` also declares a transitive dependency on all
external artifacts which are needed for this support —enabling downstream
applications to easily use this support.

To make it part of Apache Hadoop's default classpath, simply make sure that
HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-aws' in the list.

### Features

1. The "classic" `s3:` filesystem for storing objects in Amazon S3 Storage.
**NOTE: `s3:` is being phased out. Use `s3n:` or `s3a:` instead.**
1. The second-generation, `s3n:` filesystem, making it easy to share
data between hadoop and other applications via the S3 object store.
1. The third generation, `s3a:` filesystem. Designed to be a switch in
replacement for `s3n:`, this filesystem binding supports larger files and promises
higher performance.

The specifics of using these filesystems are documented below.

### Warning #1: Object Stores are not filesystems.

Amazon S3 is an example of "an object store". In order to achieve scalability
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

### Warning #2: Because Object stores don't track modification times of directories,
features of Hadoop relying on this can have unexpected behaviour. E.g. the
AggregatedLogDeletionService of YARN will not remove the appropriate logfiles.

For further discussion on these topics, please consult
[The Hadoop FileSystem API Definition](../../../hadoop-project-dist/hadoop-common/filesystem/index.html).

### Warning #3: your AWS credentials are valuable

Your AWS credentials not only pay for services, they offer read and write
access to the data. Anyone with the credentials can not only read your datasets
—they can delete them.

Do not inadvertently share these credentials through means such as
1. Checking in to SCM any configuration files containing the secrets.
1. Logging them to a console, as they invariably end up being seen.
1. Defining filesystem URIs with the credentials in the URL, such as
`s3a://AK0010:secret@landsat/`. They will end up in logs and error messages.
1. Including the secrets in bug reports.

If you do any of these: change your credentials immediately!

### Warning #4: the S3 client provided by Amazon EMR are not from the Apache
Software foundation, and are only supported by Amazon.

Specifically: on Amazon EMR, s3a is not supported, and amazon recommend
a different filesystem implementation. If you are using Amazon EMR, follow
these instructions —and be aware that all issues related to S3 integration
in EMR can only be addressed by Amazon themselves: please raise your issues
with them.

## S3

The `s3://` filesystem is the original S3 store in the Hadoop codebase.
It implements an inode-style filesystem atop S3, and was written to
provide scaleability when S3 had significant limits on the size of blobs.
It is incompatible with any other application's use of data in S3.

It is now deprecated and will be removed in Hadoop 3. Please do not use,
and migrate off data which is on it.

### Dependencies

* `jets3t` jar
* `commons-codec` jar
* `commons-logging` jar
* `httpclient` jar
* `httpcore` jar
* `java-xmlbuilder` jar

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

S3N was the first S3 Filesystem client which used "native" S3 objects, hence
the schema `s3n://`.

### Features

* Directly reads and writes S3 objects.
* Compatible with standard S3 clients.
* Supports partitioned uploads for many-GB objects.
* Available across all Hadoop 2.x releases.

The S3N filesystem client, while widely used, is no longer undergoing
active maintenance except for emergency security issues. There are
known bugs, especially: it reads to end of a stream when closing a read;
this can make `seek()` slow on large files. The reason there has been no
attempt to fix this is that every upgrade of the Jets3t library, while
fixing some problems, has unintentionally introduced new ones in either the changed
Hadoop code, or somewhere in the Jets3t/Httpclient code base.
The number of defects remained constant, they merely moved around.

By freezing the Jets3t jar version and avoiding changes to the code,
we reduce the risk of making things worse.

The S3A filesystem client can read all files created by S3N. Accordingly
it should be used wherever possible.


### Dependencies

* `jets3t` jar
* `commons-codec` jar
* `commons-logging` jar
* `httpclient` jar
* `httpcore` jar
* `java-xmlbuilder` jar


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
      <name>fs.s3.buffer.dir</name>
      <value>${hadoop.tmp.dir}/s3</value>
      <description>Determines where on the local filesystem the s3:/s3n: filesystem
      should store files before sending them to S3
      (or after retrieving them from S3).
      </description>
    </property>

    <property>
      <name>fs.s3.maxRetries</name>
      <value>4</value>
      <description>The maximum number of retries for reading or writing files to
        S3, before we signal failure to the application.
      </description>
    </property>

    <property>
      <name>fs.s3.sleepTimeSeconds</name>
      <value>10</value>
      <description>The number of seconds to sleep between each S3 retry.
      </description>
    </property>

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
      Unset by default, and the only other currently allowable value is AES256.
      </description>
    </property>

## S3A


The S3A filesystem client, prefix `s3a://`, is the S3 client undergoing
active development and maintenance.
While this means that there is a bit of instability
of configuration options and behavior, it also means
that the code is getting better in terms of reliability, performance,
monitoring and other features.

### Features

* Directly reads and writes S3 objects.
* Compatible with standard S3 clients.
* Can read data created with S3N.
* Can write data back that is readable by S3N. (Note: excluding encryption).
* Supports partitioned uploads for many-GB objects.
* Instrumented with Hadoop metrics.
* Performance optimized operations, including `seek()` and `readFully()`.
* Uses Amazon's Java S3 SDK with support for latest S3 features and authentication
schemes.
* Supports authentication via: environment variables, Hadoop configuration
properties, the Hadoop key management store and IAM roles.
* Supports S3 "Server Side Encryption" for both reading and writing.
* Supports proxies
* Test suites includes distcp and suites in downstream projects.
* Available since Hadoop 2.6; considered production ready in Hadoop 2.7.
* Actively maintained.

S3A is now the recommended client for working with S3 objects. It is also the
one where patches for functionality and performance are very welcome.

### Dependencies

* `hadoop-aws` jar.
* `aws-java-sdk-s3` jar.
* `aws-java-sdk-core` jar.
* `aws-java-sdk-kms` jar.
* `joda-time` jar; use version 2.8.1 or later.
* `httpclient` jar.
* Jackson `jackson-core`, `jackson-annotations`, `jackson-databind` jars.

### Authentication properties

    <property>
      <name>fs.s3a.access.key</name>
      <description>AWS access key ID. Omit for IAM role-based or provider-based authentication.</description>
    </property>

    <property>
      <name>fs.s3a.secret.key</name>
      <description>AWS secret key. Omit for IAM role-based or provider-based authentication.</description>
    </property>

    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <description>
        Class name of a credentials provider that implements
        com.amazonaws.auth.AWSCredentialsProvider.  Omit if using access/secret keys
        or another authentication mechanism.  The specified class must provide an
        accessible constructor accepting java.net.URI and
        org.apache.hadoop.conf.Configuration, or an accessible default constructor.
        Specifying org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider allows
        anonymous access to a publicly accessible S3 bucket without any credentials.
        Please note that allowing anonymous access to an S3 bucket compromises
        security and therefore is unsuitable for most use cases.  It can be useful
        for accessing public data sets without requiring AWS credentials.
      </description>
    </property>

    <property>
      <name>fs.s3a.session.token</name>
      <description>Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider as the providers.</description>
    </property>

#### Authentication methods

The standard way to authenticate is with an access key and secret key using the
properties above. You can also avoid configuring credentials if the EC2
instances in your cluster are configured with IAM instance profiles that grant
the appropriate S3 access.

A temporary set of credentials can also be obtained from Amazon STS; these
consist of an access key, a secret key, and a session token. To use these
temporary credentials you must include the `aws-java-sdk-sts` JAR in your
classpath (consult the POM for the current version) and set the
`TemporaryAWSCredentialsProvider` class as the provider. The session key
must be set in the property `fs.s3a.session.token` —and the access and secret
key properties to those of this temporary session.

    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <value>org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider</value>
    </property>

    <property>
      <name>fs.s3a.access.key</name>
      <value>SESSION-ACCESS-KEY</value>
    </property>

    <property>
      <name>fs.s3a.secret.key</name>
      <value>SESSION-SECRET-KEY</value>
    </property>

    <property>
      <name>fs.s3a.session.token</name>
      <value>SECRET-SESSION-TOKEN</value>
    </property>

#### Protecting the AWS Credentials in S3A

To protect the access/secret keys from prying eyes, it is recommended that you
use either IAM role-based authentication (such as EC2 instance profile) or
the credential provider framework securely storing them and accessing them
through configuration. The following describes using the latter for AWS
credentials in S3AFileSystem.

For additional reading on the credential provider API see:
[Credential Provider API](../../../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

#### Authenticating via environment variables

S3A supports configuration via [the standard AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment).

The core environment variables are for the access key and associated secret:

```
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

These environment variables can be used to set the authentication credentials
instead of properties in the Hadoop configuration. *Important:* these
environment variables are not propagated from client to server when
YARN applications are launched. That is: having the AWS environment variables
set when an application is launched will not permit the launched application
to access S3 resources. The environment variables must (somehow) be set
on the hosts/processes where the work is executed.

##### End to End Steps for Distcp and S3 with Credential Providers

###### provision

```
% hadoop credential create fs.s3a.access.key -value 123
    -provider localjceks://file/home/lmccay/aws.jceks
```

```
% hadoop credential create fs.s3a.secret.key -value 456
    -provider localjceks://file/home/lmccay/aws.jceks
```

###### configure core-site.xml or command line system property

```
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/home/lmccay/aws.jceks</value>
  <description>Path to interrogate for protected credentials.</description>
</property>
```
###### distcp

```
% hadoop distcp
    [-D hadoop.security.credential.provider.path=localjceks://file/home/lmccay/aws.jceks]
    hdfs://hostname:9001/user/lmccay/007020615 s3a://lmccay/
```

NOTE: You may optionally add the provider path property to the distcp command line instead of
added job specific configuration to a generic core­site.xml. The square brackets above illustrate
this capability.

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
      <name>fs.s3a.endpoint</name>
      <description>AWS S3 endpoint to connect to. An up-to-date list is
        provided in the AWS Documentation: regions and endpoints. Without this
        property, the standard region (s3.amazonaws.com) is assumed.
      </description>
    </property>

    <property>
      <name>fs.s3a.path.style.access</name>
      <value>false</value>
      <description>Enable S3 path style access ie disabling the default virtual hosting behaviour.
        Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting.
      </description>
    </property>

    <property>
      <name>fs.s3a.proxy.host</name>
      <description>Hostname of the (optional) proxy server for S3 connections.</description>
    </property>

    <property>
      <name>fs.s3a.proxy.port</name>
      <description>Proxy server port. If this property is not set
        but fs.s3a.proxy.host is, port 80 or 443 is assumed (consistent with
        the value of fs.s3a.connection.ssl.enabled).</description>
    </property>

    <property>
      <name>fs.s3a.proxy.username</name>
      <description>Username for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.s3a.proxy.password</name>
      <description>Password for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.s3a.proxy.domain</name>
      <description>Domain for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.s3a.proxy.workstation</name>
      <description>Workstation for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.s3a.attempts.maximum</name>
      <value>20</value>
      <description>How many times we should retry commands on transient errors.</description>
    </property>

    <property>
      <name>fs.s3a.connection.establish.timeout</name>
      <value>5000</value>
      <description>Socket connection setup timeout in milliseconds.</description>
    </property>

    <property>
      <name>fs.s3a.connection.timeout</name>
      <value>200000</value>
      <description>Socket connection timeout in milliseconds.</description>
    </property>

    <property>
      <name>fs.s3a.paging.maximum</name>
      <value>5000</value>
      <description>How many keys to request from S3 when doing
         directory listings at a time.</description>
    </property>

    <property>
      <name>fs.s3a.threads.max</name>
      <value>10</value>
      <description> Maximum number of concurrent active (part)uploads,
      which each use a thread from the threadpool.</description>
    </property>

    <property>
      <name>fs.s3a.threads.keepalivetime</name>
      <value>60</value>
      <description>Number of seconds a thread can be idle before being
        terminated.</description>
    </property>

    <property>
      <name>fs.s3a.max.total.tasks</name>
      <value>5</value>
      <description>Number of (part)uploads allowed to the queue before
      blocking additional uploads.</description>
    </property>

    <property>
      <name>fs.s3a.multipart.size</name>
      <value>104857600</value>
      <description>How big (in bytes) to split upload or copy operations up into.
      This also controls the partition size in renamed files, as rename() involves
      copying the source file(s)</description>
    </property>

    <property>
      <name>fs.s3a.multipart.threshold</name>
      <value>2147483647</value>
      <description>Threshold before uploads or copies use parallel multipart operations.</description>
    </property>

    <property>
      <name>fs.s3a.multiobjectdelete.enable</name>
      <value>true</value>
      <description>When enabled, multiple single-object delete requests are replaced by
        a single 'delete multiple objects'-request, reducing the number of requests.
        Beware: legacy S3-compatible object stores might not support this request.
      </description>
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
      <name>fs.s3a.signing-algorithm</name>
      <description>Override the default signing algorithm so legacy
        implementations can still be used</description>
    </property>

    <property>
      <name>fs.s3a.server-side-encryption-algorithm</name>
      <description>Specify a server-side encryption algorithm for s3a: file system.
        Unset by default, and the only other currently allowable value is AES256.
      </description>
    </property>

    <property>
      <name>fs.s3a.buffer.dir</name>
      <value>${hadoop.tmp.dir}/s3a</value>
      <description>Comma separated list of directories that will be used to buffer file
        uploads to. No effect if fs.s3a.fast.upload is true.</description>
    </property>

    <property>
      <name>fs.s3a.block.size</name>
      <value>33554432</value>
      <description>Block size to use when reading files using s3a: file system.
      </description>
    </property>

    <property>
      <name>fs.s3a.user.agent.prefix</name>
      <value></value>
      <description>
        Sets a custom value that will be prepended to the User-Agent header sent in
        HTTP requests to the S3 back-end by S3AFileSystem.  The User-Agent header
        always includes the Hadoop version number followed by a string generated by
        the AWS SDK.  An example is "User-Agent: Hadoop 2.8.0, aws-sdk-java/1.10.6".
        If this optional property is set, then its value is prepended to create a
        customized User-Agent.  For example, if this configuration property was set
        to "MyApp", then an example of the resulting User-Agent would be
        "User-Agent: MyApp, Hadoop 2.8.0, aws-sdk-java/1.10.6".
      </description>
    </property>

    <property>
      <name>fs.s3a.impl</name>
      <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
      <description>The implementation class of the S3A Filesystem</description>
    </property>

    <property>
      <name>fs.AbstractFileSystem.s3a.impl</name>
      <value>org.apache.hadoop.fs.s3a.S3A</value>
      <description>The implementation class of the S3A AbstractFileSystem.</description>
    </property>

    <property>
      <name>fs.s3a.readahead.range</name>
      <value>65536</value>
      <description>Bytes to read ahead during a seek() before closing and
      re-opening the S3 HTTP connection. This option will be overridden if
      any call to setReadahead() is made to an open stream.</description>
    </property>

### S3AFastOutputStream
 **Warning: NEW in hadoop 2.7. UNSTABLE, EXPERIMENTAL: use at own risk**

    <property>
      <name>fs.s3a.fast.upload</name>
      <value>false</value>
      <description>Upload directly from memory instead of buffering to
      disk first. Memory usage and parallelism can be controlled as up to
      fs.s3a.multipart.size memory is consumed for each (part)upload actively
      uploading (fs.s3a.threads.max) or queueing (fs.s3a.max.total.tasks)</description>
    </property>

    <property>
      <name>fs.s3a.fast.buffer.size</name>
      <value>1048576</value>
      <description>Size (in bytes) of initial memory buffer allocated for an
      upload. No effect if fs.s3a.fast.upload is false.</description>
    </property>

Writes are buffered in memory instead of to a file on local disk. This
removes the throughput bottleneck of the local disk write and read cycle
before starting the actual upload. Furthermore, it allows handling files that
are larger than the remaining local disk space.

However, non-trivial memory tuning is needed for optimal results and careless
settings could cause memory overflow. Up to `fs.s3a.threads.max` parallel
(part)uploads are active. Furthermore, up to `fs.s3a.max.total.tasks`
additional part(uploads) can be waiting (and thus memory buffers are created).
The memory buffer is uploaded as a single upload if it is not larger than
`fs.s3a.multipart.threshold`. Else, a multi-part upload is initiated and
parts of size `fs.s3a.multipart.size` are used to protect against overflowing
the available memory. These settings should be tuned to the envisioned
workflow (some large files, many small ones, ...) and the physical
limitations of the machine and cluster (memory, network bandwidth).

### S3A Experimental "fadvise" input policy support

**Warning: EXPERIMENTAL: behavior may change in future**

The S3A Filesystem client supports the notion of input policies, similar
to that of the Posix `fadvise()` API call. This tunes the behavior of the S3A
client to optimise HTTP GET requests for the different use cases.

#### "sequential" (default)

Read through the file, possibly with some short forward seeks.

The whole document is requested in a single HTTP request; forward seeks
within the readahead range are supported by skipping over the intermediate
data.

This is leads to maximum read throughput —but with very expensive
backward seeks.


#### "normal"

This is currently the same as "sequential".

#### "random"

Optimised for random IO, specifically the Hadoop `PositionedReadable`
operations —though `seek(offset); read(byte_buffer)` also benefits.

Rather than ask for the whole file, the range of the HTTP request is
set to that that of the length of data desired in the `read` operation
(Rounded up to the readahead value set in `setReadahead()` if necessary).

By reducing the cost of closing existing HTTP requests, this is
highly efficient for file IO accessing a binary file
through a series of `PositionedReadable.read()` and `PositionedReadable.readFully()`
calls. Sequential reading of a file is expensive, as now many HTTP requests must
be made to read through the file.

For operations simply reading through a file: copying, distCp, reading
Gzipped or other compressed formats, parsing .csv files, etc, the `sequential`
policy is appropriate. This is the default: S3A does not need to be configured.

For the specific case of high-performance random access IO, the `random` policy
may be considered. The requirements are:

* Data is read using the `PositionedReadable` API.
* Long distance (many MB) forward seeks
* Backward seeks as likely as forward seeks.
* Little or no use of single character `read()` calls or small `read(buffer)`
calls.
* Applications running close to the S3 data store. That is: in EC2 VMs in
the same datacenter as the S3 instance.

The desired fadvise policy must be set in the configuration option
`fs.s3a.experimental.input.fadvise` when the filesystem instance is created.
That is: it can only be set on a per-filesystem basis, not on a per-file-read
basis.

    <property>
      <name>fs.s3a.experimental.input.fadvise</name>
      <value>random</value>
      <description>Policy for reading files.
       Values: 'random', 'sequential' or 'normal'
       </description>
    </property>

[HDFS-2744](https://issues.apache.org/jira/browse/HDFS-2744),
*Extend FSDataInputStream to allow fadvise* proposes adding a public API
to set fadvise policies on input streams. Once implemented,
this will become the supported mechanism used for configuring the input IO policy.

## Troubleshooting S3A

Common problems working with S3A are

1. Classpath
1. Authentication
1. S3 Inconsistency side-effects

Classpath is usually the first problem. For the S3x filesystem clients,
you need the Hadoop-specific filesystem clients, third party S3 client libraries
compatible with the Hadoop code, and any dependent libraries compatible with
Hadoop and the specific JVM.

The classpath must be set up for the process talking to S3: if this is code
running in the Hadoop cluster, the JARs must be on that classpath. That
includes `distcp`.


### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`

(or `org.apache.hadoop.fs.s3native.NativeS3FileSystem`, `org.apache.hadoop.fs.s3.S3FileSystem`).

These are the Hadoop classes, found in the `hadoop-aws` JAR. An exception
reporting one of these classes is missing means that this JAR is not on
the classpath.

### `ClassNotFoundException: com.amazonaws.services.s3.AmazonS3Client`

(or other `com.amazonaws` class.)
`
This means that one or more of the `aws-*-sdk` JARs are missing. Add them.

### Missing method in AWS class

This can be triggered by incompatibilities between the AWS SDK on the classpath
and the version which Hadoop was compiled with.

The AWS SDK JARs change their signature enough between releases that the only
way to safely update the AWS SDK version is to recompile Hadoop against the later
version.

There's nothing the Hadoop team can do here: if you get this problem, then sorry,
but you are on your own. The Hadoop developer team did look at using reflection
to bind to the SDK, but there were too many changes between versions for this
to work reliably. All it did was postpone version compatibility problems until
the specific codepaths were executed at runtime —this was actually a backward
step in terms of fast detection of compatibility problems.

### Missing method in a Jackson class

This is usually caused by version mismatches between Jackson JARs on the
classpath. All Jackson JARs on the classpath *must* be of the same version.


### Authentication failure

One authentication problem is caused by classpath mismatch; see the joda time
issue above.

Otherwise, the general cause is: you have the wrong credentials —or somehow
the credentials were not readable on the host attempting to read or write
the S3 Bucket.

There's not much that Hadoop can do/does for diagnostics here,
though enabling debug logging for the package `org.apache.hadoop.fs.s3a`
can help.

There is also some logging in the AWS libraries which provide some extra details.
In particular, the setting the log `com.amazonaws.auth.AWSCredentialsProviderChain`
to log at DEBUG level will mean the invidual reasons for the (chained)
authentication clients to fail will be printed.

Otherwise, try to use the AWS command line tools with the same credentials.
If you set the environment variables, you can take advantage of S3A's support
of environment-variable authentication by attempting to use the `hdfs fs` command
to read or write data on S3. That is: comment out the `fs.s3a` secrets and rely on
the environment variables.

S3 Frankfurt is a special case. It uses the V4 authentication API.

### Authentication failures running on Java 8u60+

A change in the Java 8 JVM broke some of the `toString()` string generation
of Joda Time 2.8.0, which stopped the amazon s3 client from being able to
generate authentication headers suitable for validation by S3.

Fix: make sure that the version of Joda Time is 2.8.1 or later.

## Visible S3 Inconsistency

Amazon S3 is *an eventually consistent object store*. That is: not a filesystem.

It offers read-after-create consistency: a newly created file is immediately
visible. Except, there is a small quirk: a negative GET may be cached, such
that even if an object is immediately created, the fact that there "wasn't"
an object is still remembered.

That means the following sequence on its own will be consistent
```
touch(path) -> getFileStatus(path)
```

But this sequence *may* be inconsistent.

```
getFileStatus(path) -> touch(path) -> getFileStatus(path)
```

A common source of visible inconsistencies is that the S3 metadata
database —the part of S3 which serves list requests— is updated asynchronously.
Newly added or deleted files may not be visible in the index, even though direct
operations on the object (`HEAD` and `GET`) succeed.

In S3A, that means the `getFileStatus()` and `open()` operations are more likely
to be consistent with the state of the object store than any directory list
operations (`listStatus()`, `listFiles()`, `listLocatedStatus()`,
`listStatusIterator()`).


### `FileNotFoundException` even though the file was just written.

This can be a sign of consistency problems. It may also surface if there is some
asynchronous file write operation still in progress in the client: the operation
has returned, but the write has not yet completed. While the S3A client code
does block during the `close()` operation, we suspect that asynchronous writes
may be taking place somewhere in the stack —this could explain why parallel tests
fail more often than serialized tests.

### File not found in a directory listing, even though `getFileStatus()` finds it

(Similarly: deleted file found in listing, though `getFileStatus()` reports
that it is not there)

This is a visible sign of updates to the metadata server lagging
behind the state of the underlying filesystem.


### File not visible/saved

The files in an object store are not visible until the write has been completed.
In-progress writes are simply saved to a local file/cached in RAM and only uploaded.
at the end of a write operation. If a process terminated unexpectedly, or failed
to call the `close()` method on an output stream, the pending data will have
been lost.

### File `flush()` and `hflush()` calls do not save data to S3A

Again, this is due to the fact that the data is cached locally until the
`close()` operation. The S3A filesystem cannot be used as a store of data
if it is required that the data is persisted durably after every
`flush()/hflush()` call. This includes resilient logging, HBase-style journalling
and the like. The standard strategy here is to save to HDFS and then copy to S3.

### Other issues

*Performance slow*

S3 is slower to read data than HDFS, even on virtual clusters running on
Amazon EC2.

* HDFS replicates data for faster query performance
* HDFS stores the data on the local hard disks, avoiding network traffic
 if the code can be executed on that host. As EC2 hosts often have their
 network bandwidth throttled, this can make a tangible difference.
* HDFS is significantly faster for many "metadata" operations: listing
the contents of a directory, calling `getFileStatus()` on path,
creating or deleting directories.
* On HDFS, Directory renames and deletes are `O(1)` operations. On
S3 renaming is a very expensive `O(data)` operation which may fail partway through
in which case the final state depends on where the copy+ delete sequence was when it failed.
All the objects are copied, then the original set of objects are deleted, so
a failure should not lose data —it may result in duplicate datasets.
* Because the write only begins on a `close()` operation, it may be in the final
phase of a process where the write starts —this can take so long that some things
can actually time out.
* File IO performing many seek calls/positioned read calls will encounter
performance problems due to the size of the HTTP requests made. On S3a,
the (experimental) fadvise policy "random" can be set to alleviate this at the
expense of sequential read performance and bandwidth.

The slow performance of `rename()` surfaces during the commit phase of work,
including

* The MapReduce FileOutputCommitter.
* DistCp's rename after copy operation.

Both these operations can be significantly slower when S3 is the destination
compared to HDFS or other "real" filesystem.

*Improving S3 load-balancing behavior*

Amazon S3 uses a set of front-end servers to provide access to the underlying data.
The choice of which front-end server to use is handled via load-balancing DNS
service: when the IP address of an S3 bucket is looked up, the choice of which
IP address to return to the client is made based on the the current load
of the front-end servers.

Over time, the load across the front-end changes, so those servers considered
"lightly loaded" will change. If the DNS value is cached for any length of time,
your application may end up talking to an overloaded server. Or, in the case
of failures, trying to talk to a server that is no longer there.

And by default, for historical security reasons in the era of applets,
the DNS TTL of a JVM is "infinity".

To work with AWS better, set the DNS time-to-live of an application which
works with S3 to something lower. See [AWS documentation](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-jvm-ttl.html).


## Testing the S3 filesystem clients

Due to eventual consistency, tests may fail without reason. Transient
failures, which no longer occur upon rerunning the test, should thus be ignored.

To test the S3* filesystem clients, you need to provide two files
which pass in authentication details to the test runner

1. `auth-keys.xml`
1. `core-site.xml`

These are both Hadoop XML configuration files, which must be placed into
`hadoop-tools/hadoop-aws/src/test/resources`.

### `core-site.xml`

This file pre-exists and sources the configurations created
under `auth-keys.xml`.

For most purposes you will not need to edit this file unless you
need to apply a specific, non-default property change during the tests.

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
do not use the bucket for any purpose other than testing. Furthermore, for
s3a, all in-progress multi-part uploads to the bucket will be aborted at the
start of a test (by forcing `fs.s3a.multipart.purge=true`) to clean up the
temporary state of previously failed tests.

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
        <value>s3://test-aws-s3/</value>
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

### File `contract-test-options.xml`

The file `hadoop-tools/hadoop-aws/src/test/resources/contract-test-options.xml`
must be created and configured for the test filesystems.

If a specific file `fs.contract.test.fs.*` test path is not defined for
any of the filesystems, those tests will be skipped.

The standard S3 authentication details must also be provided. This can be
through copy-and-paste of the `auth-keys.xml` credentials, or it can be
through direct XInclude inclusion.

### s3://

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
        href="/home/testuser/.ssh/auth-keys.xml"/>
    
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

This example pulls in the `~/.ssh/auth-keys.xml` file for the credentials.
This provides one single place to keep the keys up to date —and means
that the file `contract-test-options.xml` does not contain any
secret credentials itself. As the auth keys XML file is kept out of the
source code tree, it is not going to get accidentally committed.

### Running Tests against non-AWS storage infrastructures

### S3A session tests

The test `TestS3ATemporaryCredentials` requests a set of temporary
credentials from the STS service, then uses them to authenticate with S3.

If an S3 implementation does not support STS, then the functional test
cases must be disabled:

        <property>
          <name>test.fs.s3a.sts.enabled</name>
          <value>false</value>
        </property>

These tests reqest a temporary set of credentials from the STS service endpoint.
An alternate endpoint may be defined in `test.fs.s3a.sts.endpoint`.

        <property>
          <name>test.fs.s3a.sts.endpoint</name>
          <value>https://sts.example.org/</value>
        </property>

The default is ""; meaning "use the amazon default value".

#### CSV Data source

The `TestS3AInputStreamPerformance` tests require read access to a multi-MB
text file. The default file for these tests is one published by amazon,
[s3a://landsat-pds.s3.amazonaws.com/scene_list.gz](http://landsat-pds.s3.amazonaws.com/scene_list.gz).
This is a gzipped CSV index of other files which amazon serves for open use.

The path to this object is set in the option `fs.s3a.scale.test.csvfile`:

    <property>
      <name>fs.s3a.scale.test.csvfile</name>
      <value>s3a://landsat-pds/scene_list.gz</value>
    </property>

1. If the option is not overridden, the default value is used. This
is hosted in Amazon's US-east datacenter.
1. If the property is empty, tests which require it will be skipped.
1. If the data cannot be read for any reason then the test will fail.
1. If the property is set to a different path, then that data must be readable
and "sufficiently" large.

To test on different S3 endpoints, or alternate infrastructures supporting
the same APIs, the option `fs.s3a.scale.test.csvfile` must therefore be
set to " ", or an object of at least 10MB is uploaded to the object store, and
the `fs.s3a.scale.test.csvfile` option set to its path.

      <property>
        <name>fs.s3a.scale.test.csvfile</name>
        <value> </value>
      </property>


#### Scale test operation count

Some scale tests perform multiple operations (such as creating many directories).

The exact number of operations to perform is configurable in the option
`scale.test.operation.count`

      <property>
        <name>scale.test.operation.count</name>
        <value>10</value>
      </property>

Larger values generate more load, and are recommended when testing locally,
or in batch runs.

Smaller values results in faster test runs, especially when the object
store is a long way away.

Operations which work on directories have a separate option: this controls
the width and depth of tests creating recursive directories. Larger
values create exponentially more directories, with consequent performance
impact.

      <property>
        <name>scale.test.directory.count</name>
        <value>2</value>
      </property>

DistCp tests targeting S3A support a configurable file size.  The default is
10 MB, but the configuration value is expressed in KB so that it can be tuned
smaller to achieve faster test runs.

      <property>
        <name>scale.test.distcp.file.size.kb</name>
        <value>10240</value>
      </property>

### Running the Tests

After completing the configuration, execute the test run through Maven.

    mvn clean test

It's also possible to execute multiple test suites in parallel by enabling the
`parallel-tests` Maven profile.  The tests spend most of their time blocked on
network I/O with the S3 service, so running in parallel tends to complete full
test runs faster.

    mvn -Pparallel-tests clean test

Some tests must run with exclusive access to the S3 bucket, so even with the
`parallel-tests` profile enabled, several test suites will run in serial in a
separate Maven execution step after the parallel tests.

By default, the `parallel-tests` profile runs 4 test suites concurrently.  This
can be tuned by passing the `testsThreadCount` argument.

    mvn -Pparallel-tests -DtestsThreadCount=8 clean test

### Testing against non AWS S3 endpoints.

The S3A filesystem is designed to work with storage endpoints which implement
the S3 protocols to the extent that the amazon S3 SDK is capable of talking
to it. We encourage testing against other filesystems and submissions of patches
which address issues. In particular, we encourage testing of Hadoop release
candidates, as these third-party endpoints get even less testing than the
S3 endpoint itself.


**Disabling the encryption tests**

If the endpoint doesn't support server-side-encryption, these will fail

      <property>
        <name>test.fs.s3a.encryption.enabled</name>
        <value>false</value>
      </property>

Encryption is only used for those specific test suites with `Encryption` in
their classname.
