
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

**NOTE: `s3:` has been phased out. Use `s3n:` or `s3a:` instead.**

1. The second-generation, `s3n:` filesystem, making it easy to share
data between hadoop and other applications via the S3 object store.
1. The third generation, `s3a:` filesystem. Designed to be a switch in
replacement for `s3n:`, this filesystem binding supports larger files and promises
higher performance.

The specifics of using these filesystems are documented below.

### Warning #1: Object Stores are not filesystems

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

### Warning #2: Object stores don't track modification times of directories

Features of Hadoop relying on this can have unexpected behaviour. E.g. the
AggregatedLogDeletionService of YARN will not remove the appropriate logfiles.

For further discussion on these topics, please consult
[The Hadoop FileSystem API Definition](../../../hadoop-project-dist/hadoop-common/filesystem/index.html).

### Warning #3: Object stores have differerent authorization models

The object authorization model of S3 is much different from the file
authorization model of HDFS and traditional file systems.  It is not feasible to
persist file ownership and permissions in S3, so S3A reports stub information
from APIs that would query this metadata:

* File owner is reported as the current user.
* File group also is reported as the current user.  Prior to Apache Hadoop
2.8.0, file group was reported as empty (no group associated), which is a
potential incompatibility problem for scripts that perform positional parsing of
shell output and other clients that expect to find a well-defined group.
* Directory permissions are reported as 777.
* File permissions are reported as 666.

S3A does not really enforce any authorization checks on these stub permissions.
Users authenticate to an S3 bucket using AWS credentials.  It's possible that
object ACLs have been defined to enforce authorization at the S3 side, but this
happens entirely within the S3 service, not within the S3A implementation.

For further discussion on these topics, please consult
[The Hadoop FileSystem API Definition](../../../hadoop-project-dist/hadoop-common/filesystem/index.html).

### Warning #4: Your AWS credentials are valuable

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

### Warning #5: The S3 client provided by Amazon EMR are not from the Apache
Software foundation, and are only supported by Amazon.

Specifically: on Amazon EMR, s3a is not supported, and amazon recommend
a different filesystem implementation. If you are using Amazon EMR, follow
these instructions —and be aware that all issues related to S3 integration
in EMR can only be addressed by Amazon themselves: please raise your issues
with them.

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
      <name>fs.s3n.buffer.dir</name>
      <value>${hadoop.tmp.dir}/s3</value>
      <description>Determines where on the local filesystem the s3n: filesystem
      should store files before sending them to S3
      (or after retrieving them from S3).
      </description>
    </property>

    <property>
      <name>fs.s3n.maxRetries</name>
      <value>4</value>
      <description>The maximum number of retries for reading or writing files to
        S3, before we signal failure to the application.
      </description>
    </property>

    <property>
      <name>fs.s3n.sleepTimeSeconds</name>
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

### S3A Authentication methods

S3A supports multiple authentication mechanisms, and can be configured as to
which mechanisms to use, and the order to use them. Custom implementations
of `com.amazonaws.auth.AWSCredentialsProvider` may also be used.

### Authentication properties

    <property>
      <name>fs.s3a.access.key</name>
      <description>AWS access key ID.
       Omit for IAM role-based or provider-based authentication.</description>
    </property>

    <property>
      <name>fs.s3a.secret.key</name>
      <description>AWS secret key.
       Omit for IAM role-based or provider-based authentication.</description>
    </property>

    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <description>
        Comma-separated class names of credential provider classes which implement
        com.amazonaws.auth.AWSCredentialsProvider.

        These are loaded and queried in sequence for a valid set of credentials.
        Each listed class must implement one of the following means of
        construction, which are attempted in order:
        1. a public constructor accepting java.net.URI and
            org.apache.hadoop.conf.Configuration,
        2. a public static method named getInstance that accepts no
           arguments and returns an instance of
           com.amazonaws.auth.AWSCredentialsProvider, or
        3. a public default constructor.

        Specifying org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider allows
        anonymous access to a publicly accessible S3 bucket without any credentials.
        Please note that allowing anonymous access to an S3 bucket compromises
        security and therefore is unsuitable for most use cases. It can be useful
        for accessing public data sets without requiring AWS credentials.

        If unspecified, then the default list of credential provider classes,
        queried in sequence, is:
        1. org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider: supports
            static configuration of AWS access key ID and secret access key.
            See also fs.s3a.access.key and fs.s3a.secret.key.
        2. com.amazonaws.auth.EnvironmentVariableCredentialsProvider: supports
            configuration of AWS access key ID and secret access key in
            environment variables named AWS_ACCESS_KEY_ID and
            AWS_SECRET_ACCESS_KEY, as documented in the AWS SDK.
        3. org.apache.hadoop.fs.s3a.SharedInstanceProfileCredentialsProvider:
            a shared instance of
            com.amazonaws.auth.InstanceProfileCredentialsProvider from the AWS
            SDK, which supports use of instance profile credentials if running
            in an EC2 VM.  Using this shared instance potentially reduces load
            on the EC2 instance metadata service for multi-threaded
            applications.
      </description>
    </property>

    <property>
      <name>fs.s3a.session.token</name>
      <description>
        Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
        as one of the providers.
      </description>
    </property>


#### Authenticating via environment variables

S3A supports configuration via [the standard AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment).

The core environment variables are for the access key and associated secret:

```
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

If the environment variable `AWS_SESSION_TOKEN` is set, session authentication
using "Temporary Security Credentials" is enabled; the Key ID and secret key
must be set to the credentials for that specific sesssion.

```
export AWS_SESSION_TOKEN=SECRET-SESSION-TOKEN
export AWS_ACCESS_KEY_ID=SESSION-ACCESS-KEY
export AWS_SECRET_ACCESS_KEY=SESSION-SECRET-KEY
```

These environment variables can be used to set the authentication credentials
instead of properties in the Hadoop configuration.

*Important:*
These environment variables are not propagated from client to server when
YARN applications are launched. That is: having the AWS environment variables
set when an application is launched will not permit the launched application
to access S3 resources. The environment variables must (somehow) be set
on the hosts/processes where the work is executed.


#### Changing Authentication Providers

The standard way to authenticate is with an access key and secret key using the
properties in the configuration file.

The S3A client follows the following authentication chain:

1. If login details were provided in the filesystem URI, a warning is printed
and then the username and password extracted for the AWS key and secret respectively.
1. The `fs.s3a.access.key` and `fs.s3a.secret.key` are looked for in the Hadoop
XML configuration.
1. The [AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment),
are then looked for.
1. An attempt is made to query the Amazon EC2 Instance Metadata Service to
 retrieve credentials published to EC2 VMs.

S3A can be configured to obtain client authentication providers from classes
which integrate with the AWS SDK by implementing the `com.amazonaws.auth.AWSCredentialsProvider`
Interface. This is done by listing the implementation classes, in order of
preference, in the configuration option `fs.s3a.aws.credentials.provider`.

*Important*: AWS Credential Providers are distinct from _Hadoop Credential Providers_.
As will be covered later, Hadoop Credential Providers allow passwords and other secrets
to be stored and transferred more securely than in XML configuration files.
AWS Credential Providers are classes which can be used by the Amazon AWS SDK to
obtain an AWS login from a different source in the system, including environment
variables, JVM properties and configuration files.

There are four AWS Credential Providers inside the `hadoop-aws` JAR:

| classname | description |
|-----------|-------------|
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`| Session Credentials |
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`| Simple name/secret credentials |
| `org.apache.hadoop.fs.s3a.SharedInstanceProfileCredentialsProvider`| Shared instance of EC2 Metadata Credentials, which can reduce load on the EC2 instance metadata service.  (See below.) |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`| Anonymous Login |

There are also many in the Amazon SDKs, in particular two which are automatically
set up in the authentication chain:

| classname | description |
|-----------|-------------|
| `com.amazonaws.auth.InstanceProfileCredentialsProvider`| EC2 Metadata Credentials |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`| AWS Environment Variables |


*EC2 Metadata Credentials with `SharedInstanceProfileCredentialsProvider`*

Applications running in EC2 may associate an IAM role with the VM and query the
[EC2 Instance Metadata Service](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)
for credentials to access S3.  Within the AWS SDK, this functionality is
provided by `InstanceProfileCredentialsProvider`.  Heavily multi-threaded
applications may trigger a high volume of calls to the instance metadata service
and trigger throttling: either an HTTP 429 response or a forcible close of the
connection.

To mitigate against this problem, `hadoop-aws` ships with a variant of
`InstanceProfileCredentialsProvider` called
`SharedInstanceProfileCredentialsProvider`.  Using this ensures that all
instances of S3A reuse the same instance profile credentials instead of issuing
a large volume of redundant metadata service calls.  If
`fs.s3a.aws.credentials.provider` refers to
`com.amazonaws.auth.InstanceProfileCredentialsProvider`, S3A automatically uses
`org.apache.hadoop.fs.s3a.SharedInstanceProfileCredentialsProvider` instead.

*Session Credentials with `TemporaryAWSCredentialsProvider`*

[Temporary Security Credentials](http://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html)
can be obtained from the Amazon Security Token Service; these
consist of an access key, a secret key, and a session token.

To authenticate with these:

1. Declare `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider` as the
provider.
1. Set the session key in the property `fs.s3a.session.token`,
and the access and secret key properties to those of this temporary session.

Example:

```xml
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
```

The lifetime of session credentials are fixed when the credentials
are issued; once they expire the application will no longer be able to
authenticate to AWS.

*Anonymous Login with `AnonymousAWSCredentialsProvider`*

Specifying `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` allows
anonymous access to a publicly accessible S3 bucket without any credentials.
It can be useful for accessing public data sets without requiring AWS credentials.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider</value>
</property>
```

Once this is done, there's no need to supply any credentials
in the Hadoop configuration or via environment variables.

This option can be used to verify that an object store does
not permit unauthenticated access: that is, if an attempt to list
a bucket is made using the anonymous credentials, it should fail —unless
explicitly opened up for broader access.

```bash
hadoop fs -ls \
 -D fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
 s3a://landsat-pds/
```

1. Allowing anonymous access to an S3 bucket compromises
security and therefore is unsuitable for most use cases.

1. If a list of credential providers is given in `fs.s3a.aws.credentials.provider`,
then the Anonymous Credential provider *must* come last. If not, credential
providers listed after it will be ignored.

*Simple name/secret credentials with `SimpleAWSCredentialsProvider`*

This is is the standard credential provider, which
supports the secret key in `fs.s3a.access.key` and token in `fs.s3a.secret.key`
values. It does not support authentication with logins credentials declared
in the URLs.

    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>

Apart from its lack of support of user:password details being included in filesystem
URLs (a dangerous practise that is strongly discouraged), this provider acts
exactly at the basic authenticator used in the default authentication chain.

This means that the default S3A authentication chain can be defined as

    <property>
      <name>fs.s3a.aws.credentials.provider</name>
      <value>
      org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
      com.amazonaws.auth.EnvironmentVariableCredentialsProvider,
      org.apache.hadoop.fs.s3a.SharedInstanceProfileCredentialsProvider
      </value>
    </property>


#### Protecting the AWS Credentials

To protect the access/secret keys from prying eyes, it is recommended that you
use either IAM role-based authentication (such as EC2 instance profile) or
the credential provider framework securely storing them and accessing them
through configuration. The following describes using the latter for AWS
credentials in the S3A FileSystem.


##### Storing secrets with Hadoop Credential Providers

The Hadoop Credential Provider Framework allows secure "Credential Providers"
to keep secrets outside Hadoop configuration files, storing them in encrypted
files in local or Hadoop filesystems, and including them in requests.

The S3A configuration options with sensitive data
(`fs.s3a.secret.key`, `fs.s3a.access.key` and `fs.s3a.session.token`) can
have their data saved to a binary file stored, with the values being read in
when the S3A filesystem URL is used for data access. The reference to this
credential provider is all that is passed as a direct configuration option.

For additional reading on the Hadoop Credential Provider API see:
[Credential Provider API](../../../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).


###### Create a credential file

A credential file can be created on any Hadoop filesystem; when creating one on HDFS or
a Unix filesystem the permissions are automatically set to keep the file
private to the reader —though as directory permissions are not touched,
users should verify that the directory containing the file is readable only by
the current user.


```bash
hadoop credential create fs.s3a.access.key -value 123 \
    -provider jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks

hadoop credential create fs.s3a.secret.key -value 456 \
    -provider jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks
```

A credential file can be listed, to see what entries are kept inside it

```bash
hadoop credential list -provider jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks

Listing aliases for CredentialProvider: jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks
fs.s3a.secret.key
fs.s3a.access.key
```
At this point, the credentials are ready for use.

###### Configure the `hadoop.security.credential.provider.path` property

The URL to the provider must be set in the configuration property
`hadoop.security.credential.provider.path`, either on the command line or
in XML configuration files.

```xml
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks</value>
  <description>Path to interrogate for protected credentials.</description>
</property>
```

Because this property only supplies the path to the secrets file, the configuration
option itself is no longer a sensitive item.

###### Using the credentials

Once the provider is set in the Hadoop configuration, hadoop commands
work exactly as if the secrets were in an XML file.

```bash

hadoop distcp \
    hdfs://nn1.example.com:9001/user/backup/007020615 s3a://glacier1/

hadoop fs -ls s3a://glacier1/

```

The path to the provider can also be set on the command line:

```bash
hadoop distcp \
    -D hadoop.security.credential.provider.path=jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks \
    hdfs://nn1.example.com:9001/user/backup/007020615 s3a://glacier1/

hadoop fs \
  -D hadoop.security.credential.provider.path=jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks \
  -ls s3a://glacier1/

```

Because the provider path is not itself a sensitive secret, there is no risk
from placing its declaration on the command line.


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
      <name>fs.s3a.socket.send.buffer</name>
      <value>8192</value>
      <description>Socket send buffer hint to amazon connector. Represented in bytes.</description>
    </property>

    <property>
      <name>fs.s3a.socket.recv.buffer</name>
      <value>8192</value>
      <description>Socket receive buffer hint to amazon connector. Represented in bytes.</description>
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
      <value>100M</value>
      <description>How big (in bytes) to split upload or copy operations up into.
        A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
      </description>
    </property>

    <property>
      <name>fs.s3a.multipart.threshold</name>
      <value>2147483647</value>
      <description>How big (in bytes) to split upload or copy operations up into.
        This also controls the partition size in renamed files, as rename() involves
        copying the source file(s).
        A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
      </description>
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
      <description>Set a canned ACL for newly created and copied objects. Value may be Private,
        PublicRead, PublicReadWrite, AuthenticatedRead, LogDeliveryWrite, BucketOwnerRead,
        or BucketOwnerFullControl.</description>
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
      <value>32M</value>
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
      <value>64K</value>
      <description>Bytes to read ahead during a seek() before closing and
      re-opening the S3 HTTP connection. This option will be overridden if
      any call to setReadahead() is made to an open stream.</description>
    </property>

### Working with buckets in different regions

S3 Buckets are hosted in different regions, the default being US-East.
The client talks to it by default, under the URL `s3.amazonaws.com`

S3A can work with buckets from any region. Each region has its own
S3 endpoint, documented [by Amazon](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region).

1. Applications running in EC2 infrastructure do not pay for IO to/from
*local S3 buckets*. They will be billed for access to remote buckets. Always
use local buckets and local copies of data, wherever possible.
1. The default S3 endpoint can support data IO with any bucket when the V1 request
signing protocol is used.
1. When the V4 signing protocol is used, AWS requires the explicit region endpoint
to be used —hence S3A must be configured to use the specific endpoint. This
is done in the configuration option `fs.s3a.endpoint`.
1. All endpoints other than the default endpoint only support interaction
with buckets local to that S3 instance.

While it is generally simpler to use the default endpoint, working with
V4-signing-only regions (Frankfurt, Seoul) requires the endpoint to be identified.
Expect better performance from direct connections —traceroute will give you some insight.

Examples:

The default endpoint:

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

Frankfurt

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>
```

Seoul
```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.ap-northeast-2.amazonaws.com</value>
</property>
```

If the wrong endpoint is used, the request may fail. This may be reported as a 301/redirect error,
or as a 400 Bad Request.



### <a name="s3a_fast_upload"></a>Stabilizing: S3A Fast Upload


**New in Hadoop 2.7; significantly enhanced in Hadoop 2.9**


Because of the nature of the S3 object store, data written to an S3A `OutputStream`
is not written incrementally —instead, by default, it is buffered to disk
until the stream is closed in its `close()` method.

This can make output slow:

* The execution time for `OutputStream.close()` is proportional to the amount of data
buffered and inversely proportional to the bandwidth. That is `O(data/bandwidth)`.
* The bandwidth is that available from the host to S3: other work in the same
process, server or network at the time of upload may increase the upload time,
hence the duration of the `close()` call.
* If a process uploading data fails before `OutputStream.close()` is called,
all data is lost.
* The disks hosting temporary directories defined in `fs.s3a.buffer.dir` must
have the capacity to store the entire buffered file.

Put succinctly: the further the process is from the S3 endpoint, or the smaller
the EC-hosted VM is, the longer it will take work to complete.

This can create problems in application code:

* Code often assumes that the `close()` call is fast;
 the delays can create bottlenecks in operations.
* Very slow uploads sometimes cause applications to time out. (generally,
threads blocking during the upload stop reporting progress, so trigger timeouts)
* Streaming very large amounts of data may consume all disk space before the upload begins.


Work to addess this began in Hadoop 2.7 with the `S3AFastOutputStream`
[HADOOP-11183](https://issues.apache.org/jira/browse/HADOOP-11183), and
has continued with ` S3ABlockOutputStream`
[HADOOP-13560](https://issues.apache.org/jira/browse/HADOOP-13560).


This adds an alternative output stream, "S3a Fast Upload" which:

1.  Always uploads large files as blocks with the size set by
    `fs.s3a.multipart.size`. That is: the threshold at which multipart uploads
    begin and the size of each upload are identical.
1.  Buffers blocks to disk (default) or in on-heap or off-heap memory.
1.  Uploads blocks in parallel in background threads.
1.  Begins uploading blocks as soon as the buffered data exceeds this partition
    size.
1.  When buffering data to disk, uses the directory/directories listed in
    `fs.s3a.buffer.dir`. The size of data which can be buffered is limited
    to the available disk space.
1.  Generates output statistics as metrics on the filesystem, including
    statistics of active and pending block uploads.
1.  Has the time to `close()` set by the amount of remaning data to upload, rather
    than the total size of the file.

With incremental writes of blocks, "S3A fast upload" offers an upload
time at least as fast as the "classic" mechanism, with significant benefits
on long-lived output streams, and when very large amounts of data are generated.
The in memory buffering mechanims may also  offer speedup when running adjacent to
S3 endpoints, as disks are not used for intermediate data storage.


```xml
<property>
  <name>fs.s3a.fast.upload</name>
  <value>true</value>
  <description>
    Use the incremental block upload mechanism with
    the buffering mechanism set in fs.s3a.fast.upload.buffer.
    The number of threads performing uploads in the filesystem is defined
    by fs.s3a.threads.max; the queue of waiting uploads limited by
    fs.s3a.max.total.tasks.
    The size of each buffer is set by fs.s3a.multipart.size.
  </description>
</property>

<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>disk</value>
  <description>
    The buffering mechanism to use when using S3A fast upload
    (fs.s3a.fast.upload=true). Values: disk, array, bytebuffer.
    This configuration option has no effect if fs.s3a.fast.upload is false.

    "disk" will use the directories listed in fs.s3a.buffer.dir as
    the location(s) to save data prior to being uploaded.

    "array" uses arrays in the JVM heap

    "bytebuffer" uses off-heap memory within the JVM.

    Both "array" and "bytebuffer" will consume memory in a single stream up to the number
    of blocks set by:

        fs.s3a.multipart.size * fs.s3a.fast.upload.active.blocks.

    If using either of these mechanisms, keep this value low

    The total number of threads performing work across all threads is set by
    fs.s3a.threads.max, with fs.s3a.max.total.tasks values setting the number of queued
    work items.
  </description>
</property>

<property>
  <name>fs.s3a.multipart.size</name>
  <value>100M</value>
  <description>How big (in bytes) to split upload or copy operations up into.
    A suffix from the set {K,M,G,T,P} may be used to scale the numeric value.
  </description>
</property>

<property>
  <name>fs.s3a.fast.upload.active.blocks</name>
  <value>8</value>
  <description>
    Maximum Number of blocks a single output stream can have
    active (uploading, or queued to the central FileSystem
    instance's pool of queued operations.

    This stops a single stream overloading the shared thread pool.
  </description>
</property>
```

**Notes**

* If the amount of data written to a stream is below that set in `fs.s3a.multipart.size`,
the upload is performed in the `OutputStream.close()` operation —as with
the original output stream.

* The published Hadoop metrics monitor include live queue length and
upload operation counts, so identifying when there is a backlog of work/
a mismatch between data generation rates and network bandwidth. Per-stream
statistics can also be logged by calling `toString()` on the current stream.

* Incremental writes are not visible; the object can only be listed
or read when the multipart operation completes in the `close()` call, which
will block until the upload is completed.


#### <a name="s3a_fast_upload_disk"></a>Fast Upload with Disk Buffers `fs.s3a.fast.upload.buffer=disk`

When `fs.s3a.fast.upload.buffer` is set to `disk`, all data is buffered
to local hard disks prior to upload. This minimizes the amount of memory
consumed, and so eliminates heap size as the limiting factor in queued uploads
—exactly as the original "direct to disk" buffering used when
`fs.s3a.fast.upload=false`.


```xml
<property>
  <name>fs.s3a.fast.upload</name>
  <value>true</value>
</property>

<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>disk</value>
</property>

```


#### <a name="s3a_fast_upload_bytebuffer"></a>Fast Upload with ByteBuffers: `fs.s3a.fast.upload.buffer=bytebuffer`

When `fs.s3a.fast.upload.buffer` is set to `bytebuffer`, all data is buffered
in "Direct" ByteBuffers prior to upload. This *may* be faster than buffering to disk,
and, if disk space is small (for example, tiny EC2 VMs), there may not
be much disk space to buffer with.

The ByteBuffers are created in the memory of the JVM, but not in the Java Heap itself.
The amount of data which can be buffered is
limited by the Java runtime, the operating system, and, for YARN applications,
the amount of memory requested for each container.

The slower the write bandwidth to S3, the greater the risk of running out
of memory —and so the more care is needed in
[tuning the upload settings](#s3a_fast_upload_thread_tuning).


```xml
<property>
  <name>fs.s3a.fast.upload</name>
  <value>true</value>
</property>

<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>bytebuffer</value>
</property>
```

#### <a name="s3a_fast_upload_array"></a>Fast Upload with Arrays: `fs.s3a.fast.upload.buffer=array`

When `fs.s3a.fast.upload.buffer` is set to `array`, all data is buffered
in byte arrays in the JVM's heap prior to upload.
This *may* be faster than buffering to disk.

This `array` option is similar to the in-memory-only stream offered in
Hadoop 2.7 with `fs.s3a.fast.upload=true`

The amount of data which can be buffered is limited by the available
size of the JVM heap heap. The slower the write bandwidth to S3, the greater
the risk of heap overflows. This risk can be mitigated by
[tuning the upload settings](#s3a_fast_upload_thread_tuning).

```xml
<property>
  <name>fs.s3a.fast.upload</name>
  <value>true</value>
</property>

<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>array</value>
</property>

```
#### <a name="s3a_fast_upload_thread_tuning"></a>S3A Fast Upload Thread Tuning

Both the [Array](#s3a_fast_upload_array) and [Byte buffer](#s3a_fast_upload_bytebuffer)
buffer mechanisms can consume very large amounts of memory, on-heap or
off-heap respectively. The [disk buffer](#s3a_fast_upload_disk) mechanism
does not use much memory up, but will consume hard disk capacity.

If there are many output streams being written to in a single process, the
amount of memory or disk used is the multiple of all stream's active memory/disk use.

Careful tuning may be needed to reduce the risk of running out memory, especially
if the data is buffered in memory.

There are a number parameters which can be tuned:

1. The total number of threads available in the filesystem for data
uploads *or any other queued filesystem operation*. This is set in
`fs.s3a.threads.max`

1. The number of operations which can be queued for execution:, *awaiting
a thread*: `fs.s3a.max.total.tasks`

1. The number of blocks which a single output stream can have active,
that is: being uploaded by a thread, or queued in the filesystem thread queue:
`fs.s3a.fast.upload.active.blocks`

1. How long an idle thread can stay in the thread pool before it is retired: `fs.s3a.threads.keepalivetime`


When the maximum allowed number of active blocks of a single stream is reached,
no more blocks can be uploaded from that stream until one or more of those active
blocks' uploads completes. That is: a `write()` call which would trigger an upload
of a now full datablock, will instead block until there is capacity in the queue.

How does that come together?

* As the pool of threads set in `fs.s3a.threads.max` is shared (and intended
to be used across all threads), a larger number here can allow for more
parallel operations. However, as uploads require network bandwidth, adding more
threads does not guarantee speedup.

* The extra queue of tasks for the thread pool (`fs.s3a.max.total.tasks`)
covers all ongoing background S3A operations (future plans include: parallelized
rename operations, asynchronous directory operations).

* When using memory buffering, a small value of `fs.s3a.fast.upload.active.blocks`
limits the amount of memory which can be consumed per stream.

* When using disk buffering a larger value of `fs.s3a.fast.upload.active.blocks`
does not consume much memory. But it may result in a large number of blocks to
compete with other filesystem operations.


We recommend a low value of `fs.s3a.fast.upload.active.blocks`; enough
to start background upload without overloading other parts of the system,
then experiment to see if higher values deliver more throughtput —especially
from VMs running on EC2.

```xml

<property>
  <name>fs.s3a.fast.upload.active.blocks</name>
  <value>4</value>
  <description>
    Maximum Number of blocks a single output stream can have
    active (uploading, or queued to the central FileSystem
    instance's pool of queued operations.

    This stops a single stream overloading the shared thread pool.
  </description>
</property>

<property>
  <name>fs.s3a.threads.max</name>
  <value>10</value>
  <description>The total number of threads available in the filesystem for data
    uploads *or any other queued filesystem operation*.</description>
</property>

<property>
  <name>fs.s3a.max.total.tasks</name>
  <value>5</value>
  <description>The number of operations which can be queued for execution</description>
</property>

<property>
  <name>fs.s3a.threads.keepalivetime</name>
  <value>60</value>
  <description>Number of seconds a thread can be idle before being
    terminated.</description>
</property>

```


#### <a name="s3a_multipart_purge"></a>Cleaning up After Incremental Upload Failures: `fs.s3a.multipart.purge`


If an incremental streaming operation is interrupted, there may be
intermediate partitions uploaded to S3 —data which will be billed for.

These charges can be reduced by enabling `fs.s3a.multipart.purge`,
and setting a purge time in seconds, such as 86400 seconds —24 hours.
When an S3A FileSystem instance is instantiated with the purge time greater
than zero, it will, on startup, delete all outstanding partition requests
older than this time.

```xml
<property>
  <name>fs.s3a.multipart.purge</name>
  <value>true</value>
  <description>True if you want to purge existing multipart uploads that may not have been
     completed/aborted correctly</description>
</property>

<property>
  <name>fs.s3a.multipart.purge.age</name>
  <value>86400</value>
  <description>Minimum age in seconds of multipart uploads to purge</description>
</property>
```

If an S3A client is instantited with `fs.s3a.multipart.purge=true`,
it will delete all out of date uploads *in the entire bucket*. That is: it will affect all
multipart uploads to that bucket, from all applications.

Leaving `fs.s3a.multipart.purge` to its default, `false`,
means that the client will not make any attempt to reset or change the partition
rate.

The best practise for using this option is to disable multipart purges in
normal use of S3A, enabling only in manual/scheduled housekeeping operations.

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

(or `org.apache.hadoop.fs.s3native.NativeS3FileSystem`).

These are the Hadoop classes, found in the `hadoop-aws` JAR. An exception
reporting one of these classes is missing means that this JAR is not on
the classpath.

### `ClassNotFoundException: com.amazonaws.services.s3.AmazonS3Client`

(or other `com.amazonaws` class.)

This means that one or more of the `aws-*-sdk` JARs are missing. Add them.

### Missing method in `com.amazonaws` class

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

If Hadoop cannot authenticate with the S3 service endpoint,
the client retries a number of times before eventually failing.
When it finally gives up, it will report a message about signature mismatch:

```
com.amazonaws.services.s3.model.AmazonS3Exception:
 The request signature we calculated does not match the signature you provided.
 Check your key and signing method.
  (Service: Amazon S3; Status Code: 403; Error Code: SignatureDoesNotMatch,
```

The likely cause is that you either have the wrong credentials or somehow
the credentials were not readable on the host attempting to read or write
the S3 Bucket.

Enabling debug logging for the package `org.apache.hadoop.fs.s3a`
can help provide more information.

The most common cause is that you have the wrong credentials for any of the current
authentication mechanism(s) —or somehow
the credentials were not readable on the host attempting to read or write
the S3 Bucket. However, there are a couple of system configuration problems
(JVM version, system clock) which also need to be checked.

Most common: there's an error in the configuration properties.


1. Make sure that the name of the bucket is the correct one.
That is: check the URL.

1. Make sure the property names are correct. For S3A, they are
`fs.s3a.access.key` and `fs.s3a.secret.key` —you cannot just copy the S3N
properties and replace `s3n` with `s3a`.

1. Make sure the properties are visible to the process attempting to
talk to the object store. Placing them in `core-site.xml` is the standard
mechanism.

1. If using session authentication, the session may have expired.
Generate a new session token and secret.

1. If using environement variable-based authentication, make sure that the
relevant variables are set in the environment in which the process is running.

The standard first step is: try to use the AWS command line tools with the same
credentials, through a command such as:

    hdfs fs -ls s3a://my-bucket/

Note the trailing "/" here; without that the shell thinks you are trying to list
your home directory under the bucket, which will only exist if explicitly created.


Attempting to list a bucket using inline credentials is a
means of verifying that the key and secret can access a bucket;

    hdfs fs -ls s3a://key:secret@my-bucket/

Do escape any `+` or `/` symbols in the secret, as discussed below, and never
share the URL, logs generated using it, or use such an inline authentication
mechanism in production.

Finally, if you set the environment variables, you can take advantage of S3A's
support of environment-variable authentication by attempting the same ls operation.
That is: unset the `fs.s3a` secrets and rely on the environment variables.

#### Authentication failure due to clock skew

The timestamp is used in signing to S3, so as to
defend against replay attacks. If the system clock is too far behind *or ahead*
of Amazon's, requests will be rejected.

This can surface as the situation where
read requests are allowed, but operations which write to the bucket are denied.

Check the system clock.

#### Authentication failure when using URLs with embedded secrets

If using the (strongly discouraged) mechanism of including the
AWS Key and secret in a URL, then both "+" and "/" symbols need
to encoded in the URL. As many AWS secrets include these characters,
encoding problems are not uncommon.

| symbol | encoded  value|
|-----------|-------------|
| `+` | `%2B` |
| `/` | `%2F` |


As an example, a URL for `bucket` with AWS ID `user1` and secret `a+b/c` would
be represented as

```
s3a://user1:a%2Bb%2Fc@bucket/
```

This technique is only needed when placing secrets in the URL. Again,
this is something users are strongly advised against using.

#### Authentication Failures When Running on Java 8u60+

A change in the Java 8 JVM broke some of the `toString()` string generation
of Joda Time 2.8.0, which stopped the Amazon S3 client from being able to
generate authentication headers suitable for validation by S3.

**Fix**: Make sure that the version of Joda Time is 2.8.1 or later, or
use a new version of Java 8.


### "Bad Request" exception when working with AWS S3 Frankfurt, Seoul, or other "V4" endpoint


S3 Frankfurt and Seoul *only* support
[the V4 authentication API](http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html).

Requests using the V2 API will be rejected with 400 `Bad Request`

```
$ bin/hadoop fs -ls s3a://frankfurt/
WARN s3a.S3AFileSystem: Client: Amazon S3 error 400: 400 Bad Request; Bad Request (retryable)

com.amazonaws.services.s3.model.AmazonS3Exception: Bad Request (Service: Amazon S3; Status Code: 400; Error Code: 400 Bad Request; Request ID: 923C5D9E75E44C06), S3 Extended Request ID: HDwje6k+ANEeDsM6aJ8+D5gUmNAMguOk2BvZ8PH3g9z0gpH+IuwT7N19oQOnIr5CIx7Vqb/uThE=
	at com.amazonaws.http.AmazonHttpClient.handleErrorResponse(AmazonHttpClient.java:1182)
	at com.amazonaws.http.AmazonHttpClient.executeOneRequest(AmazonHttpClient.java:770)
	at com.amazonaws.http.AmazonHttpClient.executeHelper(AmazonHttpClient.java:489)
	at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:310)
	at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:3785)
	at com.amazonaws.services.s3.AmazonS3Client.headBucket(AmazonS3Client.java:1107)
	at com.amazonaws.services.s3.AmazonS3Client.doesBucketExist(AmazonS3Client.java:1070)
	at org.apache.hadoop.fs.s3a.S3AFileSystem.verifyBucketExists(S3AFileSystem.java:307)
	at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:284)
	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2793)
	at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:101)
	at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2830)
	at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2812)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:389)
	at org.apache.hadoop.fs.Path.getFileSystem(Path.java:356)
	at org.apache.hadoop.fs.shell.PathData.expandAsGlob(PathData.java:325)
	at org.apache.hadoop.fs.shell.Command.expandArgument(Command.java:235)
	at org.apache.hadoop.fs.shell.Command.expandArguments(Command.java:218)
	at org.apache.hadoop.fs.shell.FsCommand.processRawArguments(FsCommand.java:103)
	at org.apache.hadoop.fs.shell.Command.run(Command.java:165)
	at org.apache.hadoop.fs.FsShell.run(FsShell.java:315)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:90)
	at org.apache.hadoop.fs.FsShell.main(FsShell.java:373)
ls: doesBucketExist on frankfurt-new: com.amazonaws.services.s3.model.AmazonS3Exception:
  Bad Request (Service: Amazon S3; Status Code: 400; Error Code: 400 Bad Request;
```

This happens when trying to work with any S3 service which only supports the
"V4" signing API —but the client is configured to use the default S3A service
endpoint.

The S3A client needs to be given the endpoint to use via the `fs.s3a.endpoint`
property.

As an example, the endpoint for S3 Frankfurt is `s3.eu-central-1.amazonaws.com`:

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>
```

### Error message "The bucket you are attempting to access must be addressed using the specified endpoint"

This surfaces when `fs.s3a.endpoint` is configured to use an S3 service endpoint
which is neither the original AWS one, `s3.amazonaws.com` , nor the one where
the bucket is hosted.  The error message contains the redirect target returned
by S3, which can be used to determine the correct value for `fs.s3a.endpoint`.

```
org.apache.hadoop.fs.s3a.AWSS3IOException: Received permanent redirect response
  to bucket.s3-us-west-2.amazonaws.com.  This likely indicates that the S3
  endpoint configured in fs.s3a.endpoint does not match the AWS region
  containing the bucket.: The bucket you are attempting to access must be
  addressed using the specified endpoint. Please send all future requests to
  this endpoint. (Service: Amazon S3; Status Code: 301;
  Error Code: PermanentRedirect; Request ID: 7D39EC1021C61B11)
        at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:132)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.initMultipartUploads(S3AFileSystem.java:287)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:203)
        at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2895)
        at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:102)
        at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2932)
        at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2914)
        at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:390)
```

1. Use the [Specific endpoint of the bucket's S3 service](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)
1. If not using "V4" authentication (see above), the original S3 endpoint
can be used:

```
    <property>
      <name>fs.s3a.endpoint</name>
      <value>s3.amazonaws.com</value>
    </property>
```

Using the explicit endpoint for the region is recommended for speed and the
ability to use the V4 signing API.


### "Timeout waiting for connection from pool" when writing to S3A

This happens when using the Block output stream, `fs.s3a.fast.upload=true` and
the thread pool runs out of capacity.

```
[s3a-transfer-shared-pool1-t20] INFO  http.AmazonHttpClient (AmazonHttpClient.java:executeHelper(496)) - Unable to execute HTTP request: Timeout waiting for connection from poolorg.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool
  at org.apache.http.impl.conn.PoolingClientConnectionManager.leaseConnection(PoolingClientConnectionManager.java:230)
  at org.apache.http.impl.conn.PoolingClientConnectionManager$1.getConnection(PoolingClientConnectionManager.java:199)
  at sun.reflect.GeneratedMethodAccessor13.invoke(Unknown Source)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at com.amazonaws.http.conn.ClientConnectionRequestFactory$Handler.invoke(ClientConnectionRequestFactory.java:70)
  at com.amazonaws.http.conn.$Proxy10.getConnection(Unknown Source)
  at org.apache.http.impl.client.DefaultRequestDirector.execute(DefaultRequestDirector.java:424)
  at org.apache.http.impl.client.AbstractHttpClient.doExecute(AbstractHttpClient.java:884)
  at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:82)
  at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:55)
  at com.amazonaws.http.AmazonHttpClient.executeOneRequest(AmazonHttpClient.java:728)
  at com.amazonaws.http.AmazonHttpClient.executeHelper(AmazonHttpClient.java:489)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:310)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:3785)
  at com.amazonaws.services.s3.AmazonS3Client.doUploadPart(AmazonS3Client.java:2921)
  at com.amazonaws.services.s3.AmazonS3Client.uploadPart(AmazonS3Client.java:2906)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.uploadPart(S3AFileSystem.java:1025)
  at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload$1.call(S3ABlockOutputStream.java:360)
  at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload$1.call(S3ABlockOutputStream.java:355)
  at org.apache.hadoop.fs.s3a.BlockingThreadPoolExecutorService$CallableWithPermitRelease.call(BlockingThreadPoolExecutorService.java:239)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
  at java.lang.Thread.run(Thread.java:745)
```

Make sure that `fs.s3a.connection.maximum` is at least larger
than `fs.s3a.threads.max`.

```xml
<property>
  <name>fs.s3a.threads.max</name>
  <value>20</value>
</property>

<property>
  <name>fs.s3a.connection.maximum</name>
  <value>30</value>
</property>
```

### "Timeout waiting for connection from pool" when reading from S3A

This happens when more threads are trying to read from an S3A system than
the maximum number of allocated HTTP connections.

Set `fs.s3a.connection.maximum` to a larger value (and at least as large as
`fs.s3a.threads.max`)

### Out of heap memory when writing to S3A via Fast Upload

This can happen when using the fast upload mechanism (`fs.s3a.fast.upload=true`)
and in-memory buffering (either `fs.s3a.fast.upload.buffer=array` or
`fs.s3a.fast.upload.buffer=bytebuffer`).

More data is being generated than in the JVM than it can upload to S3 —and
so much data has been buffered that the JVM has run out of memory.

Consult [S3A Fast Upload Thread Tuning](#s3a_fast_upload_thread_tuning) for
detail on this issue and options to address it. Consider also buffering to
disk, rather than memory.


### When writing to S3A: "java.io.FileNotFoundException: Completing multi-part upload"


```
java.io.FileNotFoundException: Completing multi-part upload on fork-5/test/multipart/1c397ca6-9dfb-4ac1-9cf7-db666673246b: com.amazonaws.services.s3.model.AmazonS3Exception: The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload; Request ID: 84FF8057174D9369), S3 Extended Request ID: Ij5Yn6Eq/qIERH4Z6Io3YL2t9/qNZ7z9gjPb1FrTtTovZ8k1MXqh+zCYYjqmfJ/fCY6E1+JR9jA=
  at com.amazonaws.http.AmazonHttpClient.handleErrorResponse(AmazonHttpClient.java:1182)
  at com.amazonaws.http.AmazonHttpClient.executeOneRequest(AmazonHttpClient.java:770)
  at com.amazonaws.http.AmazonHttpClient.executeHelper(AmazonHttpClient.java:489)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:310)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:3785)
  at com.amazonaws.services.s3.AmazonS3Client.completeMultipartUpload(AmazonS3Client.java:2705)
  at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload.complete(S3ABlockOutputStream.java:473)
  at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload.access$200(S3ABlockOutputStream.java:382)
  at org.apache.hadoop.fs.s3a.S3ABlockOutputStream.close(S3ABlockOutputStream.java:272)
  at org.apache.hadoop.fs.FSDataOutputStream$PositionCache.close(FSDataOutputStream.java:72)
  at org.apache.hadoop.fs.FSDataOutputStream.close(FSDataOutputStream.java:106)
```

This surfaces if, while a multipart upload was taking place, all outstanding multipart
uploads were garbage collected. The upload operation cannot complete because
the data uploaded has been deleted.

Consult [Cleaning up After Incremental Upload Failures](#s3a_multipart_purge) for
details on how the multipart purge timeout can be set. If multipart uploads
are failing with the message above, it may be a sign that this value is too low.

### When writing to S3A, HTTP Exceptions logged at info from `AmazonHttpClient`

```
[s3a-transfer-shared-pool4-t6] INFO  http.AmazonHttpClient (AmazonHttpClient.java:executeHelper(496)) - Unable to execute HTTP request: hwdev-steve-ireland-new.s3.amazonaws.com:443 failed to respond
org.apache.http.NoHttpResponseException: bucket.s3.amazonaws.com:443 failed to respond
  at org.apache.http.impl.conn.DefaultHttpResponseParser.parseHead(DefaultHttpResponseParser.java:143)
  at org.apache.http.impl.conn.DefaultHttpResponseParser.parseHead(DefaultHttpResponseParser.java:57)
  at org.apache.http.impl.io.AbstractMessageParser.parse(AbstractMessageParser.java:261)
  at org.apache.http.impl.AbstractHttpClientConnection.receiveResponseHeader(AbstractHttpClientConnection.java:283)
  at org.apache.http.impl.conn.DefaultClientConnection.receiveResponseHeader(DefaultClientConnection.java:259)
  at org.apache.http.impl.conn.ManagedClientConnectionImpl.receiveResponseHeader(ManagedClientConnectionImpl.java:209)
  at org.apache.http.protocol.HttpRequestExecutor.doReceiveResponse(HttpRequestExecutor.java:272)
  at com.amazonaws.http.protocol.SdkHttpRequestExecutor.doReceiveResponse(SdkHttpRequestExecutor.java:66)
  at org.apache.http.protocol.HttpRequestExecutor.execute(HttpRequestExecutor.java:124)
  at org.apache.http.impl.client.DefaultRequestDirector.tryExecute(DefaultRequestDirector.java:686)
  at org.apache.http.impl.client.DefaultRequestDirector.execute(DefaultRequestDirector.java:488)
  at org.apache.http.impl.client.AbstractHttpClient.doExecute(AbstractHttpClient.java:884)
  at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:82)
  at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:55)
  at com.amazonaws.http.AmazonHttpClient.executeOneRequest(AmazonHttpClient.java:728)
  at com.amazonaws.http.AmazonHttpClient.executeHelper(AmazonHttpClient.java:489)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:310)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:3785)
  at com.amazonaws.services.s3.AmazonS3Client.copyPart(AmazonS3Client.java:1731)
  at com.amazonaws.services.s3.transfer.internal.CopyPartCallable.call(CopyPartCallable.java:41)
  at com.amazonaws.services.s3.transfer.internal.CopyPartCallable.call(CopyPartCallable.java:28)
  at org.apache.hadoop.fs.s3a.BlockingThreadPoolExecutorService$CallableWithPermitRelease.call(BlockingThreadPoolExecutorService.java:239)
  at java.util.concurrent.FutureTask.run(FutureTask.java:266)
  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
  at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
  at java.lang.Thread.run(Thread.java:745)
```

These are HTTP I/O exceptions caught and logged inside the AWS SDK. The client
will attempt to retry the operation; it may just be a transient event. If there
are many such exceptions in logs, it may be a symptom of connectivity or network
problems.

### Visible S3 Inconsistency

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

* The MapReduce `FileOutputCommitter`.
* DistCp's rename-after-copy operation.
* The `hdfs fs -rm` command renaming the file under `.Trash` rather than
deleting it. Use `-skipTrash` to eliminate that step.

These operations can be significantly slower when S3 is the destination
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

This module includes both unit tests, which can run in isolation without
connecting to the S3 service, and integration tests, which require a working
connection to S3 to interact with a bucket.  Unit test suites follow the naming
convention `Test*.java`.  Integration tests follow the naming convention
`ITest*.java`.

Due to eventual consistency, integration tests may fail without reason.
Transient failures, which no longer occur upon rerunning the test, should thus
be ignored.

To integration test the S3* filesystem clients, you need to provide two files
which pass in authentication details to the test runner.

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

Without this file, *none of the integration tests in this module will be
executed*.

The XML file must contain all the ID/key information needed to connect
each of the filesystem clients to the object stores, and a URL for
each filesystem for its testing.

1. `test.fs.s3n.name` : the URL of the bucket for S3n tests
1. `test.fs.s3a.name` : the URL of the bucket for S3a tests

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


### Running the Tests

After completing the configuration, execute the test run through Maven.

    mvn clean verify

It's also possible to execute multiple test suites in parallel by passing the
`parallel-tests` property on the command line.  The tests spend most of their
time blocked on network I/O with the S3 service, so running in parallel tends to
complete full test runs faster.

    mvn -Dparallel-tests clean verify

Some tests must run with exclusive access to the S3 bucket, so even with the
`parallel-tests` property, several test suites will run in serial in a separate
Maven execution step after the parallel tests.

By default, `parallel-tests` runs 4 test suites concurrently.  This can be tuned
by passing the `testsThreadCount` property.

    mvn -Dparallel-tests -DtestsThreadCount=8 clean verify

To run just unit tests, which do not require S3 connectivity or AWS credentials,
use any of the above invocations, but switch the goal to `test` instead of
`verify`.

    mvn clean test

    mvn -Dparallel-tests clean test

    mvn -Dparallel-tests -DtestsThreadCount=8 clean test

To run only a specific named subset of tests, pass the `test` property for unit
tests or the `it.test` property for integration tests.

    mvn clean test -Dtest=TestS3AInputPolicies

    mvn clean verify -Dit.test=ITestS3AFileContextStatistics -Dtest=none

    mvn clean verify -Dtest=TestS3A* -Dit.test=ITestS3A*

Note that when running a specific subset of tests, the patterns passed in `test`
and `it.test` override the configuration of which tests need to run in isolation
in a separate serial phase (mentioned above).  This can cause unpredictable
results, so the recommendation is to avoid passing `parallel-tests` in
combination with `test` or `it.test`.  If you know that you are specifying only
tests that can run safely in parallel, then it will work.  For wide patterns,
like `ITestS3A*` shown above, it may cause unpredictable test failures.

### Testing against different regions

S3A can connect to different regions —the tests support this. Simply
define the target region in `contract-tests.xml` or any `auth-keys.xml`
file referenced.

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>
```
This is used for all tests expect for scale tests using a Public CSV.gz file
(see below)

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

### CSV Data source Tests

The `TestS3AInputStreamPerformance` tests require read access to a multi-MB
text file. The default file for these tests is one published by amazon,
[s3a://landsat-pds.s3.amazonaws.com/scene_list.gz](http://landsat-pds.s3.amazonaws.com/scene_list.gz).
This is a gzipped CSV index of other files which amazon serves for open use.

The path to this object is set in the option `fs.s3a.scale.test.csvfile`,

    <property>
      <name>fs.s3a.scale.test.csvfile</name>
      <value>s3a://landsat-pds/scene_list.gz</value>
    </property>

1. If the option is not overridden, the default value is used. This
is hosted in Amazon's US-east datacenter.
1. If `fs.s3a.scale.test.csvfile` is empty, tests which require it will be skipped.
1. If the data cannot be read for any reason then the test will fail.
1. If the property is set to a different path, then that data must be readable
and "sufficiently" large.

To test on different S3 endpoints, or alternate infrastructures supporting
the same APIs, the option `fs.s3a.scale.test.csvfile` must either be
set to " ", or an object of at least 10MB is uploaded to the object store, and
the `fs.s3a.scale.test.csvfile` option set to its path.

```xml
<property>
  <name>fs.s3a.scale.test.csvfile</name>
  <value> </value>
</property>
```

(the reason the space or newline is needed is to add "an empty entry"; an empty
`<value/>` would be considered undefined and pick up the default)

*Note:* if using a test file in an S3 region requiring a different endpoint value
set in `fs.s3a.endpoint`, define it in `fs.s3a.scale.test.csvfile.endpoint`.
If the default CSV file is used, the tests will automatically use the us-east
endpoint:

```xml
<property>
  <name>fs.s3a.scale.test.csvfile.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```
### Viewing Integration Test Reports


Integration test results and logs are stored in `target/failsafe-reports/`.
An HTML report can be generated during site generation, or with the `surefire-report`
plugin:

```
mvn surefire-report:failsafe-report-only
```
### Scale Tests

There are a set of tests designed to measure the scalability and performance
at scale of the S3A tests, *Scale Tests*. Tests include: creating
and traversing directory trees, uploading large files, renaming them,
deleting them, seeking through the files, performing random IO, and others.
This makes them a foundational part of the benchmarking.

By their very nature they are slow. And, as their execution time is often
limited by bandwidth between the computer running the tests and the S3 endpoint,
parallel execution does not speed these tests up.

#### Enabling the Scale Tests

The tests are enabled if the `scale` property is set in the maven build
this can be done regardless of whether or not the parallel test profile
is used

```bash
mvn verify -Dscale

mvn verify -Dparallel-tests -Dscale -DtestsThreadCount=8
```

The most bandwidth intensive tests (those which upload data) always run
sequentially; those which are slow due to HTTPS setup costs or server-side
actionsare included in the set of parallelized tests.


#### Maven build tuning options


Some of the tests can be tuned from the maven build or from the
configuration file used to run the tests.

```bash
mvn verify -Dscale -Dfs.s3a.scale.test.huge.filesize=128M
```

The algorithm is

1. The value is queried from the configuration file, using a default value if
it is not set.
1. The value is queried from the JVM System Properties, where it is passed
down by maven.
1. If the system property is null, empty, or it has the value `unset`, then
the configuration value is used. The `unset` option is used to
[work round a quirk in maven property propagation](http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven).

Only a few properties can be set this way; more will be added.

| Property | Meaninging |
|-----------|-------------|
| `fs.s3a.scale.test.timeout`| Timeout in seconds for scale tests |
| `fs.s3a.scale.test.huge.filesize`| Size for huge file uploads |
| `fs.s3a.scale.test.huge.huge.partitionsize`| Size for partitions in huge file uploads |

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

##### `fs.s3a.scale.test.huge.filesize`: size in MB for "Huge file tests".

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
