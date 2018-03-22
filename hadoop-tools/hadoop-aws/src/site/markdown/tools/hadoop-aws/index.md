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

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

**NOTE:  Hadoop's `s3:` and `s3n:` connectors have been removed.
Please use `s3a:` as the connector to data hosted in S3 with Apache Hadoop.**

**Consult the [s3n documentation](./s3n.html) for migration instructions.**


See also:

* [Encryption](./encryption.html)
* [Performance](./performance.html)
* [S3Guard](./s3guard.html)
* [Troubleshooting](./troubleshooting_s3a.html)
* [Committing work to S3 with the "S3A Committers"](./committers.html)
* [S3A Committers Architecture](./committer_architecture.html)
* [Working with IAM Assumed Roles](./assumed_roles.html)
* [Testing](./testing.html)

##<a name="overview"></a> Overview

Apache Hadoop's `hadoop-aws` module provides support for AWS integration.
applications to easily use this support.

To include the S3A client in Apache Hadoop's default classpath:

1. Make sure that`HADOOP_OPTIONAL_TOOLS` in `hadoop-env.sh` includes `hadoop-aws`
in its list of optional modules to add in the classpath.

1. For client side interaction, you can declare that relevant JARs must be loaded
in your `~/.hadooprc` file:

        hadoop_add_to_classpath_tools hadoop-aws

The settings in this file does not propagate to deployed applications, but it will
work for local clients such as the `hadoop fs` command.


## <a name="introduction"></a> Introducing the Hadoop S3A client.

Hadoop's "S3A" client offers high-performance IO against Amazon S3 object store
and compatible implementations.

* Directly reads and writes S3 objects.
* Compatible with standard S3 clients.
* Compatible with files created by the older `s3n://` client and Amazon EMR's `s3://` client.
* Supports partitioned uploads for many-GB objects.
* Offers a high-performance random IO mode for working with columnar data such
as Apache ORC and Apache Parquet files.
* Uses Amazon's Java S3 SDK with support for latest S3 features and authentication
schemes.
* Supports authentication via: environment variables, Hadoop configuration
properties, the Hadoop key management store and IAM roles.
* Supports per-bucket configuration.
* With [S3Guard](./s3guard.html), adds high performance and consistent metadata/
directory read operations. This delivers consistency as well as speed.
* Supports S3 "Server Side Encryption" for both reading and writing:
 SSE-S3, SSE-KMS and SSE-C
* Instrumented with Hadoop metrics.
* Actively maintained by the open source community.


### Other S3 Connectors

There other Hadoop connectors to S3. Only S3A is actively maintained by
the Hadoop project itself.

1. Apache's Hadoop's original `s3://` client. This is no longer included in Hadoop.
1. Amazon EMR's `s3://` client. This is from the Amazon EMR team, who actively
maintain it.
1. Apache's Hadoop's [`s3n:` filesystem client](./s3n.html).
   This connector is no longer available: users must migrate to the newer `s3a:` client.


##<a name="getting_started"></a> Getting Started

S3A depends upon two JARs, alongside `hadoop-common` and its dependencies.

* `hadoop-aws` JAR.
* `aws-java-sdk-bundle` JAR.

The versions of `hadoop-common` and `hadoop-aws` must be identical.

To import the libraries into a Maven build, add `hadoop-aws` JAR to the
build dependencies; it will pull in a compatible aws-sdk JAR.

The `hadoop-aws` JAR *does not* declare any dependencies other than that
dependencies unique to it, the AWS SDK JAR. This is simplify excluding/tuning
Hadoop dependency JARs in downstream applications. The `hadoop-client` or
`hadoop-common` dependency must be declared


```xml
<properties>
 <!-- Your exact Hadoop version here-->
  <hadoop.version>3.0.0</hadoop.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
</dependencies>
```

## <a name="warning"></a> Warnings

Amazon S3 is an example of "an object store". In order to achieve scalability
and especially high availability, S3 has —as many other cloud object stores have
done— relaxed some of the constraints which classic "POSIX" filesystems promise.

The [S3Guard](./s3guard.html) feature attempts to address some of these, but
it cannot do so completely. Do read these warnings and consider how
they apply.

For further discussion on these topics, please consult
[The Hadoop FileSystem API Definition](../../../hadoop-project-dist/hadoop-common/filesystem/index.html).

### Warning #1: S3 Consistency model

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


### Warning #2: Directories are mimiced

The S3A clients mimics directories by:

1. Creating a stub entry after a `mkdirs` call, deleting it when a file
is added anywhere underneath
1. When listing a directory, searching for all objects whose path starts with
the directory path, and returning them as the listing.
1. When renaming a directory, taking such a listing and asking S3 to copying the
individual objects to new objects with the destination filenames.
1. When deleting a directory, taking such a listing and deleting the entries in
batches.
1. When renaming or deleting directories, taking such a listing and working
on the individual files.


Here are some of the consequences:

* Directories may lack modification times.
Parts of Hadoop relying on this can have unexpected behaviour. E.g. the
`AggregatedLogDeletionService` of YARN will not remove the appropriate logfiles.
* Directory listing can be slow. Use `listFiles(path, recursive)` for high
performance recursive listings whenever possible.
* It is possible to create files under files if the caller tries hard.
* The time to rename a directory is proportional to the number of files
underneath it (directory or indirectly) and the size of the files. (The copyis
executed inside the S3 storage, so the time is independent of the bandwidth
from client to S3).
* Directory renames are not atomic: they can fail partway through, and callers
cannot safely rely on atomic renames as part of a commit algorithm.
* Directory deletion is not atomic and can fail partway through.

The final three issues surface when using S3 as the immediate destination
of work, as opposed to HDFS or other "real" filesystem.

The [S3A committers](./committers.html) are the sole mechanism available
to safely save the output of queries directly into S3 object stores
through the S3A filesystem.


### Warning #3: Object stores have differerent authorization models

The object authorization model of S3 is much different from the file
authorization model of HDFS and traditional file systems.
The S3A client simply reports stub information from APIs that would query this metadata:

* File owner is reported as the current user.
* File group also is reported as the current user. Prior to Apache Hadoop
2.8.0, file group was reported as empty (no group associated), which is a
potential incompatibility problem for scripts that perform positional parsing of
shell output and other clients that expect to find a well-defined group.
* Directory permissions are reported as 777.
* File permissions are reported as 666.

S3A does not really enforce any authorization checks on these stub permissions.
Users authenticate to an S3 bucket using AWS credentials.  It's possible that
object ACLs have been defined to enforce authorization at the S3 side, but this
happens entirely within the S3 service, not within the S3A implementation.

### Warning #4: Your AWS credentials are very, very valuable

Your AWS credentials not only pay for services, they offer read and write
access to the data. Anyone with the credentials can not only read your datasets
—they can delete them.

Do not inadvertently share these credentials through means such as

1. Checking in to SCM any configuration files containing the secrets.
1. Logging them to a console, as they invariably end up being seen.
1. Defining filesystem URIs with the credentials in the URL, such as
`s3a://AK0010:secret@landsat-pds/`. They will end up in logs and error messages.
1. Including the secrets in bug reports.

If you do any of these: change your credentials immediately!


## <a name="authenticating"></a> Authenticating with S3

Except when interacting with public S3 buckets, the S3A client
needs the credentials needed to interact with buckets.

The client supports multiple authentication mechanisms and can be configured as to
which mechanisms to use, and their order of use. Custom implementations
of `com.amazonaws.auth.AWSCredentialsProvider` may also be used.

### Authentication properties

```xml
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
    3. com.amazonaws.auth.InstanceProfileCredentialsProvider: supports use
        of instance profile credentials if running in an EC2 VM.
  </description>
</property>

<property>
  <name>fs.s3a.session.token</name>
  <description>
    Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
    as one of the providers.
  </description>
</property>
```

### <a name="auth_env_vars"></a> Authenticating via the AWS Environment Variables

S3A supports configuration via [the standard AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment).

The core environment variables are for the access key and associated secret:

```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

If the environment variable `AWS_SESSION_TOKEN` is set, session authentication
using "Temporary Security Credentials" is enabled; the Key ID and secret key
must be set to the credentials for that specific sesssion.

```bash
export AWS_SESSION_TOKEN=SECRET-SESSION-TOKEN
export AWS_ACCESS_KEY_ID=SESSION-ACCESS-KEY
export AWS_SECRET_ACCESS_KEY=SESSION-SECRET-KEY
```

These environment variables can be used to set the authentication credentials
instead of properties in the Hadoop configuration.

*Important:*
These environment variables are generally not propagated from client to server when
YARN applications are launched. That is: having the AWS environment variables
set when an application is launched will not permit the launched application
to access S3 resources. The environment variables must (somehow) be set
on the hosts/processes where the work is executed.

### <a name="auth_providers"></a> Changing Authentication Providers

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

There are three AWS Credential Providers inside the `hadoop-aws` JAR:

| classname | description |
|-----------|-------------|
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`| Session Credentials |
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`| Simple name/secret credentials |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`| Anonymous Login |

There are also many in the Amazon SDKs, in particular two which are automatically
set up in the authentication chain:

| classname | description |
|-----------|-------------|
| `com.amazonaws.auth.InstanceProfileCredentialsProvider`| EC2 Metadata Credentials |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`| AWS Environment Variables |


### <a name="auth_iam"></a> EC2 IAM Metadata Authentication with `InstanceProfileCredentialsProvider`

Applications running in EC2 may associate an IAM role with the VM and query the
[EC2 Instance Metadata Service](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)
for credentials to access S3.  Within the AWS SDK, this functionality is
provided by `InstanceProfileCredentialsProvider`, which internally enforces a
singleton instance in order to prevent throttling problem.

### <a name="auth_session"></a> Using Session Credentials with `TemporaryAWSCredentialsProvider`

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

### <a name="auth_anon"></a> Anonymous Login with `AnonymousAWSCredentialsProvider`

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

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
</property>
```

Apart from its lack of support of user:password details being included in filesystem
URLs (a dangerous practise that is strongly discouraged), this provider acts
exactly at the basic authenticator used in the default authentication chain.

This means that the default S3A authentication chain can be defined as

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>
  org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
  com.amazonaws.auth.EnvironmentVariableCredentialsProvider,
  com.amazonaws.auth.InstanceProfileCredentialsProvider
  </value>
</property>
```

## <a name="auth_security"></a> Protecting the AWS Credentials

It is critical that you never share or leak your AWS credentials.
Loss of credentials can leak/lose all your data, run up large bills,
and significantly damage your organisation.

1. Never share your secrets.

1. Never commit your secrets into an SCM repository.
The [git secrets](https://github.com/awslabs/git-secrets) can help here.

1. Avoid using s3a URLs which have key and secret in the URL. This
is dangerous as the secrets leak into the logs.

1. Never include AWS credentials in bug reports, files attached to them,
or similar.

1. If you use the `AWS_` environment variables,  your list of environment variables
is equally sensitive.

1. Never use root credentials.
Use IAM user accounts, with each user/application having its own set of credentials.

1. Use IAM permissions to restrict the permissions individual users and applications
have. This is best done through roles, rather than configuring individual users.

1. Avoid passing in secrets to Hadoop applications/commands on the command line.
The command line of any launched program is visible to all users on a Unix system
(via `ps`), and preserved in command histories.

1. Explore using [IAM Assumed Roles](assumed_roles.html) for role-based permissions
management: a specific S3A connection can be made with a different assumed role
and permissions from the primary user account.

1. Consider a workflow in which usera and applications are issued with short-lived
session credentials, configuring S3A to use these through
the `TemporaryAWSCredentialsProvider`.

1. Have a secure process in place for cancelling and re-issuing credentials for
users and applications. Test it regularly by using it to refresh credentials.

When running in EC2, the IAM EC2 instance credential provider will automatically
obtain the credentials needed to access AWS services in the role the EC2 VM
was deployed as.
This credential provider is enabled in S3A by default.

The safest way to keep the AWS login keys a secret within Hadoop is to use
Hadoop Credentials.

## <a name="hadoop_credential_providers"></a>Storing secrets with Hadoop Credential Providers

The Hadoop Credential Provider Framework allows secure "Credential Providers"
to keep secrets outside Hadoop configuration files, storing them in encrypted
files in local or Hadoop filesystems, and including them in requests.

The S3A configuration options with sensitive data
(`fs.s3a.secret.key`, `fs.s3a.access.key`,  `fs.s3a.session.token`
and `fs.s3a.server-side-encryption.key`) can
have their data saved to a binary file stored, with the values being read in
when the S3A filesystem URL is used for data access. The reference to this
credential provider then declareed in the hadoop configuration.

For additional reading on the Hadoop Credential Provider API see:
[Credential Provider API](../../../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).


The following configuration options can be storeed in Hadoop Credential Provider
stores.

```
fs.s3a.access.key
fs.s3a.secret.key
fs.s3a.session.token
fs.s3a.server-side-encryption.key
fs.s3a.server-side-encryption-algorithm
```

The first three are for authentication; the final two for
[encryption](./encryption.html). Of the latter, only the encryption key can
be considered "sensitive". However, being able to include the algorithm in
the credentials allows for a JCECKS file to contain all the options needed
to encrypt new data written to S3.

### Step 1: Create a credential file

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

### Step 2: Configure the `hadoop.security.credential.provider.path` property

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

The property `hadoop.security.credential.provider.path` is global to all
filesystems and secrets.
There is another property, `fs.s3a.security.credential.provider.path`
which only lists credential providers for S3A filesystems.
The two properties are combined into one, with the list of providers in the
`fs.s3a.` property taking precedence
over that of the `hadoop.security` list (i.e. they are prepended to the common list).

```xml
<property>
  <name>fs.s3a.security.credential.provider.path</name>
  <value />
  <description>
    Optional comma separated list of credential providers, a list
    which is prepended to that set in hadoop.security.credential.provider.path
  </description>
</property>
```

This was added to support binding different credential providers on a per
bucket basis, without adding alternative secrets in the credential list.
However, some applications (e.g Hive) prevent the list of credential providers
from being dynamically updated by users. As per-bucket secrets are now supported,
it is better to include per-bucket keys in JCEKS files and other sources
of credentials.

### Using secrets from credential providers

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
  -D fs.s3a.security.credential.provider.path=jceks://hdfs@nn1.example.com:9001/user/backup/s3.jceks \
  -ls s3a://glacier1/
```

Because the provider path is not itself a sensitive secret, there is no risk
from placing its declaration on the command line.


## <a name="general_configuration"></a>General S3A Client configuration

All S3A client options are configured with options with the prefix `fs.s3a.`.

The client supports <a href="per_bucket_configuration">Per-bucket configuration</a>
to allow different buckets to override the shared settings. This is commonly
used to change the endpoint, encryption and authentication mechanisms of buckets.
S3Guard options, various minor options.

Here are the S3A properties for use in production. The S3Guard options are
documented in the [S3Guard documenents](./s3guard.html); some testing-related
options are covered in [Testing](./testing.md).

```xml
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
    Unset by default. It supports the following values: 'AES256' (for SSE-S3), 'SSE-KMS'
     and 'SSE-C'
  </description>
</property>

<property>
    <name>fs.s3a.server-side-encryption.key</name>
    <description>Specific encryption key to use if fs.s3a.server-side-encryption-algorithm
    has been set to 'SSE-KMS' or 'SSE-C'. In the case of SSE-C, the value of this property
    should be the Base64 encoded key. If you are using SSE-KMS and leave this property empty,
    you'll be using your default's S3 KMS key, otherwise you should set this property to
    the specific KMS key id.</description>
</property>

<property>
  <name>fs.s3a.buffer.dir</name>
  <value>${hadoop.tmp.dir}/s3a</value>
  <description>Comma separated list of directories that will be used to buffer file
    uploads to.</description>
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

<property>
  <name>fs.s3a.list.version</name>
  <value>2</value>
  <description>Select which version of the S3 SDK's List Objects API to use.
  Currently support 2 (default) and 1 (older API).</description>
</property>
```

## <a name="retry_and_recovery"></a>Retry and Recovery

The S3A client makes a best-effort attempt at recovering from network failures;
this section covers the details of what it does.

The S3A divides exceptions returned by the AWS SDK into different categories,
and chooses a different retry policy based on their type and whether or
not the failing operation is idempotent.


### Unrecoverable Problems: Fail Fast

* No object/bucket store: `FileNotFoundException`
* No access permissions: `AccessDeniedException`
* Network errors considered unrecoverable (`UnknownHostException`,
 `NoRouteToHostException`, `AWSRedirectException`).
* Interruptions: `InterruptedIOException`, `InterruptedException`.
* Rejected HTTP requests: `InvalidRequestException`

These are all considered unrecoverable: S3A will make no attempt to recover
from them.

### Possibly Recoverable Problems: Retry

* Connection timeout: `ConnectTimeoutException`. Timeout before
setting up a connection to the S3 endpoint (or proxy).
* HTTP response status code 400, "Bad Request"

The status code 400, Bad Request usually means that the request
is unrecoverable; it's the generic "No" response. Very rarely it
does recover, which is why it is in this category, rather than that
of unrecoverable failures.

These failures will be retried with a fixed sleep interval set in
`fs.s3a.retry.interval`, up to the limit set in `fs.s3a.retry.limit`.


### Only retriable on idempotent operations

Some network failures are considered to be retriable if they occur on
idempotent operations; there's no way to know if they happened
after the request was processed by S3.

* `SocketTimeoutException`: general network failure.
* `EOFException` : the connection was broken while reading data
* "No response from Server" (443, 444) HTTP responses.
* Any other AWS client, service or S3 exception.

These failures will be retried with a fixed sleep interval set in
`fs.s3a.retry.interval`, up to the limit set in `fs.s3a.retry.limit`.

*Important*: DELETE is considered idempotent, hence: `FileSystem.delete()`
and `FileSystem.rename()` will retry their delete requests on any
of these failures.

The issue of whether delete should be idempotent has been a source
of historical controversy in Hadoop.

1. In the absence of any other changes to the object store, a repeated
DELETE request will eventually result in the named object being deleted;
it's a no-op if reprocessed. As indeed, is `Filesystem.delete()`.
1. If another client creates a file under the path, it will be deleted.
1. Any filesystem supporting an atomic `FileSystem.create(path, overwrite=false)`
operation to reject file creation if the path exists MUST NOT consider
delete to be idempotent, because a `create(path, false)` operation will
only succeed if the first `delete()` call has already succeeded.
1. And a second, retried `delete()` call could delete the new data.

Because S3 is eventually consistent *and* doesn't support an
atomic create-no-overwrite operation, the choice is more ambiguous.

Currently S3A considers delete to be
idempotent because it is convenient for many workflows, including the
commit protocols. Just be aware that in the presence of transient failures,
more things may be deleted than expected. (For anyone who considers this to
be the wrong decision: rebuild the `hadoop-aws` module with the constant
`S3AFileSystem.DELETE_CONSIDERED_IDEMPOTENT` set to `false`).






### Throttled requests from S3 and Dynamo DB


When S3A or Dynamo DB returns a response indicating that requests
from the caller are being throttled, an exponential back-off with
an initial interval and a maximum number of requests.

```xml
<property>
  <name>fs.s3a.retry.throttle.limit</name>
  <value>${fs.s3a.attempts.maximum}</value>
  <description>
    Number of times to retry any throttled request.
  </description>
</property>

<property>
  <name>fs.s3a.retry.throttle.interval</name>
  <value>1000ms</value>
  <description>
    Interval between retry attempts on throttled requests.
  </description>
</property>
```

Notes

1. There is also throttling taking place inside the AWS SDK; this is managed
by the value `fs.s3a.attempts.maximum`.
1. Throttling events are tracked in the S3A filesystem metrics and statistics.
1. Amazon KMS may throttle a customer based on the total rate of uses of
KMS *across all user accounts and applications*.

Throttling of S3 requests is all too common; it is caused by too many clients
trying to access the same shard of S3 Storage. This generally
happen if there are too many reads, those being the most common in Hadoop
applications. This problem is exacerbated by Hive's partitioning
strategy used when storing data, such as partitioning by year and then month.
This results in paths with little or no variation at their start, which ends
up in all the data being stored in the same shard(s).

Here are some expensive operations; the more of these taking place
against part of an S3 bucket, the more load it experiences.
* Many clients trying to list directories or calling `getFileStatus` on
paths (LIST and HEAD requests respectively)
* The GET requests issued when reading data.
* Random IO used when reading columnar data (ORC, Parquet) means that many
more GET requests than a simple one-per-file read.
* The number of active writes to that part of the S3 bucket.

A special case is when enough data has been written into part of an S3 bucket
that S3 decides to split the data across more than one shard: this
is believed to be one by some copy operation which can take some time.
While this is under way, S3 clients access data under these paths will
be throttled more than usual.


Mitigation strategies

1. Use separate buckets for intermediate data/different applications/roles.
1. Use significantly different paths for different datasets in the same bucket.
1. Increase the value of `fs.s3a.retry.throttle.interval` to provide
longer delays between attempts.
1. Reduce the parallelism of the queries. The more tasks trying to access
data in parallel, the more load.
1. Reduce `fs.s3a.threads.max` to reduce the amount of parallel operations
performed by clients.
!. Maybe: increase `fs.s3a.readahead.range` to increase the minimum amount
of data asked for in every GET request, as well as how much data is
skipped in the existing stream before aborting it and creating a new stream.
1. If the DynamoDB tables used by S3Guard are being throttled, increase
the capacity through `hadoop s3guard set-capacity` (and pay more, obviously).
1. KMS: "consult AWS about increasing your capacity".




## <a name="per_bucket_configuration"></a>Configuring different S3 buckets with Per-Bucket Configuration

Different S3 buckets can be accessed with different S3A client configurations.
This allows for different endpoints, data read and write strategies, as well
as login details.

1. All `fs.s3a` options other than a small set of unmodifiable values
 (currently `fs.s3a.impl`) can be set on a per bucket basis.
1. The bucket specific option is set by replacing the `fs.s3a.` prefix on an option
with `fs.s3a.bucket.BUCKETNAME.`, where `BUCKETNAME` is the name of the bucket.
1. When connecting to a bucket, all options explicitly set will override
the base `fs.s3a.` values.

As an example, a configuration could have a base configuration to use the IAM
role information available when deployed in Amazon EC2.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>com.amazonaws.auth.InstanceProfileCredentialsProvider</value>
</property>
```

This will become the default authentication mechanism for S3A buckets.

A bucket `s3a://nightly/` used for nightly data can then be given
a session key:

```xml
<property>
  <name>fs.s3a.bucket.nightly.access.key</name>
  <value>AKAACCESSKEY-2</value>
</property>

<property>
  <name>fs.s3a.bucket.nightly.secret.key</name>
  <value>SESSIONSECRETKEY</value>
</property>

<property>
  <name>fs.s3a.bucket.nightly.session.token</name>
  <value>Short-lived-session-token</value>
</property>

<property>
  <name>fs.s3a.bucket.nightly.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider</value>
</property>
```

Finally, the public `s3a://landsat-pds/` bucket can be accessed anonymously:

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider</value>
</property>
```

### Customizing S3A secrets held in credential files


Secrets in JCEKS files or provided by other Hadoop credential providers
can also be configured on a per bucket basis. The S3A client will
look for the per-bucket secrets be


Consider a JCEKS file with six keys:

```
fs.s3a.access.key
fs.s3a.secret.key
fs.s3a.server-side-encryption-algorithm
fs.s3a.bucket.nightly.access.key
fs.s3a.bucket.nightly.secret.key
fs.s3a.bucket.nightly.session.token
fs.s3a.bucket.nightly.server-side-encryption.key
fs.s3a.bucket.nightly.server-side-encryption-algorithm
```

When accessing the bucket `s3a://nightly/`, the per-bucket configuration
options for that bucket will be used, here the access keys and token,
and including the encryption algorithm and key.


###  <a name="per_bucket_endpoints"></a>Using Per-Bucket Configuration to access data round the world

S3 Buckets are hosted in different "regions", the default being "US-East".
The S3A client talks to this region by default, issuing HTTP requests
to the server `s3.amazonaws.com`.

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

If the wrong endpoint is used, the request may fail. This may be reported as a 301/redirect error,
or as a 400 Bad Request: take these as cues to check the endpoint setting of
a bucket.

Here is a list of properties defining all AWS S3 regions, current as of June 2017:

```xml
<!--
 This is the default endpoint, which can be used to interact
 with any v2 region.
 -->
<property>
  <name>central.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>

<property>
  <name>canada.endpoint</name>
  <value>s3.ca-central-1.amazonaws.com</value>
</property>

<property>
  <name>frankfurt.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>

<property>
  <name>ireland.endpoint</name>
  <value>s3-eu-west-1.amazonaws.com</value>
</property>

<property>
  <name>london.endpoint</name>
  <value>s3.eu-west-2.amazonaws.com</value>
</property>

<property>
  <name>mumbai.endpoint</name>
  <value>s3.ap-south-1.amazonaws.com</value>
</property>

<property>
  <name>ohio.endpoint</name>
  <value>s3.us-east-2.amazonaws.com</value>
</property>

<property>
  <name>oregon.endpoint</name>
  <value>s3-us-west-2.amazonaws.com</value>
</property>

<property>
  <name>sao-paolo.endpoint</name>
  <value>s3-sa-east-1.amazonaws.com</value>
</property>

<property>
  <name>seoul.endpoint</name>
  <value>s3.ap-northeast-2.amazonaws.com</value>
</property>

<property>
  <name>singapore.endpoint</name>
  <value>s3-ap-southeast-1.amazonaws.com</value>
</property>

<property>
  <name>sydney.endpoint</name>
  <value>s3-ap-southeast-2.amazonaws.com</value>
</property>

<property>
  <name>tokyo.endpoint</name>
  <value>s3-ap-northeast-1.amazonaws.com</value>
</property>

<property>
  <name>virginia.endpoint</name>
  <value>${central.endpoint}</value>
</property>
```

This list can be used to specify the endpoint of individual buckets, for example
for buckets in the central and EU/Ireland endpoints.

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.endpoint</name>
  <value>${central.endpoint}</value>
  <description>The endpoint for s3a://landsat-pds URLs</description>
</property>

<property>
  <name>fs.s3a.bucket.eu-dataset.endpoint</name>
  <value>${ireland.endpoint}</value>
  <description>The endpoint for s3a://eu-dataset URLs</description>
</property>
```

Why explicitly declare a bucket bound to the central endpoint? It ensures
that if the default endpoint is changed to a new region, data store in
US-east is still reachable.

## <a name="upload"></a>How S3A writes data to S3

The original S3A client implemented file writes by
buffering all data to disk as it was written to the `OutputStream`.
Only when the stream's `close()` method was called would the upload start.

This made output slow, especially on large uploads, and could even
fill up the disk space of small (virtual) disks.

Hadoop 2.7 added the `S3AFastOutputStream` alternative, which Hadoop 2.8 expanded.
It is now considered stable and has replaced the original `S3AOutputStream`,
which is no longer shipped in hadoop.

The "fast" output stream

1.  Uploads large files as blocks with the size set by
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
1.  Has the time to `close()` set by the amount of remaining data to upload, rather
    than the total size of the file.

Because it starts uploading while data is still being written, it offers
significant benefits when very large amounts of data are generated.
The in memory buffering mechanisms may also offer speedup when running adjacent to
S3 endpoints, as disks are not used for intermediate data storage.


```xml
<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>disk</value>
  <description>
    The buffering mechanism to use.
    Values: disk, array, bytebuffer.

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

* Files being written are still invisible until the write
completes in the `close()` call, which will block until the upload is completed.


### <a name="upload_disk"></a>Buffering upload data on disk `fs.s3a.fast.upload.buffer=disk`

When `fs.s3a.fast.upload.buffer` is set to `disk`, all data is buffered
to local hard disks prior to upload. This minimizes the amount of memory
consumed, and so eliminates heap size as the limiting factor in queued uploads
—exactly as the original "direct to disk" buffering.


```xml
<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>disk</value>
</property>

<property>
  <name>fs.s3a.buffer.dir</name>
  <value>${hadoop.tmp.dir}/s3a</value>
  <description>Comma separated list of directories that will be used to buffer file
    uploads to.</description>
</property>
```

This is the default buffer mechanism. The amount of data which can
be buffered is limited by the amount of available disk space.

### <a name="upload_bytebuffer"></a>Buffering upload data in ByteBuffers: `fs.s3a.fast.upload.buffer=bytebuffer`

When `fs.s3a.fast.upload.buffer` is set to `bytebuffer`, all data is buffered
in "Direct" ByteBuffers prior to upload. This *may* be faster than buffering to disk,
and, if disk space is small (for example, tiny EC2 VMs), there may not
be much disk space to buffer with.

The ByteBuffers are created in the memory of the JVM, but not in the Java Heap itself.
The amount of data which can be buffered is
limited by the Java runtime, the operating system, and, for YARN applications,
the amount of memory requested for each container.

The slower the upload bandwidth to S3, the greater the risk of running out
of memory —and so the more care is needed in
[tuning the upload settings](#upload_thread_tuning).


```xml
<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>bytebuffer</value>
</property>
```

### <a name="upload_array"></a>Buffering upload data in byte arrays: `fs.s3a.fast.upload.buffer=array`

When `fs.s3a.fast.upload.buffer` is set to `array`, all data is buffered
in byte arrays in the JVM's heap prior to upload.
This *may* be faster than buffering to disk.

The amount of data which can be buffered is limited by the available
size of the JVM heap heap. The slower the write bandwidth to S3, the greater
the risk of heap overflows. This risk can be mitigated by
[tuning the upload settings](#upload_thread_tuning).

```xml
<property>
  <name>fs.s3a.fast.upload.buffer</name>
  <value>array</value>
</property>
```

### <a name="upload_thread_tuning"></a>Upload Thread Tuning

Both the [Array](#upload_array) and [Byte buffer](#upload_bytebuffer)
buffer mechanisms can consume very large amounts of memory, on-heap or
off-heap respectively. The [disk buffer](#upload_disk) mechanism
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
then experiment to see if higher values deliver more throughput —especially
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

### <a name="multipart_purge"></a>Cleaning up after partial Upload Failures

There are two mechanisms for cleaning up after leftover multipart
uploads:
- Hadoop s3guard CLI commands for listing and deleting uploads by their
age. Documented in the [S3Guard](./s3guard.html) section.
- The configuration parameter `fs.s3a.multipart.purge`, covered below.

If a large stream write operation is interrupted, there may be
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

If an S3A client is instantiated with `fs.s3a.multipart.purge=true`,
it will delete all out of date uploads *in the entire bucket*. That is: it will affect all
multipart uploads to that bucket, from all applications.

Leaving `fs.s3a.multipart.purge` to its default, `false`,
means that the client will not make any attempt to reset or change the partition
rate.

The best practise for using this option is to disable multipart purges in
normal use of S3A, enabling only in manual/scheduled housekeeping operations.

### S3A "fadvise" input policy support

The S3A Filesystem client supports the notion of input policies, similar
to that of the Posix `fadvise()` API call. This tunes the behavior of the S3A
client to optimise HTTP GET requests for the different use cases.

See [Improving data input performance through fadvise](./performance.html#fadvise)
for the details.

##<a name="metrics"></a>Metrics

S3A metrics can be monitored through Hadoop's metrics2 framework. S3A creates
its own metrics system called s3a-file-system, and each instance of the client
will create its own metrics source, named with a JVM-unique numerical ID.

As a simple example, the following can be added to `hadoop-metrics2.properties`
to write all S3A metrics to a log file every 10 seconds:

    s3a-file-system.sink.my-metrics-config.class=org.apache.hadoop.metrics2.sink.FileSink
    s3a-file-system.sink.my-metrics-config.filename=/var/log/hadoop-yarn/s3a-metrics.out
    *.period=10

Lines in that file will be structured like the following:

    1511208770680 s3aFileSystem.s3aFileSystem: Context=s3aFileSystem, s3aFileSystemId=892b02bb-7b30-4ffe-80ca-3a9935e1d96e, bucket=bucket,
    Hostname=hostname-1.hadoop.apache.com, files_created=1, files_copied=2, files_copied_bytes=10000, files_deleted=5, fake_directories_deleted=3,
    directories_created=3, directories_deleted=0, ignored_errors=0, op_copy_from_local_file=0, op_exists=0, op_get_file_status=15, op_glob_status=0,
    op_is_directory=0, op_is_file=0, op_list_files=0, op_list_located_status=0, op_list_status=3, op_mkdirs=1, op_rename=2, object_copy_requests=0,
    object_delete_requests=6, object_list_requests=23, object_continue_list_requests=0, object_metadata_requests=46, object_multipart_aborted=0,
    object_put_bytes=0, object_put_requests=4, object_put_requests_completed=4, stream_write_failures=0, stream_write_block_uploads=0,
    stream_write_block_uploads_committed=0, stream_write_block_uploads_aborted=0, stream_write_total_time=0, stream_write_total_data=0,
    s3guard_metadatastore_put_path_request=10, s3guard_metadatastore_initialization=0, object_put_requests_active=0, object_put_bytes_pending=0,
    stream_write_block_uploads_active=0, stream_write_block_uploads_pending=0, stream_write_block_uploads_data_pending=0,
    S3guard_metadatastore_put_path_latencyNumOps=0, S3guard_metadatastore_put_path_latency50thPercentileLatency=0,
    S3guard_metadatastore_put_path_latency75thPercentileLatency=0, S3guard_metadatastore_put_path_latency90thPercentileLatency=0,
    S3guard_metadatastore_put_path_latency95thPercentileLatency=0, S3guard_metadatastore_put_path_latency99thPercentileLatency=0

Depending on other configuration, metrics from other systems, contexts, etc. may
also get recorded, for example the following:

    1511208770680 metricssystem.MetricsSystem: Context=metricssystem, Hostname=s3a-metrics-4.gce.cloudera.com, NumActiveSources=1, NumAllSources=1,
    NumActiveSinks=1, NumAllSinks=0, Sink_fileNumOps=2, Sink_fileAvgTime=1.0, Sink_fileDropped=0, Sink_fileQsize=0, SnapshotNumOps=5,
    SnapshotAvgTime=0.0, PublishNumOps=2, PublishAvgTime=0.0, DroppedPubAll=0

Note that low-level metrics from the AWS SDK itself are not currently included
in these metrics.

##<a name="further_reading"></a> Other Topics

### <a name="distcp"></a> Copying Data with distcp

Hadoop's `distcp` tool is often used to copy data between a Hadoop
cluster and Amazon S3.
See [Copying Data Between a Cluster and Amazon S3](https://hortonworks.github.io/hdp-aws/s3-copy-data/index.html)
for details on S3 copying specifically.

The `distcp update` command tries to do incremental updates of data.
It is straightforward to verify when files do not match when they are of
different length, but not when they are the same size.

Distcp addresses this by comparing file checksums on the source and destination
filesystems, which it tries to do *even if the filesystems have incompatible
checksum algorithms*.

The S3A connector can provide the HTTP etag header to the caller as the
checksum of the uploaded file. Doing so will break distcp operations
between hdfs and s3a.

For this reason, the etag-as-checksum feature is disabled by default.

```xml
<property>
  <name>fs.s3a.etag.checksum.enabled</name>
  <value>false</value>
  <description>
    Should calls to getFileChecksum() return the etag value of the remote
    object.
    WARNING: if enabled, distcp operations between HDFS and S3 will fail unless
    -skipcrccheck is set.
  </description>
</property>
```

If enabled, `distcp` between two S3 buckets can use the checksum to compare
objects. Their checksums should be identical if they were either each uploaded
as a single file PUT, or, if in a multipart PUT, in blocks of the same size,
as configured by the value `fs.s3a.multipart.size`.

To disable checksum verification in `distcp`, use the `-skipcrccheck` option:

```bash
hadoop distcp -update -skipcrccheck /user/alice/datasets s3a://alice-backup/datasets
```

