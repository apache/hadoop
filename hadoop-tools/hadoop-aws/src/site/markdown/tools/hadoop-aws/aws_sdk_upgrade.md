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

# Upgrading S3A to AWS SDK V2

This document explains the upcoming work for upgrading S3A to AWS SDK V2.
This work is tracked in [HADOOP-18073](https://issues.apache.org/jira/browse/HADOOP-18073).

## Why the upgrade?

- Moving to SDK V2 will provide performance benefits.
For example, the [transfer manager for SDK V2](https://aws.amazon.com/blogs/developer/introducing-amazon-s3-transfer-manager-in-the-aws-sdk-for-java-2-x/)
is built using java bindings of the AWS Common Runtime S3
client (https://github.com/awslabs/aws-crt-java) (CRT).
CRT is a set of packages written in C, designed for maximising performance when interacting with AWS
services such as S3.
- The V1 SDK is essentially in maintenance mode.
- New features such as [additional checksum algorithms](https://aws.amazon.com/blogs/aws/new-additional-checksum-algorithms-for-amazon-s3/)
which S3A will benefit from are not available in SDK V1.

## What's changing?

The [SDK V2](https://github.com/aws/aws-sdk-java-v2) for S3 is very different from
[SDK V1](https://github.com/aws/aws-sdk-java), and brings breaking changes for S3A.
A complete list of the changes can be found in the
[Changelog](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#41-s3-changes).

# S3A integration changes.

## Deployment Changes


### Packaging: `aws-java-sdk-bundle-1.12.x.jar` becomes `bundle-2.x.y.jar`

As the module name is lost, in hadoop releases a large JAR file with
the name "bundle" is now part of the distribution.
This is the AWS v2 SDK shaded artifact.

The new and old SDKs can co-exist; the only place that the hadoop code
may still use the original SDK is when a non-standard V1 AWS credential
provider is declared.

Any deployment of the S3A connector must include this JAR or
the subset of non-shaded aws- JARs needed for communication
with S3 and any other services used.
As before: the exact set of dependencies used by the S3A connector
is neither defined nor comes with any commitments of stability
or compatibility of dependent libraries.

### Configuration Option Changes

### Credential Providers declared in `fs.s3a.aws.credentials.provider`

V1 Credential providers are *only* supported when the V1 SDK is on the classpath.

The standard set of v1 credential providers used in hadoop deployments are
automatically remapped to v2 equivalents,
while the stable hadoop providers have been upgraded in place; their names
are unchanged.
As result, standard cluster configurations should seamlessly upgrade.

| v1 Credential Provider                                      | Remapped V2 substitute                                                           |
|-------------------------------------------------------------|----------------------------------------------------------------------------------|
| `com.amazonaws.auth.AnonymousAWSCredentials`                | `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`                       |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | `software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider` |
| `com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper` | `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`                   |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider`     | `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`                   |


There are still a number of troublespots here:

#### Other `com.amazonaws.auth.` AWS providers

There should be equivalents in the new SDK, but as well as being renamed
they are likely to have moved different factory/builder mechanisms.
Identify the changed classes and use their
names in the `fs.s3a.aws.credentials.provider` option.

If a v2 equivalent is not found; provided the v1 SDK is added to the classpath,
it should still be possible to use the existing classes.


#### Private/third-party credential providers

Provided the v1 SDK is added to the classpath,
it should still be possible to use the existing classes.

Adding a v2 equivalent is the recommended long-term solution.

#### Private subclasses of the Hadoop credential providers

Because all the standard hadoop credential providers have been upgraded,
any subclasses of these are not going to link or work.

These will need to be upgraded in source, as covered below.


## Source code/binary integration changes

The major changes and how this affects S3A are listed below.

### SDK API Package Change

* Package names have changed, all classes in SDK V2 are under `software.amazon.awssdk`, SDK V1 classes
were under `com.amazonaws`.
* There is no interoperability between the old and new classes.
* All classnames are different, often in very subtle ways. It is possible to use both in the same
  class, as is done in the package `org.apache.hadoop.fs.s3a.adapter`.
* All the core message classes are now automatically generated from a JSON protocol description.
* All getter methods have been renamed.
* All classes are constructed via builder methods
* Message classes are no longer Java `Serializable`.

Most of these changes simply create what will feel to be gratuitous migration effort;
the removable of the `Serializable` nature from all message response classes can
potentially break applications -such as anything passing them between Spark workers.
See AWS SDK v2 issue [Simplify Modeled Message Marshalling #82](https://github.com/aws/aws-sdk-java-v2/issues/82),
note that it was filed in 2017, then implement your own workaround pending that issue
being resolved.

### Credential Providers

- Interface change: [com.amazonaws.auth.AWSCredentialsProvider](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/AWSCredentialsProvider.java)
has been replaced by [software.amazon.awssdk.auth.credentials.AwsCredentialsProvider](https://github.com/aws/aws-sdk-java-v2/blob/master/core/auth/src/main/java/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.java).
- Credential provider class changes: the package and class names of credential providers have
changed.

The change in interface will mean that custom credential providers will need to be updated to now
implement `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider` instead of
`com.amazonaws.auth.AWSCredentialsProvider`.

#### Original v1 `AWSCredentialsProvider` interface

Note how the interface begins with the capitalized "AWS" acronym.
The v2 interface starts with "Aws". This is a very subtle change
for developers to spot.
Compilers _will_ detect and report the type mismatch.

```java
public interface AWSCredentialsProvider {

    public AWSCredentials getCredentials();

    public void refresh();

}

```
The interface binding also supported a factory method, `AWSCredentialsProvider instance()` which,
if available, would be invoked in preference to using any constructor.

If the interface implemented `Closeable` or `AutoCloseable`, these would
be invoked when the provider chain was being shut down.

#### v2 `AwsCredentialsProvider` interface

Note how the interface begins with the capitalized "AWS" acronym.
The v2 interface starts with "Aws". This is a very subtle change.
Compilers will detect and report the type mismatch, but it is not
immediately obvious to developers migrating existing code.

```java
public interface AwsCredentialsProvider {

  AwsCredentials resolveCredentials();

}
```

1. There is no `refresh()` method any more.
2. `getCredentials()` has become `resolveCredentials()`.
3. There is now the expectation in the SDK that credential resolution/lookup etc will be
   performed in `resolveCredentials()`.
4. If the interface implements `Closeable` or `AutoCloseable`, these will
   be invoked when the provider chain is being shut down.


### Delegation Tokens

1. Custom credential providers used in delegation token binding classes will need to be updated
2. The return type from delegation token binding has changed to support more class
   instances being returned in the future.

`AWSCredentialProviderList` has been upgraded to the V2 API.
* It still retains a `refresh()` method but this is now a deprecated no-op.
* It is still `Closeable`; its `close()` method iterates through all entries in
the list; if they are `Closeable` or `AutoCloseable` then their `close()` method is invoked.
* Accordingly, providers may still perform background refreshes in separate threads;
  the S3A client will close its provider list when the filesystem itself is closed.

### AmazonS3 replaced by S3Client

The s3 client is an instance of `S3Client` in V2 rather than `AmazonS3`.

For this reason, the `S3ClientFactory` will be deprecated and replaced by one that creates a V2
`S3Client`.

The `getAmazonS3ClientForTesting()` method has been updated to return the `S3Client`.

### Signers

Interface change: [com.amazonaws.auth.Signer](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/Signer.java)
has been replaced by [software.amazon.awssdk.core.signer.Signer](https://github.com/aws/aws-sdk-java-v2/blob/master/core/sdk-core/src/main/java/software/amazon/awssdk/core/signer/Signer.java).

The change in signers will mean the custom signers will need to be updated to implement the new
interface.

There is no support to assist in this migration.

### S3A Auditing Extensions.

The callbacks from the SDK have all changed, as has
the interface `org.apache.hadoop.fs.s3a.audit.AWSAuditEventCallbacks`

Examine the interface and associated implementations to
see how to migrate.
