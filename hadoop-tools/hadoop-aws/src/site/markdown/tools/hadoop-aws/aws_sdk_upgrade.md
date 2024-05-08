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

## Packaging: `aws-java-sdk-bundle-1.12.x.jar` becomes `bundle-2.x.y.jar`

As the module name is lost, in hadoop releases a large JAR file with
the name "bundle" is now part of the distribution.
This is the AWS V2 SDK shaded artifact.

The new and old SDKs can co-exist; the only place that the hadoop code
may still use the original SDK is when a non-standard V1 AWS credential
provider is declared.

Any deployment of the S3A connector must include this JAR or
the subset of non-shaded aws- JARs needed for communication
with S3 and any other services used.
As before: the exact set of dependencies used by the S3A connector
is neither defined nor comes with any commitments of stability
or compatibility of dependent libraries.



## Credential Provider changes and migration

- Interface change: [com.amazonaws.auth.AWSCredentialsProvider](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/AWSCredentialsProvider.java)
has been replaced by [software.amazon.awssdk.auth.credentials.AwsCredentialsProvider](https://github.com/aws/aws-sdk-java-v2/blob/master/core/auth/src/main/java/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.java).
- Credential provider class changes: the package and class names of credential providers have
changed.

The change in interface will mean that custom credential providers will need to be updated to now
implement `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider` instead of
`com.amazonaws.auth.AWSCredentialsProvider`.

[HADOOP-18980](https://issues.apache.org/jira/browse/HADOOP-18980) introduces extended version of
the credential provider remapping. `fs.s3a.aws.credentials.provider.mapping` can be used to
list comma-separated key-value pairs of mapped credential providers that are separated by
equal operator (=).
The key can be used by `fs.s3a.aws.credentials.provider` or
`fs.s3a.assumed.role.credentials.provider` configs, and the key will be translated into
the specified value of credential provider class based on the key-value pair
provided by the config `fs.s3a.aws.credentials.provider.mapping`.

For example, if `fs.s3a.aws.credentials.provider.mapping` is set with value:

```xml
<property>
  <name>fs.s3a.aws.credentials.provider.mapping</name>
  <vale>
    com.amazonaws.auth.AnonymousAWSCredentials=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider,
    com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,
    com.amazonaws.auth.InstanceProfileCredentialsProvider=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
  </vale>
</property>
```

and if `fs.s3a.aws.credentials.provider` is set with:

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <vale>com.amazonaws.auth.AnonymousAWSCredentials</vale>
</property>
```

`com.amazonaws.auth.AnonymousAWSCredentials` will be internally remapped to
`org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` by S3A while preparing
the AWS credential provider list.

Similarly, if `fs.s3a.assumed.role.credentials.provider` is set with:

```xml
<property>
  <name>fs.s3a.assumed.role.credentials.provider</name>
  <vale>com.amazonaws.auth.InstanceProfileCredentialsProvider</vale>
</property>
```

`com.amazonaws.auth.InstanceProfileCredentialsProvider` will be internally
remapped to `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` by
S3A while preparing the assumed role AWS credential provider list.


### Original V1 `AWSCredentialsProvider` interface

Note how the interface begins with the capitalized "AWS" acronym.
The V2 interface starts with "Aws". This is a very subtle change
for developers to spot.
Compilers _will_ detect and report the type mismatch.


```java
package com.amazonaws.auth;

public interface AWSCredentialsProvider {

    public AWSCredentials getCredentials();

    public void refresh();

}

```
The interface binding also supported a factory method, `AWSCredentialsProvider instance()` which,
if available, would be invoked in preference to using any constructor.

If the interface implemented `Closeable` or `AutoCloseable`, these would
be invoked when the provider chain was being shut down.

### New V2 `AwsCredentialsProvider` interface

```java
package software.amazon.awssdk.auth.credentials;

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
5. A static method `create()` which returns an `AwsCredentialsProvider` or subclass; this will be used
   in preference to a constructor

### S3A `AWSCredentialProviderList` is now a V2 credential provider

The class `org.apache.hadoop.fs.s3a.AWSCredentialProviderList` has moved from
being a V1 to a V2 credential provider; even if an instance can be created with
existing code, the V1 methods will not resolve:

```
java.lang.NoSuchMethodError: org.apache.hadoop.fs.s3a.AWSCredentialProviderList.getCredentials()Lcom/amazonaws/auth/AWSCredentials;
  at org.apache.hadoop.fs.store.diag.S3ADiagnosticsInfo.validateFilesystem(S3ADiagnosticsInfo.java:903)
```

### Migration of Credential Providers listed in `fs.s3a.aws.credentials.provider`


Before: `fs.s3a.aws.credentials.provider` took a list of v1 credential providers,
This took a list containing
1. V1 credential providers implemented in the `hadoop-aws` module.
2. V1 credential providers implemented in the `aws-sdk-bundle` library.
3. Custom V1 credential providers placed onto the classpath.
4. Custom subclasses of hadoop-aws credential providers.

And here is how they change
1. All `hadoop-aws` credential providers migrated to V2.
2. Well-known `aws-sdk-bundle` credential providers _automatically remapped_ to their V2 equivalents.
3. Custom v1 providers supported if the original `aws-sdk-bundle` JAR is on the classpath.
4. Custom subclasses of hadoop-aws credential providers need manual migration.

Because of (1) and (2), As result, standard `fs.s3a.aws.credentials.provider` configurations
should seamlessly upgrade. This also means that the same provider list, if restricted to
those classes, will work across versions.


### `hadoop-aws` credential providers migration to V2

All the fs.s3a credential providers have the same name and functionality as before.

| Hadoop module credential provider                              | Authentication Mechanism                         |
|----------------------------------------------------------------|--------------------------------------------------|
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`     | Session Credentials in configuration             |
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`        | Simple name/secret credentials in configuration  |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`     | Anonymous Login                                  |
| `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider`  | [Assumed Role credentials](./assumed_roles.html) |
| `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` | EC2/k8s instance credentials                     |

### Automatic `aws-sdk-bundle` credential provider remapping

The commonly-used set of V1 credential providers are automatically remapped to V2 equivalents.



| V1 Credential Provider                                      | Remapped V2 substitute                                                           |
|-------------------------------------------------------------|----------------------------------------------------------------------------------|
| `com.amazonaws.auth.AnonymousAWSCredentials`                | `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`                       |
| `com.amazonaws.auth.EnvironmentVariableCredentialsProvider` | `software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider` |
| `com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper` | `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`                   |
| `com.amazonaws.auth.InstanceProfileCredentialsProvider`     | `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`                   |
| `com.amazonaws.auth.profile.ProfileCredentialsProvider`     | `software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider`             |

There are still a number of troublespots here:

#### Less widely used`com.amazonaws.auth.` AWS providers

There should be equivalents in the new SDK, but as well as being renamed
they are likely to have moved different factory/builder mechanisms.
Identify the changed classes and use their
names in the `fs.s3a.aws.credentials.provider` option.

If a V2 equivalent is not found; provided the V1 SDK is added to the classpath,
it should still be possible to use the existing classes.


#### Private/third-party credential providers

Provided the V1 SDK is added to the classpath,
it should still be possible to use the existing classes.

Adding a V2 equivalent is the recommended long-term solution.

#### Custom subclasses of the Hadoop credential providers

Because all the standard hadoop credential providers have been upgraded,
any subclasses of these are not going to link or work.

These will need to be manually migrated to being V2 Credential providers.


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
See AWS SDK V2 issue [Simplify Modeled Message Marshalling #82](https://github.com/aws/aws-sdk-java-v2/issues/82),
note that it was filed in 2017, then implement your own workaround pending that issue
being resolved.

### Compilation/Linkage Errors

Any code making use of V1 sdk classes will fail if they
* Expect the V1 sdk classes to be on the classpath when `hadoop-aws` is declared as a dependency
* Use V1-SDK-compatible methods previously exported by the `S3AFileSystem` class and associated classes.
* Try to pass s3a classes to V1 SDK classes (e.g. credential providers).

The sole solution to these problems is "move to the V2 SDK".

Some `S3AUtils` methods are deleted
```
cannot find symbol
[ERROR]   symbol:   method createAwsConf(org.apache.hadoop.conf.Configuration,java.lang.String)
[ERROR]   location: class org.apache.hadoop.fs.s3a.S3AUtils
```

The signature and superclass of `AWSCredentialProviderList` has changed, which can surface in different
ways

Signature mismatch
```
 cannot find symbol
[ERROR]   symbol:   method getCredentials()
[ERROR]   location: variable credentials of type org.apache.hadoop.fs.s3a.AWSCredentialProviderList
```

It is no longer a V1 credential provider, cannot be used to pass credentials to a V1 SDK class
```
incompatible types: org.apache.hadoop.fs.s3a.AWSCredentialProviderList cannot be converted to com.amazonaws.auth.AWSCredentialsProvider
```

### `AmazonS3` replaced by `S3Client`; factory and accessor changed.

The V1 s3 client class `com.amazonaws.services.s3.AmazonS3` has been superseded by
`software.amazon.awssdk.services.s3.S3Client`

The `S3ClientFactory` interface has been replaced by one that creates a V2 `S3Client`.
* Custom implementations will need to be updated.
* The `InconsistentS3ClientFactory` class has been deleted.

### `S3AFileSystem` method changes: `S3AInternals`.

The low-level s3 operations/client accessors have been moved into a new interface,
`org.apache.hadoop.fs.s3a.S3AInternals`, which must be accessed via the
`S3AFileSystem.getS3AInternals()` method.
They have also been updated to return V2 SDK classes.

```java
@InterfaceStability.Unstable
@InterfaceAudience.LimitedPrivate("testing/diagnostics")
public interface S3AInternals {
  S3Client getAmazonS3V2Client(String reason);

  @Retries.RetryTranslated
  @AuditEntryPoint
  String getBucketLocation() throws IOException;

  @AuditEntryPoint
  @Retries.RetryTranslated
  String getBucketLocation(String bucketName) throws IOException;

  @AuditEntryPoint
  @Retries.RetryTranslated
  HeadObjectResponse getObjectMetadata(Path path) throws IOException;

  AWSCredentialProviderList shareCredentials(final String purpose);

  @AuditEntryPoint
  @Retries.RetryTranslated
  HeadBucketResponse getBucketMetadata() throws IOException;

  boolean isMultipartCopyEnabled();

  @AuditEntryPoint
  @Retries.RetryTranslated
  long abortMultipartUploads(Path path) throws IOException;
}
```


#### `S3AFileSystem.getAmazonS3ClientForTesting(String)` moved and return type changed

The `S3AFileSystem.getAmazonS3ClientForTesting()` method has been been deleted.

Compilation
```
cannot find symbol
[ERROR]   symbol:   method getAmazonS3ClientForTesting(java.lang.String)
[ERROR]   location: variable fs of type org.apache.hadoop.fs.s3a.S3AFileSystem
```

It has been replaced by an `S3AInternals` equivalent which returns the V2 `S3Client`
of the filesystem instance.

```java
((S3AFilesystem)fs).getAmazonS3ClientForTesting("testing")
```

```java
((S3AFilesystem)fs).getS3AInternals().getAmazonS3Client("testing")
```

##### `S3AFileSystem.getObjectMetadata(Path path)`  moved to `S3AInternals`; return type changed

The `getObjectMetadata(Path)` call has been moved to the `S3AInternals` interface
and an instance of the `software.amazon.awssdk.services.s3.model.HeadObjectResponse` class
returned.
The original `S3AFileSystem` method has been deleted

Before:
```java
((S3AFilesystem)fs).getObjectMetadata(path)
```

After:
```java
((S3AFilesystem)fs).getS3AInternals().getObjectMetadata(path)
```

##### `AWSCredentialProviderList shareCredentials(String)` moved to `S3AInternals`

The operation to share a reference-counted access to the AWS credentials used
by the S3A FS has been moved to `S3AInternals`.

This is very much an implementation method, used to allow extension modules to share
an authentication chain into other AWS SDK client services (dynamoDB, etc.).

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

The option `fs.s3a.audit.request.handlers` to declare a list of v1 SDK
`com.amazonaws.handlers.RequestHandler2` implementations to include
in the AWS request chain is no longer supported: a warning is printed
and the value ignored.

The V2 SDK equivalent, classes implementing `software.amazon.awssdk.core.interceptor.ExecutionInterceptor`
can be declared in the configuration option `fs.s3a.audit.execution.interceptors`.
