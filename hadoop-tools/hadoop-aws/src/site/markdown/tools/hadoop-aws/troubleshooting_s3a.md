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

# Troubleshooting

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

## <a name="introduction"></a> Introduction

Common problems working with S3 are:

1. [Classpath setup](#classpath)
2. [Authentication](#authentication)
3. [Access Denial](#access_denied)
4. [Connectivity Problems](#connectivity)
5. [File System Semantics](#semantics)
6. [Encryption](#encryption)
7. [Other Errors](#other)
8. [SDK Upgrade Warnings](#upgrade_warnings)

This document also includes some [best pactises](#best) to aid troubleshooting.


Troubleshooting IAM Assumed Roles is covered in its
[specific documentation](assumed_roles.html#troubleshooting).

## <a name="classpath"></a> Classpath Setup

Classpath is usually the first problem. For the S3A filesystem client,
you need the Hadoop-specific filesystem clients, the very same AWS SDK library
which Hadoop was built against, and any dependent libraries compatible with
Hadoop and the specific JVM.

The classpath must be set up for the process talking to S3: if this is code
running in the Hadoop cluster, the JARs must be on that classpath. That
includes `distcp` and the `hadoop fs` command.

<b>Critical:</b> *Do not attempt to "drop in" a newer version of the AWS
SDK than that which the Hadoop version was built with*
Whatever problem you have, changing the AWS SDK version will not fix things,
only change the stack traces you see.

Similarly, don't try and mix a `hadoop-aws` JAR from one Hadoop release
with that of any other. The JAR must be in sync with `hadoop-common` and
some other Hadoop JARs.

<i>Randomly changing hadoop- and aws- JARs in the hope of making a problem
"go away" or to gain access to a feature you want,
will not lead to the outcome you desire.</i>

Tip: you can use [mvnrepository](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)
to determine the dependency version requirements of a specific `hadoop-aws`
JAR published by the ASF.


### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`

These are Hadoop filesystem client classes, found in the `hadoop-aws` JAR.
An exception reporting this class as missing means that this JAR is not on
the classpath.


### `java.lang.NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception`

This is one of the first stack traces which can surface when trying to instantiate
an S3A filesystem instance without having the AWS V2 SDK `bundle.jar` on the classpath

```
java.lang.NoClassDefFoundError: software/amazon/awssdk/services/s3/model/S3Exception
    at java.lang.ClassLoader.defineClass1(Native Method)
    at java.lang.ClassLoader.defineClass(ClassLoader.java:756)
    at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
    at java.net.URLClassLoader.defineClass(URLClassLoader.java:473)
    at java.net.URLClassLoader.access$100(URLClassLoader.java:74)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:369)
    at java.net.URLClassLoader$1.run(URLClassLoader.java:363)
    at java.security.AccessController.doPrivileged(Native Method)
    at java.net.URLClassLoader.findClass(URLClassLoader.java:362)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
    at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
    at java.lang.Class.forName0(Native Method)
    at java.lang.Class.forName(Class.java:348)
    at org.apache.hadoop.conf.Configuration.getClassByNameOrNull(Configuration.java:2639)
    at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:2604)
    at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2700)
    at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:3414)
    at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3449)
    at org.apache.hadoop.fs.FileSystem.access$300(FileSystem.java:162)
```

Fix: add it to classpath.

Maven/Ivy/SBT/Gradle builds which import `hadoop-aws` or
`hadoop-cloud-storage` artifacts should get the artifact automatically.

### `ClassNotFoundException: com.amazonaws.auth.AWSCredentials`

(or other `com.amazonaws` class.)

With the move to the [V2 AWS SDK](../aws_sdk_upgrade.html),
the v1 SDK classes are no longer on the classpath.

If this happens when trying to use a custom credential provider defined
in `fs.s3a.aws.credentials.provider`, then add the `aws-sdk-bundle.jar`
JAR to the classpath.

If this happens in your own/third-party code, then again, add the JAR,
and/or consider moving to the v2 sdk yourself.

### `java.lang.NoSuchMethodError` referencing a `software.amazon` class

This can be triggered by incompatibilities between the AWS SDK on the classpath
and the version which Hadoop was compiled with.

The AWS SDK JARs change their signature enough between releases that the only
way to safely update the AWS SDK version is to recompile Hadoop against the later
version.

The fix is to use the same version of the AWS SDK with which Hadoop
was built.

This can also be caused by having more than one version of an AWS SDK
JAR on the classpath. If the full `bundle.jar` JAR is on the
classpath, do not add any of the `aws-sdk-` JARs *except* for
`aws-crt.jar` (which is required) and
`eventstream.jar` which is required when using S3 Select.


### `java.lang.NoSuchMethodError` referencing an `org.apache.hadoop` class

This happens if the `hadoop-aws` and `hadoop-common` JARs are out of sync.
You can't mix them around: they have to have exactly matching version numbers.

### `java.lang.NoClassDefFoundError: org/wildfly/openssl/OpenSSLProvider`

This happens when <a href="performance.html#openssl">OpenSSL performance
acceleration</a> has been configured by setting `fs.s3a.ssl.channel.mode`
to `openssl` but the wildfly JAR is not on the classpath.

Fixes:
* Add it to the classpath
* Use a different channel mode, including `default`, which will
revert to the JVM SSL implementation when the wildfly
or native openssl libraries cannot be loaded.


## <a name="authentication"></a> Authentication Failure

If Hadoop cannot authenticate with the S3 service endpoint,
the client retries a number of times before eventually failing.
When it finally gives up, it will report a message about signature mismatch:

```
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

1. If using a private S3 server, make sure endpoint in `fs.s3a.endpoint` has
been set to this server -and that the client is not accidentally trying to
authenticate with the public Amazon S3 service.

1. Make sure the property names are correct. For S3A, they are
`fs.s3a.access.key` and `fs.s3a.secret.key` —you cannot just copy the S3N
properties and replace `s3n` with `s3a`.

1. Make sure the properties are visible to the process attempting to
talk to the object store. Placing them in `core-site.xml` is the standard
mechanism.

1. If using session authentication, the session may have expired.
Generate a new session token and secret.

1. If using environment variable-based authentication, make sure that the
relevant variables are set in the environment in which the process is running.

The standard first step is: try to use the AWS command line tools with the same
credentials, through a command such as:

    hadoop fs -ls s3a://my-bucket/

Note the trailing "/" here; without that the shell thinks you are trying to list
your home directory under the bucket, which will only exist if explicitly created.

Finally, if you set the environment variables, you can take advantage of S3A's
support of environment-variable authentication by attempting the same ls operation.
That is: unset the `fs.s3a` secrets and rely on the environment variables.


### Authentication failure "The Filesystem URI contains login details."

```
The Filesystem URI contains login details. This authentication mechanism is no longer supported.
```

The S3A connector no longer supports the dangerously insecure mechanism of
passing login details within the S3A URLs.

Fix: use a more secure mechanism to pass down the secrets.

### Authentication failure due to clock skew

The timestamp is used in signing to S3, so as to
defend against replay attacks. If the system clock is too far behind *or ahead*
of Amazon's, requests will be rejected.

This can surface as the situation where
read requests are allowed, but operations which write to the bucket are denied.

Check the system clock.

### <a name="bad_request"></a> "Bad Request" exception when working with data stores in an AWS region other than us-eaast



```
$ bin/hadoop fs -ls s3a://frankfurt/
WARN s3a.S3AFileSystem: Client: Amazon S3 error 400: 400 Bad Request; Bad Request (retryable)

ls: doesBucketExist on frankfurt-new: S3Exception:
  Bad Request (Service: Amazon S3; Status Code: 400; Error Code: 400 Bad Request;
```

The S3A client needs to be given the endpoint to use via the `fs.s3a.endpoint`
property.

As an example, the endpoint for S3 Frankfurt is `s3.eu-central-1.amazonaws.com`:

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>
```
### <a name="AuthorizationHeaderMalformed"></a> "Authorization Header is Malformed"(400) exception when PrivateLink URL is used in "fs.s3a.endpoint"

When [PrivateLink](https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html) URL
is used instead of standard s3a endpoint, it returns "authorization
header is malformed" exception. So, if we set `fs.s3a.endpoint=bucket.vpce
-<some_string>.s3.ca-central-1.vpce.amazonaws.com` and make s3 calls we get:
```
S3Exception: The authorization header is malformed; the region 'vpce' is wrong; expecting 'ca-central-1'
(Service: Amazon S3; Status Code: 400; Error Code: AuthorizationHeaderMalformed; Request ID: req-id; S3 Extended Request ID: req-id-2), S3 Extended Request ID: req-id-2:AuthorizationHeaderMalformed: The authorization
header is malformed; the region 'vpce' is wrong; expecting 'ca-central-1' (Service: Amazon S3; Status Code: 400; Error Code: AuthorizationHeaderMalformed; Request ID: req-id;
```
Cause:

Since, endpoint parsing is done in a way that it assumes the AWS S3 region
would be the 2nd component of the `fs.s3a.endpoint` URL delimited by ".", in
case of PrivateLink URL, it can't figure out the region and throws an
authorization exception. Thus, to add support to using PrivateLink URLs we use `fs.s3a.endpoint.region`
to set the region and bypass this parsing of `fs.s3a.endpoint`, in the case shown above to make it work we'll set the AWS
S3 region as `ca-central-1`.

```xml
<property>
  <name>fs.s3a.endpoint.region</name>
  <value>ca-central-1</value>
</property>
```

### `Classdoes not implement software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`

A credential provider listed in `fs.s3a.aws.credentials.provider` does not implement
the interface `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`.

```
InstantiationIOException: `s3a://stevel-gcs/': Class org.apache.hadoop.fs.s3a.S3ARetryPolicy does not implement software.amazon.awssdk.auth.credentials.AwsCredentialsProvider (configuration key fs.s3a.aws.credentials.provider)
        at org.apache.hadoop.fs.s3a.impl.InstantiationIOException.isNotInstanceOf(InstantiationIOException.java:128)
        at org.apache.hadoop.fs.s3a.S3AUtils.getInstanceFromReflection(S3AUtils.java:604)
        at org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.createAWSV2CredentialProvider(CredentialProviderListFactory.java:299)
        at org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.buildAWSProviderList(CredentialProviderListFactory.java:245)
        at org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.createAWSCredentialProviderList(CredentialProviderListFactory.java:144)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.bindAWSClient(S3AFileSystem.java:971)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:624)
        at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3601)
        at org.apache.hadoop.fs.FileSystem.access$300(FileSystem.java:171)
        at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3702)
        at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:3653)
        at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:555)
        at org.apache.hadoop.fs.Path.getFileSystem(Path.java:366)

```

There's two main causes

1. A class listed there is not an implementation of the interface.
   Fix: review the settings and correct as appropriate.
1. A class listed there does implement the interface, but it has been loaded in a different
   classloader, so the JVM does not consider it to be an implementation.
   Fix: learn the entire JVM classloader model and see if you can then debug it.
   Tip: having both the AWS Shaded SDK and individual AWS SDK modules on your classpath
   may be a cause of this.

If you see this and you are trying to use the S3A connector with Spark, then the cause can
be that the isolated classloader used to load Hive classes is interfering with the S3A
connector's dynamic loading of `software.amazon.awssdk` classes. To fix this, declare that
the classes in the aws SDK are loaded from the same classloader which instantiated
the S3A FileSystem instance:

```
spark.sql.hive.metastore.sharedPrefixes software.amazon.awssdk.
```

## <a name="access_denied"></a> "The security token included in the request is invalid"

You are trying to use session/temporary credentials and the session token
supplied is considered invalid.

This can surface if your configuration is setting the `fs.s3a.secret.key`,
`fs.s3a.access.key` and `fs.s3a.session.key` correctly, but the
AWS credential provider list set in `AWS_CREDENTIALS_PROVIDER` does not include
`org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`.


### <a name="access_denied_unknown-ID"></a> AccessDeniedException "The AWS Access Key Id you provided does not exist in our records."

The value of `fs.s3a.access.key` does not match a known access key ID.
It may be mistyped, or the access key may have been deleted by one of the account managers.

```
java.nio.file.AccessDeniedException: bucket: doesBucketExist on bucket:
    S3Exception:
    The AWS Access Key Id you provided does not exist in our records.

```

###  <a name="access_denied_disabled"></a> `AccessDeniedException` All access to this object has been disabled

Caller has no permission to access the bucket at all.

```
doesBucketExist on fdsd: java.nio.file.AccessDeniedException: fdsd: doesBucketExist on fdsd:
 S3Exception: All access to this object has been disabled
 (Service: Amazon S3; Status Code: 403; Error Code: AllAccessDisabled; Request ID: E6229D7F8134E64F;
  S3 Extended Request ID: 6SzVz2t4qa8J2Wxo/oc8yBuB13Mgrn9uMKnxVY0hsBd2kU/YdHzW1IaujpJdDXRDCQRX3f1RYn0=),
  S3 Extended Request ID: 6SzVz2t4qa8J2Wxo/oc8yBuB13Mgrn9uMKnxVY0hsBd2kU/YdHzW1IaujpJdDXRDCQRX3f1RYn0=:AllAccessDisabled
 All access to this object has been disabled (Service: Amazon S3; Status Code: 403;

```

Check the name of the bucket is correct, and validate permissions for the active user/role.

### <a name="access_denied_writing"></a> `AccessDeniedException` "Access denied" when trying to manipulate data

Data can be read, but attempts to write data or manipulate the store fail with
403/Access denied.

The bucket may have an access policy which the request does not comply with.
or the caller does not have the right to access the data.

```
java.nio.file.AccessDeniedException: test/: PUT 0-byte object  on test/:
 S3Exception: Access Denied (Service: Amazon S3; Status Code: 403;
 Error Code: AccessDenied; Request ID: EDC662AD2EEEA33C;

```

In the AWS S3 management console, select the "permissions" tab for the bucket, then "bucket policy".

If there is a bucket access policy, e.g. required encryption headers,
then the settings of the s3a client must guarantee the relevant headers are set
(e.g. the encryption options match).
Note: S3 Default Encryption options are not considered here:
if the bucket policy requires AES256 as the encryption policy on PUT requests,
then the encryption option must be set in the hadoop client so that the header is set.


Otherwise, the problem will likely be that the user does not have full access to the
operation. Check what they were trying to (read vs write) and then look
at the permissions of the user/role.

If the client using [assumed roles](assumed_roles.html), and a policy
is set in `fs.s3a.assumed.role.policy`, then that policy declares
_all_ the rights which the caller has.

### <a name="access_denied_renaming"></a> `AccessDeniedException` in rename, "MultiObjectDeleteException: One or more objects could not be deleted"


```
mv: rename s3a://london/dest to s3a://london/src on
s3a://london/dest:
    MultiObjectDeleteException: One or more objects
    could not be deleted (Service: null; Status Code: 200; Error Code: null; Request
    ID: 5C9018EF245F02C5; S3 Extended Request ID:
    5fQ2RVCPF0rdvADRv2XY3U4yb2J0gHRID/4jm1eqCXp7RxpU0dH9DliChYsCUD1aVCFtbwfWJWY=),
    S3 Extended Request ID:
    5fQ2RVCPF0rdvADRv2XY3U4yb2J0gHRID/4jm1eqCXp7RxpU0dH9DliChYsCUD1aVCFtbwfWJWY=:null:
    AccessDenied: dest/file10: Access Denied
```

The S3A connector's emulation of file and directory rename is implemented by copying each file,
then deleting the originals. This delete process is done in batches, by default in a single
"multiple object delete request". If one or more of the objects listed in the request cannot
be deleted, an error is returned in S3 listing which objects were not deleted.
If the cause was "access denied", it is translated into an `AccessDeniedException`.

The rename is halted at this point: files may be present in both the source and destination directories.
Those files which could not be deleted from the source directory will also have been copied
into the destination directory. Files which were successfully deleted from the source
directory will _only_ be in the destination. And files for which the rename operation
had yet to commence -they will only be in the source tree.

The user has to recover from this themselves. Be assured: no data will have been deleted, it
is just that the data may now be scattered across two directories.
Note: this is one reason why any application which tries to atomically commit work
via rename (classic Hadoop output committers, distcp with the `-atomic` option) are
not safe to use with S3. It is not a file system.

For an 'AccessDenied' failure, the root cause is IAM permissions.
The user/role/bucket must have the permission
`s3:DeleteObject` on the source path. It is safest to grant `s3:Delete*` so
that if a future version of the S3A connector supported extra operations
(explicit deletion of versioned files, get/set/delete object tagging, ...),
the client will have the permission to use them.

### <a name="kms_access_denied"></a>  `AccessDeniedException` when using SSE-KMS

When trying to write or read SEE-KMS-encrypted data, the client gets a
`java.nio.AccessDeniedException` with the error 403/Forbidden.

The caller does not have the permissions to access
the key with which the data was encrypted.

### <a name="access_denied_requester_pays"></a>`AccessDeniedException` when using a "Requester Pays" enabled bucket

When making cross-account requests to a requester pays enabled bucket, all calls must acknowledge via a header that the requester will be billed.

If you don't enable this acknowledgement within S3A, then you will see a message similar to this:

```
java.nio.file.AccessDeniedException: s3a://my-bucket/my-object: getFileStatus on s3a://my-bucket/my-object:
S3Exception: Forbidden (Service: Amazon S3; Status Code: 403;
Error Code: 403 Forbidden; Request ID: myshortreqid; S3 Extended Request ID: mylongreqid):403 Forbidden
```

To enable requester pays, set `fs.s3a.requester.pays.enabled` property to `true`.

### <a name="access_denied_archive_storage_class"></a>`AccessDeniedException` "InvalidObjectState" when trying to read files

```
java.nio.file.AccessDeniedException: file1: copyFile(file1, file2) on file1: S3Exception: Operation is not valid for the source object's storage class (Service: Amazon S3; Status Code: 403; Error Code: InvalidObjectState; Request ID: SK9EMPC1YRX75VZR; S3 Extended Request ID: /nhUfdwJ+y5DLz6B4YR2FdA0FnQWwhDAkSCakn42zs2JssK3qWTrfwdNDiy6bOyXHOvJY0VAlHw=; Proxy: null), S3 Extended Request ID: /nhUfdwJ+y5DLz6B4YR2FdA0FnQWwhDAkSCakn42zs2JssK3qWTrfwdNDiy6bOyXHOvJY0VAlHw=:InvalidObjectState

Caused by: S3Exception: Operation is not valid for the source object's storage class (Service: Amazon S3; Status Code: 403; Error Code: InvalidObjectState; Request ID: SK9EMPC1YRX75VZR; S3 Extended Request ID: /nhUfdwJ+y5DLz6B4YR2FdA0FnQWwhDAkSCakn42zs2JssK3qWTrfwdNDiy6bOyXHOvJY0VAlHw=; Proxy: null), S3 Extended Request ID: /nhUfdwJ+y5DLz6B4YR2FdA0FnQWwhDAkSCakn42zs2JssK3qWTrfwdNDiy6bOyXHOvJY0VAlHw=
```

This happens when you're trying to read or copy files that have archive storage class such as
Glacier.

If you want to access the file with S3A after writes, do not set `fs.s3a.create.storage.class` to `glacier` or `deep_archive`.

### <a name="no_region_session_credentials"></a> "Unable to find a region via the region provider chain." when using session credentials.

Region must be provided when requesting session credentials, or an exception will be thrown with the
message:

```
 Unable to find a region via the region provider
chain. Must provide an explicit region in the builder or setup environment to supply a region.
```

In this case you have to set the `fs.s3a.assumed.role.sts.endpoint` property to a valid S3 sts
endpoint and region like the following:

```xml

<property>
  <name>fs.s3a.assumed.role.sts.endpoint</name>
  <value>${sts.endpoint}</value>
</property>

<property>
  <name>fs.s3a.assumed.role.sts.endpoint.region</name>
  <value>${sts.region}</value>
</property>
```


## <a name="connectivity"></a> Connectivity Problems

### <a name="bad_endpoint"></a> Error "The bucket you are attempting to access must be addressed using the specified endpoint"

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

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

Using the explicit endpoint for the region is recommended for speed.

### <a name="NoRegion"></a>  `Unable to find a region via the region provider chain`

S3A client creation fails, possibly after a pause of some seconds.

This failure surfaces when _all_ the following conditions are met:

1. Deployment outside EC2.
1. `fs.s3a.endpoint` is unset.
1. `fs.s3a.endpoint.region` is set to `""`.   (Hadoop 3.3.2+ only)
1. Without the file `~/.aws/config` existing or without a region set in it.
1. Without the JVM system property `aws.region` declaring a region.
1. Without the environment variable `AWS_REGION` declaring a region.

Stack trace (Hadoop 3.3.1):
```
Unable to find a region via the region provider chain.
        Must provide an explicit region in the builder or setup environment to supply a region.

```

Log and stack trace on later releases, with
"S3A filesystem client is using the SDK region resolution chain."
warning that the SDK resolution chain is in use:

```
2021-06-23 19:56:55,971 [main] WARN  s3a.DefaultS3ClientFactory (LogExactlyOnce.java:warn(39)) -
    S3A filesystem client is using the SDK region resolution chain.

2021-06-23 19:56:56,073 [main] WARN  fs.FileSystem (FileSystem.java:createFileSystem(3464)) -
    Failed to initialize filesystem s3a://osm-pds/planet:
 org.apache.hadoop.fs.s3a.AWSClientIOException: creating AWS S3 client on s3a://osm-pds:
  com.amazonaws.SdkClientException: Unable to find a region via the region provider chain.
  Must provide an explicit region in the builder or setup environment to supply a region.:
   Unable to find a region via the region provider chain.
    Must provide an explicit region in the builder or setup environment to supply a region.
    at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:208)
    at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:122)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.bindAWSClient(S3AFileSystem.java:788)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:478)
    at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3460)
    at org.apache.hadoop.fs.FileSystem.access$300(FileSystem.java:172)
    at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3565)
    at org.apache.hadoop.fs.FileSystem$Cache.getUnique(FileSystem.java:3518)
    at org.apache.hadoop.fs.FileSystem.newInstance(FileSystem.java:592)

```

Due to changes in S3 client construction in Hadoop 3.3.1 this option surfaces in
non-EC2 deployments where no AWS endpoint was declared:
[HADOOP-17771](https://issues.apache.org/jira/browse/HADOOP-17771). On Hadoop
3.3.2 and later it takes active effort to create this stack trace.

**Fix: set `fs.s3a.endpoint` to `s3.amazonaws.com`**

Set `fs.s3a.endpoint` to the endpoint where the data is stored
(best), or to `s3.amazonaws.com` (second-best).

```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

For Apache Spark, this can be done in `spark-defaults.conf`

```
spark.hadoop.fs.s3a.endpoint s3.amazonaws.com
```

Or in Scala by editing the spark configuration during setup.

```scala
sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
```

Tip: set the logging of `org.apache.hadoop.fs.s3a.DefaultS3ClientFactory`
to `DEBUG` to see how the endpoint and region configuration is determined.

```
log4j.logger.org.apache.hadoop.fs.s3a.DefaultS3ClientFactory=DEBUG
```

### <a name="timeout_from_pool"></a> "Timeout waiting for connection from pool" when writing data

This happens when using the output stream thread pool runs out of capacity.

```
[s3a-transfer-shared-pool1-t20] INFO  http.AmazonHttpClient (AmazonHttpClient.java:executeHelper(496))
 - Unable to execute HTTP request:
   org.apache.http.conn.ConnectionPoolTimeoutException:
   Timeout waiting for connection from pool

```

Make sure that `fs.s3a.connection.maximum` is at least larger
than `fs.s3a.threads.max`.

```xml
<property>
  <name>fs.s3a.threads.max</name>
  <value>64</value>
</property>

<property>
  <name>fs.s3a.connection.maximum</name>
  <value>64</value>
</property>
```

### "Timeout waiting for connection from pool" when reading data

This happens when more threads are trying to read from an S3A system than
the maximum number of allocated HTTP connections.

Set `fs.s3a.connection.maximum` to a larger value (and at least as large as
`fs.s3a.threads.max`)


### `NoHttpResponseException`

The HTTP Server did not respond.

```
2017-02-07 10:01:07,950 INFO [s3a-transfer-shared-pool1-t7]   Unable to execute HTTP request: bucket.s3.amazonaws.com:443 failed to respond
org.apache.http.NoHttpResponseException: bucket.s3.amazonaws.com:443 failed to respond

```

Probably network problems, unless it really is an outage of S3.

If you are working with a third party store, check its network configuration.


### Out of heap memory when writing with via Fast Upload

This can happen when using the upload buffering mechanism
uses memory (either `fs.s3a.fast.upload.buffer=array` or
`fs.s3a.fast.upload.buffer=bytebuffer`).

More data is being generated than in the JVM than it can upload to S3 —and
so much data has been buffered that the JVM has run out of memory.

1. Consult [S3A Fast Upload Thread Tuning](./index.html#fast_upload_thread_tuning) for
detail on this issue and options to address it.

1. Switch to buffering to disk, rather than memory.


This surfaces if, while a multipart upload was taking place, all outstanding multipart
uploads were garbage collected. The upload operation cannot complete because
the data uploaded has been deleted.

Consult [Cleaning up After Incremental Upload Failures](./index.html#multipart_purge) for
details on how the multipart purge timeout can be set. If multipart uploads
are failing with the message above, it may be a sign that this value is too low.

### `MultiObjectDeleteException` during delete or rename of files

```
Exception in thread "main" com.amazonaws.services.s3.model.MultiObjectDeleteException:
    Status Code: 0, AWS Service: null, AWS Request ID: null, AWS Error Code: null,
    AWS Error Message: One or more objects could not be deleted, S3 Extended Request ID: null
  at com.amazonaws.services.s3.AmazonS3Client.deleteObjects(AmazonS3Client.java:1745)
```
This happens when trying to delete multiple objects, and one of the objects
could not be deleted. It *should not occur* just because the object is missing.
More specifically: at the time this document was written, we could not create
such a failure.

It will occur if the caller lacks the permission to delete any of the objects.

Consult the log to see the specifics of which objects could not be deleted.
Do you have permission to do so?

If this operation is failing for reasons other than the caller lacking
permissions:

1. Try setting `fs.s3a.multiobjectdelete.enable` to `false`.
1. Consult [HADOOP-11572](https://issues.apache.org/jira/browse/HADOOP-11572)
for up to date advice.

### "Failed to Sanitize XML document"

```
org.apache.hadoop.fs.s3a.AWSClientIOException: getFileStatus on test/testname/streaming/:
  Failed to sanitize XML document

```

We believe this is caused by the connection to S3 being broken.
See [HADOOP-13811](https://issues.apache.org/jira/browse/HADOOP-13811).

It may go away if the operation is retried.

### JSON Parse Error from AWS SDK

Sometimes a JSON Parse error is reported with the stack trace from `software.amazon.awssdk`,

Again, we believe this is caused by the connection to S3 being broken.

It may go away if the operation is retried.


## <a name="semantics"></a>File System Semantics

These are the issues where S3 does not appear to behave the way a filesystem
"should". That's because it "isn't".


### File not visible/saved

The files in an object store are not visible until the write has been completed.
In-progress writes are simply saved to a local file/cached in RAM and only uploaded.
at the end of a write operation. If a process terminated unexpectedly, or failed
to call the `close()` method on an output stream, the pending data will have
been lost.

### File `flush()` calls do not save data to S3

Again, this is due to the fact that the data is cached locally until the
`close()` operation. The S3A filesystem cannot be used as a store of data
if it is required that the data is persisted durably after every
`Syncable.hflush()` or `Syncable.hsync()` call.
This includes resilient logging, HBase-style journaling
and the like. The standard strategy here is to save to HDFS and then copy to S3.

### <a name="syncable"></a> `UnsupportedOperationException` "S3A streams are not Syncable. See HADOOP-17597."

The application has tried to call either the `Syncable.hsync()` or `Syncable.hflush()`
methods on an S3A output stream. This has been rejected because the
connector isn't saving any data at all. The `Syncable` API, especially the
`hsync()` call, are critical for applications such as HBase to safely
persist data.

When configured to do so, the S3A connector throws an `UnsupportedOperationException`
when these API calls are made, because the API guarantees absolutely cannot be met:
_nothing is being flushed or saved_.

* Applications which intend to invoke the Syncable APIs should call `hasCapability("hsync")` on
  the stream to see if they are supported.
* Or catch and downgrade `UnsupportedOperationException`.

These recommendations _apply to all filesystems_.

For consistency with other filesystems, S3A output streams
do not by default reject the `Syncable` calls -instead
they print a warning of its use.


The count of invocations of the two APIs are collected in the S3A filesystem
Statistics/IOStatistics and so their use can be monitored.

To switch the S3A connector to rejecting all use of
`hsync()` or `hflush()` calls, set the option
`fs.s3a.downgrade.syncable.exceptions` to `false`.

```xml
<property>
  <name>fs.s3a.downgrade.syncable.exceptions</name>
  <value>false</value>
</property>
```

Regardless of the setting, the `Syncable` API calls do not work.
Telling the store to *not* downgrade the calls is a way to
1. Prevent applications which require Syncable to work from being deployed
  against S3.
2. Identify applications which are making the calls even though they don't
  need to. These applications can then be fixed -something which may take
  time.

Put differently: it is safest to disable downgrading syncable exceptions.
However, enabling the downgrade stops applications unintentionally using the API
from breaking.

*Tip*: try turning it on in staging environments to see what breaks.

### `RemoteFileChangedException` and read-during-overwrite

```
org.apache.hadoop.fs.s3a.RemoteFileChangedException: re-open `s3a://my-bucket/test/file.txt':
  Change reported by S3 while reading at position 1949.
  ETag f9c186d787d4de9657e99f280ba26555 was unavailable

```

If an S3 object is updated while an S3A filesystem reader has an open
`InputStream` on it, the reader may encounter `RemoteFileChangedException`.  This
occurs if the S3A `InputStream` needs to re-open the object (e.g. during a seek())
and detects the change.

If the change detection mode is configured to 'warn', a warning like the
following will be seen instead of `RemoteFileChangedException`:

```
WARN  - ETag change detected on re-open s3a://my-bucket/test/readFileToChange.txt at 1949.
 Expected f9c186d787d4de9657e99f280ba26555 got 043abff21b7bd068d2d2f27ccca70309
```

Using a third-party S3 implementation that doesn't support eTags might result in
the following error.

```
org.apache.hadoop.fs.s3a.NoVersionAttributeException: `s3a://my-bucket/test/file.txt':
 Change detection policy requires ETag
  at org.apache.hadoop.fs.s3a.impl.ChangeTracker.processResponse(ChangeTracker.java:153)

```

If the change policy is `versionid` there are a number of possible causes

* The bucket does not have object versioning enabled.
* The bucket does have versioning enabled, but the object being read was created
before versioning was enabled.
* The bucket is on a third-party store which does not support object versioning.

See [Handling Read-During-Overwrite](./index.html#handling_read-during-overwrite)
for more information.


### Rename not behaving as "expected"

S3 is not a filesystem. The S3A connector mimics file and directory rename by

* HEAD then LIST of source path. The source MUST exist, else a `FileNotFoundException`
  is raised.
* HEAD then LIST of the destination path.
  This SHOULD NOT exist.
  If it does and if the source is a directory, the destination MUST be an empty directory.
  If the source is a file, the destination MAY be a directory, empty or not.
  If the destination exists and relevant conditions are not met, a `FileAlreadyExistsException`
  is raised.
* If the destination path does not exist, a HEAD request of the parent path
  to verify that there is no object there.
  Directory markers are not checked for, nor that the path has any children,
* File-by-file copy of source objects to destination.
  Parallelized, with page listings of directory objects and issuing of DELETE requests.
* Post-delete recreation of source parent directory marker, if needed.

This is slow (`O(data)`) and can cause timeouts on code which is required
to send regular progress reports/heartbeats -for example, distCp.
It is _very unsafe_ if the calling code expects atomic renaming as part
of any commit algorithm.
This is why the [S3A Committers](committers.md) or similar are needed to safely
commit output.

There is also the risk of race conditions arising if many processes/threads
are working with the same directory tree
[HADOOP-16721](https://issues.apache.org/jira/browse/HADOOP-16721).

To reduce this risk, since Hadoop 3.3.1, the S3A connector no longer verifies the parent directory
of the destination of a rename is a directory -only that it is _not_ a file.
You can rename a directory or file deep under a file if you try -after which
there is no guarantee of the files being found in listings. Try not to do that.

## <a name="encryption"></a> S3 Server Side Encryption

### `AWSS3IOException` `KMS.NotFoundException` "Invalid arn" when using SSE-KMS

When performing file operations, the user may run into an issue where the KMS
key arn is invalid.

```
org.apache.hadoop.fs.s3a.AWSS3IOException: innerMkdirs on /test:
 S3Exception:
  Invalid arn (Service: Amazon S3; Status Code: 400; Error Code: KMS.NotFoundException;
   Request ID: CA89F276B3394565),
   S3 Extended Request ID: ncz0LWn8zor1cUO2fQ7gc5eyqOk3YfyQLDn2OQNoe5Zj/GqDLggUYz9QY7JhdZHdBaDTh+TL5ZQ=:
   Invalid arn (Service: Amazon S3; Status Code: 400; Error Code: KMS.NotFoundException; Request ID: CA89F276B3394565)
```

Possible causes:

* the KMS key ARN is entered incorrectly, or
* the KMS key referenced by the ARN is in a different region than the S3 bucket
being used.

### Using SSE-C "Bad Request"

When performing file operations the user may run into an unexpected 400/403
error such as
```
org.apache.hadoop.fs.s3a.AWSS3IOException: getFileStatus on fork-4/:
 S3Exception:
Bad Request (Service: Amazon S3; Status Code: 400;
Error Code: 400 Bad Request; Request ID: 42F9A1987CB49A99),
S3 Extended Request ID: jU2kcwaXnWj5APB14Cgb1IKkc449gu2+dhIsW/+7x9J4D+VUkKvu78mBo03oh9jnOT2eoTLdECU=:
Bad Request (Service: Amazon S3; Status Code: 400; Error Code: 400 Bad Request; Request ID: 42F9A1987CB49A99)
```

This can happen in the cases of not specifying the correct SSE-C encryption key.
Such cases can be as follows:
1. An object is encrypted using SSE-C on S3 and either the wrong encryption type
is used, no encryption is specified, or the SSE-C specified is incorrect.
2. A directory is encrypted with a SSE-C keyA and the user is trying to move a
file using configured SSE-C keyB into that structure.

## <a name="client-side-encryption"></a> S3 Client Side Encryption

### Instruction file not found for S3 object

Reading an unencrypted file would fail when read through CSE enabled client.
```
java.lang.SecurityException: Instruction file not found for S3 object with bucket name: ap-south-cse, key: unencryptedData.txt

```
CSE enabled client should read encrypted data only.

### CSE-KMS method requires KMS key ID

KMS key ID is required for CSE-KMS to encrypt data, not providing one leads
 to failure.

```
2021-07-07 11:33:04,550 WARN fs.FileSystem: Failed to initialize filesystem
s3a://ap-south-cse/: java.lang.IllegalArgumentException: CSE-KMS
method requires KMS key ID. Use fs.s3a.encryption.key property to set it.
-ls: CSE-KMS method requires KMS key ID. Use fs.s3a.encryption.key property to
 set it.
```

set `fs.s3a.encryption.key=<KMS_KEY_ID>` generated through AWS console.

### `software.amazon.awssdk.services.kms.model.IncorrectKeyException` The key ID in the request does not identify a CMK that can perform this operation.

KMS key ID used to PUT(encrypt) the data, must be the one used to GET the
data.
 ```
cat: open s3a://ap-south-cse/encryptedData.txt at 0 on
s3a://ap-south-cse/encryptedData.txt:
software.amazon.awssdk.services.kms.model.IncorrectKeyException: The key ID in the
request does not identify a CMK that can perform this operation. (Service: AWSKMS;
Status Code: 400; ErrorCode: IncorrectKeyException;
Request ID: da21aa8a-f00d-467c-94a0-32b627d32bc0; Proxy: null):IncorrectKeyException:
The key ID in the request does not identify a CMK that can perform this
operation. (Service: AWSKMS ; Status Code: 400; Error Code: IncorrectKeyException;
Request ID: da21aa8a-f00d-467c-94a0-32b627d32bc0; Proxy: null)
```
Use the same KMS key ID used to upload data to download and read it as well.

### `software.amazon.awssdk.services.kms.model.NotFoundException` key/<KMS_KEY_ID> does not exist

Using a KMS key ID from a different region than the bucket used to store data
 would lead to failure while uploading.

```
mkdir: PUT 0-byte object  on testmkdir:
software.amazon.awssdk.services.kms.model.NotFoundException: Key
'arn:aws:kms:ap-south-1:152813717728:key/<KMS_KEY_ID>'
does not exist (Service: AWSKMS; Status Code: 400; Error Code: NotFoundException;
Request ID: 279db85d-864d-4a38-9acd-d892adb504c0; Proxy: null):NotFoundException:
Key 'arn:aws:kms:ap-south-1:152813717728:key/<KMS_KEY_ID>'
does not exist(Service: AWSKMS; Status Code: 400; Error Code: NotFoundException;
Request ID: 279db85d-864d-4a38-9acd-d892adb504c0; Proxy: null)
```
While generating the KMS Key ID make sure to generate it in the same region
 as your bucket.

### Unable to perform range get request: Range get support has been disabled

If Range get is not supported for a CSE algorithm or is disabled:
```
java.lang.SecurityException: Unable to perform range get request: Range get support has been disabled. See https://docs.aws.amazon.com/general/latest/gr/aws_sdk_cryptography.html

```
Range gets must be enabled for CSE to work.

### WARNING: Range gets do not provide authenticated encryption properties even when used with an authenticated mode (AES-GCM).

The S3 Encryption Client is configured to support range get requests. This
 warning would be shown everytime S3-CSE is used.
```
2021-07-14 12:54:09,525 [main] WARN  s3.AmazonS3EncryptionClientV2
(AmazonS3EncryptionClientV2.java:warnOnRangeGetsEnabled(401)) - The S3
Encryption Client is configured to support range get requests. Range gets do
not provide authenticated encryption properties even when used with an
authenticated mode (AES-GCM). See https://docs.aws.amazon.com/general/latest
/gr/aws_sdk_cryptography.html
```
We can Ignore this warning since, range gets must be enabled for S3-CSE to
get data.

### WARNING: If you don't have objects encrypted with these legacy modes, you should disable support for them to enhance security.

The S3 Encryption Client is configured to read encrypted data with legacy
encryption modes through the CryptoMode setting, and we would see this
warning for all S3-CSE request.

```
2021-07-14 12:54:09,519 [main] WARN  s3.AmazonS3EncryptionClientV2
(AmazonS3EncryptionClientV2.java:warnOnLegacyCryptoMode(409)) - The S3
Encryption Client is configured to read encrypted data with legacy
encryption modes through the CryptoMode setting. If you don't have objects
encrypted with these legacy modes, you should disable support for them to
enhance security. See https://docs.aws.amazon.com/general/latest/gr/aws_sdk_cryptography.html
```
We can ignore this, since this CryptoMode setting(CryptoMode.AuthenticatedEncryption)
is required for range gets to work.

### `software.amazon.awssdk.services.kms.mode.InvalidKeyUsageException: You cannot generate a data key with an asymmetric CMK`

If you generated an Asymmetric CMK from AWS console then CSE-KMS won't be
able to generate unique data key for encryption.

```
Caused by: software.amazon.awssdk.services.kms.mode.InvalidKeyUsageException:
You cannot generate a data key with an asymmetric CMK
(Service: AWSKMS; Status Code: 400; Error Code: InvalidKeyUsageException; Request ID: 93609c15-e490-4035-8390-f4396f0d90bf; Proxy: null)
```

Generate a Symmetric Key in the same region as your S3 storage for CSE-KMS to
work.

### software.amazon.awssdk.services.kms.mode.NotFoundException: Invalid keyId

If the value in `fs.s3a.encryption.key` property, does not exist
/valid in AWS KMS CMK(Customer managed keys), then this error would be seen.

```
Caused by: software.amazon.awssdk.services.kms.model.NotFoundException: Invalid keyId abc
(Service: AWSKMS; Status Code: 400; Error Code: NotFoundException; Request ID:
 9d53552a-3d1b-47c8-984c-9a599d5c2391; Proxy: null)
```

Check if `fs.s3a.encryption.key` is set correctly and matches the
same on AWS console.

### software.amazon.awssdk.services.kms.model.KmsException: User: <User_ARN> is not authorized to perform : kms :GenerateDataKey on resource: <KEY_ID>

User doesn't have authorization to the specific AWS KMS Key ID.
```
Caused by: software.amazon.awssdk.services.kms.model.KmsException:
User: arn:aws:iam::152813717728:user/<user> is not authorized to perform:
 kms:GenerateDataKey on resource: <key_ID>
(Service: AWSKMS; Status Code: 400; Error Code: AccessDeniedException;
  Request ID: 4ded9f1f-b245-4213-87fc-16cba7a1c4b9; Proxy: null)
```

The user trying to use the KMS Key ID should have the right permissions to access
(encrypt/decrypt) using the AWS KMS Key used via `fs.s3a.encryption.key`.
If not, then add permission(or IAM role) in "Key users" section by selecting the
AWS-KMS CMK Key on AWS console.


### <a name="not_all_bytes_were_read"></a> Message appears in logs "Not all bytes were read from the S3ObjectInputStream"


This is a message which can be generated by the Amazon SDK when the client application
calls `abort()` on the HTTP input stream, rather than reading to the end of
the file/stream and causing `close()`. The S3A client does call `abort()` when
seeking round large files, [so leading to the message](https://github.com/aws/aws-sdk-java/issues/1211).

No ASF Hadoop releases have shipped with an SDK which prints this message
when used by the S3A client. However third party and private builds of Hadoop
may cause the message to be logged.

Ignore it. The S3A client does call `abort()`, but that's because our benchmarking
shows that it is generally more efficient to abort the TCP connection and initiate
a new one than read to the end of a large file.

Note: the threshold when data is read rather than the stream aborted can be tuned
by `fs.s3a.readahead.range`; seek policy in `fs.s3a.experimental.input.fadvise`.

### <a name="upload_failure"></a> `PathIOException` Number of parts in multipart upload exceeded.

Number of parts in multipart upload exceeded

```
org.apache.hadoop.fs.PathIOException: `test/testMultiPartUploadFailure': Number of parts in multipart upload exceeded. Current part count = X, Part count limit = Y

    at org.apache.hadoop.fs.s3a.WriteOperationHelper.newUploadPartRequest(WriteOperationHelper.java:432)
    at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload.uploadBlockAsync(S3ABlockOutputStream.java:627)
    at org.apache.hadoop.fs.s3a.S3ABlockOutputStream$MultiPartUpload.access$000(S3ABlockOutputStream.java:532)
    at org.apache.hadoop.fs.s3a.S3ABlockOutputStream.uploadCurrentBlock(S3ABlockOutputStream.java:316)
    at org.apache.hadoop.fs.s3a.S3ABlockOutputStream.write(S3ABlockOutputStream.java:301)
```

This is a known issue where upload fails if number of parts
is more than 10000 (specified by aws SDK). You can configure
`fs.s3a.multipart.size` to reduce the number of parts.

### <a name="no_such_bucket"></a> `UnknownStoreException` Bucket does not exist.

The bucket does not exist.

```
org.apache.hadoop.fs.s3a.UnknownStoreException: `s3a://random-bucket-7d9217b0-b426-4344-82ea-25d6cbb316f1/':
        Bucket does not exist: software.amazon.awssdk.services.s3.model.NoSuchBucketException: null
    (Service: S3, Status Code: 404, Request ID: RD254TC8EVDV98AK,
    Extended Request ID: 49F5CO1IKavFsz+VBecf2uwZeNVar3InHkdIrONvAK5yQ73gqZ1hFoAEMo8/x5wRNe3OXO3aebvZkev2bS81kw==)
    (Service: S3, Status Code: 404, Request ID: RD254TC8EVDV98AK): null
    (Service: S3, Status Code: 404, Request ID: RD254TC8EVDV98AK, Extended Request ID: 49F5CO1IKavFsz+VBecf2uwZeNVar3InHkdIrONvAK5yQ73gqZ1hFoAEMo8/x5wRNe3OXO3aebvZkev2bS81kw==)
    (Service: S3, Status Code: 404, Request ID: RD254TC8EVDV98AK)
```

Check the URI is correct, and that the bucket actually exists.

If using a third-party store, verify that you've configured
the client to talk to the specific server in `fs.s3a.endpoint`.
Forgetting to update this value and asking the AWS S3 endpoint
for a bucket is not an unusual occurrence.

This can surface during filesystem API calls if the bucket is deleted while you are using it,
 -or the startup check for bucket existence has been disabled by setting `fs.s3a.bucket.probe` to 0.


## <a name="s3guard"></a> S3Guard Errors

S3Guard has been completely cut from the s3a connector
[HADOOP-17409 Remove S3Guard - no longer needed](HADOOP-17409 Remove S3Guard - no longer needed).

To avoid consistency problems with older releases, if an S3A filesystem is configured to use DynamoDB the filesystem
will fail to initialize.

### <a name="s3guard unsupported"></a> S3Guard is no longer needed/supported

The option `fs.s3a.metadatastore.impl` or the per-bucket version has a value of
`org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore`

```
org.apache.hadoop.fs.PathIOException: `s3a://production-london': S3Guard is no longer needed/supported,
 yet s3a://production-london is configured to use DynamoDB as the S3Guard metadata store.
 This is no longer needed or supported.
 Origin of setting is fs.s3a.bucket.production-london.metadatastore.impl via [core-site.xml]
        at org.apache.hadoop.fs.s3a.s3guard.S3Guard.checkNoS3Guard(S3Guard.java:111)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:540)
        at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3459)
        at org.apache.hadoop.fs.FileSystem.access$300(FileSystem.java:171)
        at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3564)
        at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:3511)
        at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:538)
        at org.apache.hadoop.fs.Path.getFileSystem(Path.java:366)
        at org.apache.hadoop.fs.shell.PathData.expandAsGlob(PathData.java:342)
        at org.apache.hadoop.fs.shell.Command.expandArgument(Command.java:252)
        at org.apache.hadoop.fs.shell.Command.expandArguments(Command.java:235)
        at org.apache.hadoop.fs.shell.FsCommand.processRawArguments(FsCommand.java:105)
        at org.apache.hadoop.fs.shell.Command.run(Command.java:179)
        at org.apache.hadoop.fs.FsShell.run(FsShell.java:327)
        at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:81)
        at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:95)
        at org.apache.hadoop.fs.FsShell.main(FsShell.java:390)

ls: `s3a://production-london': S3Guard is no longer needed/supported,
    yet s3a://production-london is configured to use DynamoDB as the S3Guard metadata store.
    This is no longer needed or supported.
    Origin of setting is fs.s3a.bucket.production-london.metadatastore.impl via [core-site.xml]
```

The error message will state the property from where it came, here `fs.s3a.bucket.production-london.metadatastore.impl` and which
file the option was set if known, here `core-site.xml`.

Fix: remove the configuration options enabling S3Guard.

Consult the [S3Guard documentation](s3guard.html) for more details.

## <a name="other"></a> Other Errors

### <a name="integrity"></a> `SdkClientException` Unable to verify integrity of data upload

Something has happened to the data as it was uploaded.

```
Caused by: org.apache.hadoop.fs.s3a.AWSClientIOException: saving output on dest/_task_tmp.-ext-10000/_tmp.000000_0:
    com.amazonaws.AmazonClientException: Unable to verify integrity of data upload.
    Client calculated content hash (contentMD5: L75PalQk0CIhTp04MStVOA== in base 64)
    didn't match hash (etag: 37ace01f2c383d6b9b3490933c83bb0f in hex) calculated by Amazon S3.
    You may need to delete the data stored in Amazon S3.
    (metadata.contentMD5: L75PalQk0CIhTp04MStVOA==, md5DigestStream: null,
    bucketName: ext2, key: dest/_task_tmp.-ext-10000/_tmp.000000_0):
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:144)
  at org.apache.hadoop.fs.s3a.S3AOutputStream.close(S3AOutputStream.java:121)
  at org.apache.hadoop.fs.FSDataOutputStream$PositionCache.close(FSDataOutputStream.java:72)
  at org.apache.hadoop.fs.FSDataOutputStream.close(FSDataOutputStream.java:106)
  at org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat$1.close(HiveIgnoreKeyTextOutputFormat.java:99)
  at org.apache.hadoop.hive.ql.exec.FileSinkOperator$FSPaths.closeWriters(FileSinkOperator.java:190)
  ... 22 more
Caused by: com.amazonaws.AmazonClientException: Unable to verify integrity of data upload.
  Client calculated content hash (contentMD5: L75PalQk0CIhTp04MStVOA== in base 64)
  didn't match hash (etag: 37ace01f2c383d6b9b3490933c83bb0f in hex) calculated by Amazon S3.
  You may need to delete the data stored in Amazon S3.
  (metadata.contentMD5: L75PalQk0CIhTp04MStVOA==, md5DigestStream: null,
  bucketName: ext2, key: dest/_task_tmp.-ext-10000/_tmp.000000_0)
  at com.amazonaws.services.s3.AmazonS3Client.putObject(AmazonS3Client.java:1492)
  at com.amazonaws.services.s3.transfer.internal.UploadCallable.uploadInOneChunk(UploadCallable.java:131)
  at com.amazonaws.services.s3.transfer.internal.UploadCallable.call(UploadCallable.java:123)
  at com.amazonaws.services.s3.transfer.internal.UploadMonitor.call(UploadMonitor.java:139)
  at com.amazonaws.services.s3.transfer.internal.UploadMonitor.call(UploadMonitor.java:47)
  ... 4 more
```

As it uploads data to S3, the AWS SDK builds up an MD5 checksum of what was
PUT/POSTed. When S3 returns the checksum of the uploaded data, that is compared
with the local checksum. If there is a mismatch, this error is reported.

The uploaded data is already on S3 and will stay there, though if this happens
during a multipart upload, it may not be visible (but still billed: clean up
your multipart uploads via the `hadoop s3guard uploads` command).

Possible causes for this

1. A (possibly transient) network problem, including hardware faults.
1. A proxy server is doing bad things to the data.
1. Some signing problem, especially with third-party S3-compatible object
   stores.

This is a very, very rare occurrence.


### `AWSS3IOException` The Content-MD5 you specified did not match what we received

Reads work, but writes, even `mkdir`, fail:

This has been seen with third party stores.

If the store is configured to require content-MD5 headers with data uploaded: disable it.

If the store requires the use of the v2 signing algorithm, know that it is unsupported on this release.

### When writing data: "java.io.FileNotFoundException: Completing multi-part upload"

A multipart upload was trying to complete, but failed as there was no upload
with that ID.

```
java.io.FileNotFoundException: Completing multi-part upload on fork-5/test/multipart/1c397ca6-9dfb-4ac1-9cf7-db666673246b:
 S3Exception: The specified upload does not exist.
  The upload ID may be invalid, or the upload may have been aborted or completed.
   (Service: Amazon S3; Status Code: 404; Error Code: NoSuchUpload;
```

This can happen when all outstanding uploads have been aborted, including the
active ones.

If the bucket has a lifecycle policy of deleting multipart uploads, make sure
that the expiry time of the deletion is greater than that required for all open
writes to complete the write,
*and for all jobs using the S3A committers to commit their work.*

### Application hangs after reading a number of files

The pool of https client connections and/or IO threads have been used up, and
none are being freed.

1. The pools aren't big enough.
   See ["Timeout waiting for connection from pool"](#timeout_from_pool)
2. Likely root cause: whatever code is reading files isn't calling `close()`
   on the input streams. Make sure your code does this!
   And if it's someone else's: make sure you have a recent version; search their
   issue trackers to see if its a known/fixed problem. If not, it's time to work
   with the developers, or come up with a workaround
   (i.e closing the input stream yourself).

### Issue: when writing data, HTTP Exceptions logged at info from `AmazonHttpClient`

```
[s3a-transfer-shared-pool4-t6] INFO  http.AmazonHttpClient (AmazonHttpClient.java:executeHelper(496))
 - Unable to execute HTTP request: hwdev-steve-ireland-new.s3.amazonaws.com:443 failed to respond
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

The above error could be because of a stale http connections. The default value in AWS
SDK is set to -1 (infinite) which means the connection will be reused indefinitely.
We have introduced a new config `fs.s3a.connection.ttl` to configure this.
Tuning this setting down (together with an appropriately-low setting for Java's DNS cache TTL)
ensures that your application will quickly rotate over to new IP addresses when the
service begins announcing them through DNS, at the cost of having to re-establish new
connections more frequently.

```xml
<property>
  <name>fs.s3a.connection.ttl</name>
  <value>300000</value>
  <description>
      Expiration time for a connection in the connection pool in milliseconds.
      When a connection is retrieved from the connection pool,
      this parameter is checked to see if the connection can be reused.
      Default value is 5 minutes.
  </description>
</property>
```
### `AWSBadRequestException` IllegalLocationConstraintException/The unspecified location constraint is incompatible

```
 Cause: org.apache.hadoop.fs.s3a.AWSBadRequestException: put on :
  S3Exception:
   The unspecified location constraint is incompatible for the region specific
    endpoint this request was sent to.
    (Service: Amazon S3; Status Code: 400; Error Code: IllegalLocationConstraintException;
  ...
  Cause: S3Exception:
   The unspecified location constraint is incompatible for the region specific endpoint
   this request was sent to. (Service: Amazon S3; Status Code: 400; Error Code: IllegalLocationConstraintException;
   Request ID: EEBC5A08BCB3A645)

```

Something has been trying to write data to "/".

## <a name="best"></a> Best Practises

### <a name="logging"></a> Enabling low-level logging

The AWS SDK and the Apache S3 components can be configured to log at
more detail, as can S3A itself.

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
log4j.logger.software.amazon.awssdk.request=DEBUG
```

If using the "unshaded" JAR, then the Apache HttpClient can be directly configured:

```properties
log4j.logger.org.apache.http=DEBUG
```


This produces a log such as this, which is for a V4-authenticated PUT of a 0-byte file used
as an empty directory marker

```
execchain.MainClientExec (MainClientExec.java:execute(255)) - Executing request PUT /test/ HTTP/1.1
execchain.MainClientExec (MainClientExec.java:execute(266)) - Proxy auth state: UNCHALLENGED
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(135)) - http-outgoing-0 >> PUT /test/ HTTP/1.1
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Host: ireland-new.s3-eu-west-1.amazonaws.com
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> x-amz-content-sha256: UNSIGNED-PAYLOAD
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Authorization: AWS4-HMAC-SHA256 Credential=AKIAIYZ5JEEEER/20170904/eu-west-1/s3/aws4_request,  ...
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> X-Amz-Date: 20170904T172929Z
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> User-Agent: Hadoop 3.0.0-beta-1, aws-sdk-java/1.11.134 ...
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> amz-sdk-invocation-id: 75b530f8-ad31-1ad3-13db-9bd53666b30d
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> amz-sdk-retry: 0/0/500
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Content-Type: application/octet-stream
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Content-Length: 0
http.headers (LoggingManagedHttpClientConnection.java:onRequestSubmitted(138)) - http-outgoing-0 >> Connection: Keep-Alive
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "PUT /test/ HTTP/1.1[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Host: ireland-new.s3-eu-west-1.amazonaws.com[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "x-amz-content-sha256: UNSIGNED-PAYLOAD[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Authorization: AWS4-HMAC-SHA256 Credential=AKIAIYZ5JEEEER/20170904/eu-west-1/s3/aws4_request, ,,,
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "X-Amz-Date: 20170904T172929Z[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "User-Agent: 3.0.0-beta-1, aws-sdk-java/1.11.134  ...
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "amz-sdk-invocation-id: 75b530f8-ad31-1ad3-13db-9bd53666b30d[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "amz-sdk-retry: 0/0/500[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Content-Type: application/octet-stream[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Content-Length: 0[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "Connection: Keep-Alive[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 >> "[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "HTTP/1.1 200 OK[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "x-amz-id-2: mad9GqKztzlL0cdnCKAj9GJOAs+DUjbSC5jRkO7W1E7Nk2BUmFvt81bhSNPGdZmyyKqQI9i/B/A=[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "x-amz-request-id: C953D2FE4ABF5C51[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Date: Mon, 04 Sep 2017 17:29:30 GMT[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "ETag: "d41d8cd98f00b204e9800998ecf8427e"[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Content-Length: 0[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "Server: AmazonS3[\r][\n]"
http.wire (Wire.java:wire(72)) - http-outgoing-0 << "[\r][\n]"
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(124)) - http-outgoing-0 << HTTP/1.1 200 OK
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << x-amz-id-2: mad9GqKztzlL0cdnCKAj9GJOAs+DUjbSC5jRkO7W1E7Nk2BUmFvt81bhSNPGdZmyyKqQI9i/B/A=
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << x-amz-request-id: C953D2FE4ABF5C51
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Date: Mon, 04 Sep 2017 17:29:30 GMT
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << ETag: "d41d8cd98f00b204e9800998ecf8427e"
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Content-Length: 0
http.headers (LoggingManagedHttpClientConnection.java:onResponseReceived(127)) - http-outgoing-0 << Server: AmazonS3
execchain.MainClientExec (MainClientExec.java:execute(284)) - Connection can be kept alive for 60000 MILLISECONDS

```

### <a name="audit-logging"></a> Enable S3 Server-side Logging

The [Auditing](auditing) feature of the S3A connector can be used to generate
S3 Server Logs with information which can be used to debug problems
working with S3, such as throttling events.

Consult the [auditing documentation](auditing) documentation.
As auditing is enabled by default, enabling S3 Logging for a bucket
should be sufficient to collect these logs.

### <a name="retries"></a>  Reducing failures by configuring retry policy

The S3A client can ba configured to retry those operations which are considered
retryable. That can be because they are idempotent, or
because the failure happened before the request was processed by S3.

The number of retries and interval between each retry can be configured:

```xml
<property>
  <name>fs.s3a.retry.limit</name>
  <value>7</value>
  <description>
    Number of times to retry any repeatable S3 client request on failure,
    excluding throttling requests.
  </description>
</property>

<property>
  <name>fs.s3a.retry.interval</name>
  <value>500ms</value>
  <description>
    Initial retry interval when retrying operations for any reason other
    than S3 throttle errors.
  </description>
</property>
```

Not all failures are retried *in the S3A Code* -though some may be retried within the
AWS SDK.
Specifically excluded are those considered unrecoverable:

* Low-level networking: `UnknownHostException`, `NoRouteToHostException`.
* 302 redirects.
* Missing resources, 404/`FileNotFoundException`.
* HTTP 416 response/`EOFException`. This can surface if the length of a file changes
  while another client is reading it.
* Failures during execution or result processing of non-idempotent operations where
it is considered likely that the operation has already taken place.

In future, others may be added to this list.

When one of these failures arises in the S3/S3A client, the retry mechanism
is bypassed and the operation will fail.

*Warning*: the S3A client considers DELETE, PUT and COPY operations to
be idempotent, and will retry them on failure. These are only really idempotent
if no other client is attempting to manipulate the same objects, such as:
renaming() the directory tree or uploading files to the same location.
Please don't do that. Given that the emulated directory rename and delete operations
are not atomic, even without retries, multiple S3 clients working with the same
paths can interfere with each other

### <a name="aws-timeouts"></a> Tuning AWS request timeouts

It is possible to configure a global timeout for AWS service calls using following property:

```xml
<property>
  <name>fs.s3a.connection.request.timeout</name>
  <value>0</value>
  <description>
    Time out on HTTP requests to the AWS service; 0 means no timeout.
    Measured in seconds; the usual time suffixes are all supported

    Important: this is the maximum duration of any AWS service call,
    including upload and copy operations. If non-zero, it must be larger
    than the time to upload multi-megabyte blocks to S3 from the client,
    and to rename many-GB files. Use with care.

    Values that are larger than Integer.MAX_VALUE milliseconds are
    converged to Integer.MAX_VALUE milliseconds
  </description>
</property>
```

If this value is configured too low, user may encounter `SdkClientException`s due to many requests
timing-out.

```
software.amazon.awssdk.core.exception.SdkClientException: Unable to execute HTTP request:
  Request did not complete before the request timeout configuration.:
  Unable to execute HTTP request: Request did not complete before the request timeout configuration.
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:205)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:112)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:315)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:407)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:311)
```

When this happens, try to set `fs.s3a.connection.request.timeout` to a larger value or disable it
completely by setting it to `0`.
