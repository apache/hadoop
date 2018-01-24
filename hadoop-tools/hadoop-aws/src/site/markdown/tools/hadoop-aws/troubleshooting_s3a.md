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

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

##<a name="introduction"></a> Introduction

Common problems working with S3 are

1. Classpath setup
1. Authentication
1. S3 Inconsistency side-effects

Classpath is usually the first problem. For the S3x filesystem clients,
you need the Hadoop-specific filesystem clients, third party S3 client libraries
compatible with the Hadoop code, and any dependent libraries compatible with
Hadoop and the specific JVM.

The classpath must be set up for the process talking to S3: if this is code
running in the Hadoop cluster, the JARs must be on that classpath. That
includes `distcp` and the `hadoop fs` command.

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Troubleshooting IAM Assumed Roles is covered in its
[specific documentation](assumed_roles.html#troubeshooting).

## <a name="classpath"></a> Classpath Setup

Note that for security reasons, the S3A client does not provide much detail
on the authentication process (i.e. the secrets used to authenticate).

### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`

These is Hadoop filesytem client classes, found in the `hadoop-aws` JAR.
An exception reporting this class as missing means that this JAR is not on
the classpath.

### `ClassNotFoundException: com.amazonaws.services.s3.AmazonS3Client`

(or other `com.amazonaws` class.)

This means that the `aws-java-sdk-bundle.jar` JAR is not on the classpath:
add it.

### Missing method in `com.amazonaws` class

This can be triggered by incompatibilities between the AWS SDK on the classpath
and the version which Hadoop was compiled with.

The AWS SDK JARs change their signature enough between releases that the only
way to safely update the AWS SDK version is to recompile Hadoop against the later
version.

The sole fix is to use the same version of the AWS SDK with which Hadoop
was built.


## <a name="authentication"></a> Authentication Failure

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

1. If using environement variable-based authentication, make sure that the
relevant variables are set in the environment in which the process is running.

The standard first step is: try to use the AWS command line tools with the same
credentials, through a command such as:

    hadoop fs -ls s3a://my-bucket/

Note the trailing "/" here; without that the shell thinks you are trying to list
your home directory under the bucket, which will only exist if explicitly created.


Attempting to list a bucket using inline credentials is a
means of verifying that the key and secret can access a bucket;

    hadoop fs -ls s3a://key:secret@my-bucket/

Do escape any `+` or `/` symbols in the secret, as discussed below, and never
share the URL, logs generated using it, or use such an inline authentication
mechanism in production.

Finally, if you set the environment variables, you can take advantage of S3A's
support of environment-variable authentication by attempting the same ls operation.
That is: unset the `fs.s3a` secrets and rely on the environment variables.

### Authentication failure due to clock skew

The timestamp is used in signing to S3, so as to
defend against replay attacks. If the system clock is too far behind *or ahead*
of Amazon's, requests will be rejected.

This can surface as the situation where
read requests are allowed, but operations which write to the bucket are denied.

Check the system clock.

### Authentication failure when using URLs with embedded secrets

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

### <a name="bad_request"></a> "Bad Request" exception when working with AWS S3 Frankfurt, Seoul, or other "V4" endpoint


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
"V4" signing API —but the client is configured to use the default S3 service
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


### "403 Access denied" when trying to write data

Data can be read, but attempts to write data or manipulate the store fail with
403/Access denied.

The bucket may have an access policy which the request does not comply with.

```
java.nio.file.AccessDeniedException: test/: PUT 0-byte object  on test/:
 com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403;
 Error Code: AccessDenied; Request ID: EDC662AD2EEEA33C;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:210)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:110)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:259)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:313)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:255)
  at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:230)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.createEmptyObject(S3AFileSystem.java:2691)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.createFakeDirectory(S3AFileSystem.java:2666)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerMkdirs(S3AFileSystem.java:2030)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.mkdirs(S3AFileSystem.java:1965)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2305)
```

In the AWS S3 management console, select the "permissions" tab for the bucket, then "bucket policy".
If there is no bucket policy, then the error cannot be caused by one.

If there is a bucket access policy, e.g. required encryption headers,
then the settings of the s3a client must guarantee the relevant headers are set
(e.g. the encryption options match).
Note: S3 Default Encryption options are not considered here:
if the bucket policy requires AES256 as the encryption policy on PUT requests,
then the encryption option must be set in the s3a client so that the header is set.

## <a name="connectivity"></a> Connectivity Problems

### <a name="bad_endpoint"></a> Error message "The bucket you are attempting to access must be addressed using the specified endpoint"

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

Using the explicit endpoint for the region is recommended for speed and
to use the V4 signing API.


### <a name="timeout"></a> "Timeout waiting for connection from pool" when writing data

This happens when using the output stream thread pool runs out of capacity.

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

### "Timeout waiting for connection from pool" when reading data

This happens when more threads are trying to read from an S3A system than
the maximum number of allocated HTTP connections.

Set `fs.s3a.connection.maximum` to a larger value (and at least as large as
`fs.s3a.threads.max`)

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
  com.amazonaws.AmazonClientException: Failed to sanitize XML document
  destined for handler class com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser$ListBucketHandler:
  Failed to sanitize XML document destined for handler class com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser$ListBucketHandler
    at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:105)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.getFileStatus(S3AFileSystem.java:1462)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.innerListStatus(S3AFileSystem.java:1227)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.listStatus(S3AFileSystem.java:1203)
    at org.apache.hadoop.fs.s3a.S3AGlobber.listStatus(S3AGlobber.java:69)
    at org.apache.hadoop.fs.s3a.S3AGlobber.doGlob(S3AGlobber.java:210)
    at org.apache.hadoop.fs.s3a.S3AGlobber.glob(S3AGlobber.java:125)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.globStatus(S3AFileSystem.java:1853)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.globStatus(S3AFileSystem.java:1841)
```

We believe this is caused by the connection to S3 being broken.
See [HADOOP-13811](https://issues.apache.org/jira/browse/HADOOP-13811).

It may go away if the operation is retried.

### JSON Parse Error from AWS SDK

Sometimes a JSON Parse error is reported with the stack trace in the `com.amazonaws`,

Again, we believe this is caused by the connection to S3 being broken.

It may go away if the operation is retried.



## Miscellaneous Errors

### When writing data: "java.io.FileNotFoundException: Completing multi-part upload"


A multipart upload was trying to complete, but failed as there was no upload
with that ID.
```
java.io.FileNotFoundException: Completing multi-part upload on fork-5/test/multipart/1c397ca6-9dfb-4ac1-9cf7-db666673246b:
 com.amazonaws.services.s3.model.AmazonS3Exception: The specified upload does not exist.
  The upload ID may be invalid, or the upload may have been aborted or completed. (Service: Amazon S3; Status Code: 404;
   Error Code: NoSuchUpload;
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

This can happen when all outstanding uploads have been aborted, including
the active ones.

If the bucket has a lifecycle policy of deleting multipart uploads, make
sure that the expiry time of the deletion is greater than that required
for all open writes to complete the write,
*and for all jobs using the S3A committers to commit their work.*


### Application hangs after reading a number of files




The pool of https client connectons and/or IO threads have been used up,
and none are being freed.


1. The pools aren't big enough. Increas `fs.s3a.connection.maximum` for
the http connections, and `fs.s3a.threads.max` for the thread pool.
2. Likely root cause: whatever code is reading files isn't calling `close()`
on the input streams. Make sure your code does this!
And if it's someone else's: make sure you have a recent version; search their
issue trackers to see if its a known/fixed problem.
If not, it's time to work with the developers, or come up with a workaround
(i.e closing the input stream yourself).

### "Timeout waiting for connection from pool"

This the same problem as above, exhibiting itself as the http connection
pool determining that it has run out of capacity.

```

java.io.InterruptedIOException: getFileStatus on s3a://example/fork-0007/test:
 com.amazonaws.SdkClientException: Unable to execute HTTP request: Timeout waiting for connection from pool
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:145)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:119)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.s3GetFileStatus(S3AFileSystem.java:2040)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.checkPathForDirectory(S3AFileSystem.java:1857)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerMkdirs(S3AFileSystem.java:1890)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.mkdirs(S3AFileSystem.java:1826)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2230)
  ...
Caused by: com.amazonaws.SdkClientException: Unable to execute HTTP request: Timeout waiting for connection from pool
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleRetryableException(AmazonHttpClient.java:1069)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1035)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:742)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:716)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4221)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4168)
  at com.amazonaws.services.s3.AmazonS3Client.getObjectMetadata(AmazonS3Client.java:1249)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.getObjectMetadata(S3AFileSystem.java:1162)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.s3GetFileStatus(S3AFileSystem.java:2022)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.checkPathForDirectory(S3AFileSystem.java:1857)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerMkdirs(S3AFileSystem.java:1890)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.mkdirs(S3AFileSystem.java:1826)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2230)
...
Caused by: com.amazonaws.thirdparty.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool
  at com.amazonaws.thirdparty.apache.http.impl.conn.PoolingHttpClientConnectionManager.leaseConnection(PoolingHttpClientConnectionManager.java:286)
  at com.amazonaws.thirdparty.apache.http.impl.conn.PoolingHttpClientConnectionManager$1.get(PoolingHttpClientConnectionManager.java:263)
  at sun.reflect.GeneratedMethodAccessor10.invoke(Unknown Source)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at com.amazonaws.http.conn.ClientConnectionRequestFactory$Handler.invoke(ClientConnectionRequestFactory.java:70)
  at com.amazonaws.http.conn.$Proxy15.get(Unknown Source)
  at com.amazonaws.thirdparty.apache.http.impl.execchain.MainClientExec.execute(MainClientExec.java:190)
  at com.amazonaws.thirdparty.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:184)
  at com.amazonaws.thirdparty.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:184)
  at com.amazonaws.thirdparty.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:82)
  at com.amazonaws.thirdparty.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:55)
  at com.amazonaws.http.apache.client.impl.SdkHttpClient.execute(SdkHttpClient.java:72)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1190)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1030)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:742)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:716)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4221)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4168)
  at com.amazonaws.services.s3.AmazonS3Client.getObjectMetadata(AmazonS3Client.java:1249)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.getObjectMetadata(S3AFileSystem.java:1162)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.s3GetFileStatus(S3AFileSystem.java:2022)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.checkPathForDirectory(S3AFileSystem.java:1857)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerMkdirs(S3AFileSystem.java:1890)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.mkdirs(S3AFileSystem.java:1826)
  at org.apache.hadoop.fs.FileSystem.mkdirs(FileSystem.java:2230)
```

This is the same problem as the previous one, exhibited differently.

### Issue: when writing data, HTTP Exceptions logged at info from `AmazonHttpClient`

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

## File System Semantics

These are the issues where S3 does not appear to behave the way a filesystem
"should".

### Visible S3 Inconsistency

Amazon S3 is *an eventually consistent object store*. That is: not a filesystem.

To reduce visible inconsistencies, use the [S3Guard](./s3guard.html) consistency
cache.


By default, Amazon S3 offers read-after-create consistency: a newly created file
is immediately visible.
There is a small quirk: a negative GET may be cached, such
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

That means the `getFileStatus()` and `open()` operations are more likely
to be consistent with the state of the object store, but without S3Guard enabled,
directory list operations such as `listStatus()`, `listFiles()`, `listLocatedStatus()`,
and `listStatusIterator()` may not see newly created files, and still list
old files.

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

Fix: Use S3Guard


### File not visible/saved

The files in an object store are not visible until the write has been completed.
In-progress writes are simply saved to a local file/cached in RAM and only uploaded.
at the end of a write operation. If a process terminated unexpectedly, or failed
to call the `close()` method on an output stream, the pending data will have
been lost.

### File `flush()`, `hsync` and `hflush()` calls do not save data to S3

Again, this is due to the fact that the data is cached locally until the
`close()` operation. The S3A filesystem cannot be used as a store of data
if it is required that the data is persisted durably after every
`Syncable.hflush()` or `Syncable.hsync()` call.
This includes resilient logging, HBase-style journalling
and the like. The standard strategy here is to save to HDFS and then copy to S3.

## <a name="encryption"></a> S3 Server Side Encryption

### Using SSE-KMS "Invalid arn"

When performing file operations, the user may run into an issue where the KMS
key arn is invalid.
```
com.amazonaws.services.s3.model.AmazonS3Exception:
Invalid arn (Service: Amazon S3; Status Code: 400; Error Code: KMS.NotFoundException; Request ID: 708284CF60EE233F),
S3 Extended Request ID: iHUUtXUSiNz4kv3Bdk/hf9F+wjPt8GIVvBHx/HEfCBYkn7W6zmpvbA3XT7Y5nTzcZtfuhcqDunw=:
Invalid arn (Service: Amazon S3; Status Code: 400; Error Code: KMS.NotFoundException; Request ID: 708284CF60EE233F)
```

This is due to either, the KMS key id is entered incorrectly, or the KMS key id
is in a different region than the S3 bucket being used.

### Using SSE-C "Bad Request"

When performing file operations the user may run into an unexpected 400/403
error such as
```
org.apache.hadoop.fs.s3a.AWSS3IOException: getFileStatus on fork-4/: com.amazonaws.services.s3.model.AmazonS3Exception:
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

## <a name="performance"></a> Performance

S3 is slower to read data than HDFS, even on virtual clusters running on
Amazon EC2.

* HDFS replicates data for faster query performance.
* HDFS stores the data on the local hard disks, avoiding network traffic
 if the code can be executed on that host. As EC2 hosts often have their
 network bandwidth throttled, this can make a tangible difference.
* HDFS is significantly faster for many "metadata" operations: listing
the contents of a directory, calling `getFileStatus()` on path,
creating or deleting directories. (S3Guard reduces but does not eliminate
the speed gap).
* On HDFS, Directory renames and deletes are `O(1)` operations. On
S3 renaming is a very expensive `O(data)` operation which may fail partway through
in which case the final state depends on where the copy+ delete sequence was when it failed.
All the objects are copied, then the original set of objects are deleted, so
a failure should not lose data —it may result in duplicate datasets.
* Unless fast upload enabled, the write only begins on a `close()` operation.
This can take so long that some applications can actually time out.
* File IO involving many seek calls/positioned read calls will encounter
performance problems due to the size of the HTTP requests made. Enable the
"random" fadvise policy to alleviate this at the
expense of sequential read performance and bandwidth.

The slow performance of `rename()` surfaces during the commit phase of work,
including

* The MapReduce `FileOutputCommitter`. This also used by Apache Spark.
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

## <a name="network_performance"></a>Troubleshooting network performance

An example of this is covered in [HADOOP-13871](https://issues.apache.org/jira/browse/HADOOP-13871).

1. For public data, use `curl`:

        curl -O https://landsat-pds.s3.amazonaws.com/scene_list.gz
1. Use `nettop` to monitor a processes connections.

Consider reducing the connection timeout of the s3a connection.

```xml
<property>
  <name>fs.s3a.connection.timeout</name>
  <value>15000</value>
</property>
```
This *may* cause the client to react faster to network pauses, so display
stack traces fast. At the same time, it may be less resilient to
connectivity problems.


## Other Issues

### <a name="logging"></a> Enabling low-level logging

The AWS SDK and the Apache S3 components can be configured to log at
more detail, as can S3A itself.

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
log4j.logger.com.amazonaws.request=DEBUG
log4j.logger.com.amazonaws.thirdparty.apache.http=DEBUG
```

If using the "unshaded" JAR, then the Apache HttpClient can be directly configured:

```properties
log4j.logger.org.apache.http=DEBUG
```


This produces a log such as this, wich is for a V4-authenticated PUT of a 0-byte file used
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


## <a name="retries"></a>  Reducing failures by configuring retry policy

The S3A client can ba configured to rety those operations which are considered
retriable. That can be because they are idempotent, or
because there failure happened before the request was processed by S3.

The number of retries and interval between each retry can be configured:

```xml
<property>
  <name>fs.s3a.attempts.maximum</name>
  <value>20</value>
  <description>How many times we should retry commands on transient errors,
  excluding throttling errors.</description>
</property>

<property>
  <name>fs.s3a.retry.interval</name>
  <value>500ms</value>
  <description>
    Interval between retry attempts.
  </description>
</property>
```

Not all failures are retried. Specifically excluded are those considered
unrecoverable:

* Low-level networking: `UnknownHostException`, `NoRouteToHostException`.
* 302 redirects
* Missing resources, 404/`FileNotFoundException`
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
aren't atomic, even without retries, multiple S3 clients working with the same
paths can interfere with each other

#### <a name="retries"></a> Throttling

When many requests are made of a specific S3 bucket (or shard inside it),
S3 will respond with a 503 "throttled" response.
Throttling can be recovered from, provided overall load decreases.
Furthermore, because it is sent before any changes are made to the object store,
is inherently idempotent. For this reason, the client will always attempt to
retry throttled requests.

The limit of the number of times a throttled request can be retried,
and the exponential interval increase between attempts, can be configured
independently of the other retry limits.

```xml
<property>
  <name>fs.s3a.retry.throttle.limit</name>
  <value>20</value>
  <description>
    Number of times to retry any throttled request.
  </description>
</property>

<property>
  <name>fs.s3a.retry.throttle.interval</name>
  <value>500ms</value>
  <description>
    Interval between retry attempts on throttled requests.
  </description>
</property>
```

If a client is failing due to `AWSServiceThrottledException` failures,
increasing the interval and limit *may* address this. However, it
it is a sign of AWS services being overloaded by the sheer number of clients
and rate of requests. Spreading data across different buckets, and/or using
a more balanced directory structure may be beneficial.
Consult [the AWS documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html).

Reading or writing data encrypted with SSE-KMS forces S3 to make calls of
the AWS KMS Key Management Service, which comes with its own
[Request Rate Limits](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).
These default to 1200/second for an account, across all keys and all uses of
them, which, for S3 means: across all buckets with data encrypted with SSE-KMS.

###### Tips to Keep Throttling down

* If you are seeing a lot of throttling responses on a large scale
operation like a `distcp` copy, *reduce* the number of processes trying
to work with the bucket (for distcp: reduce the number of mappers with the
`-m` option).

* If you are reading or writing lists of files, if you can randomize
the list so they are not processed in a simple sorted order, you may
reduce load on a specific shard of S3 data, so potentially increase throughput.

* An S3 Bucket is throttled by requests coming from all
simultaneous clients. Different applications and jobs may interfere with
each other: consider that when troubleshooting.
Partitioning data into different buckets may help isolate load here.

* If you are using data encrypted with SSE-KMS, then the
will also apply: these are stricter than the S3 numbers.
If you believe that you are reaching these limits, you may be able to
get them increased.
Consult [the KMS Rate Limit documentation](http://docs.aws.amazon.com/kms/latest/developerguide/limits.html).

* S3Guard uses DynamoDB for directory and file lookups;
it is rate limited to the amount of (guaranteed) IO purchased for a
table. If significant throttling events/rate is observed here, the preallocated
IOPs can be increased with the `s3guard set-capacity` command, or
through the AWS Console. Throttling events in S3Guard are noted in logs, and
also in the S3A metrics `s3guard_metadatastore_throttle_rate` and
`s3guard_metadatastore_throttled`.
