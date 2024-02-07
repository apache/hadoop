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

# Upgrade S3A to AWS SDK V2: Changelog

Note: This document is not meant to be committed as part of the final merge, and instead just serves
as a guide to help with reviewing the PR.

This document tracks changes to S3A during the upgrade to AWS SDK V2. Once the upgrade
is complete, some of its content will be added to the existing document
[Upcoming upgrade to AWS Java SDK V2](./aws_sdk_upgrade.html).

This work is tracked in [HADOOP-18073](https://issues.apache.org/jira/browse/HADOOP-18073).

## Contents

* [Client Configuration](#client-configuration)
* [Endpoint and region configuration](#endpoint-and-region-configuration)
* [List Object](#list-object)
* [EncryptionSecretOperations](#encryptionsecretoperations)
* [GetObjectMetadata](#getobjectmetadata)
* [PutObject](#putobject)
* [CopyObject](#copyobject)
* [MultipartUpload](#multipartupload)
* [GetObject](#getObject)
* [DeleteObject](#deleteobject)
* [Select](#select)
* [CredentialsProvider](#credentialsprovider)
* [Auditing](#auditing)
* [Metric Collection](#metric-collection)
* [Exception Handling](#exception-handling)
* [Failure Injection](#failure-injection)

### Client Configuration:

* We now have two clients, a sync S3 Client and an async S3 Client. The async s3 client is required
  as the select operation is currently only supported on the async client. Once we are confident in
  the current set of changes, we will also be exploring moving other operations to the async client
  as this could provide potential performance benefits. However those changes are not in the scope
  of this PR, and will be done separately.
* The [createAwsConf](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AUtils.java#L1190)
method is now split into:
   ```
   createClientConfigBuilder // sets request timeout, user agent*
   createHttpClientBuilder* // sets max connections, connection timeout, socket timeout
   createProxyConfigurationBuilder // sets proxy config, defined in table below
   ```

The table below lists the configurations S3A was using and what they now map to.

|SDK V1    |SDK V2    |
|---|---|
|setMaxConnections    |httpClientBuilder.maxConnections    |
|setProtocol    |The protocol is now HTTPS by default, and can only be modified by setting an HTTP endpoint on the client builder. This is done when setting the endpoint in getS3Endpoint()    |
|setMaxErrorRetry    |createRetryPolicyBuilder    |
|setConnectionTimeout    |httpClientBuilder.connectionTimeout    |
|setSocketTimeout    |httpClientBuilder.socketTimeout    |
|setRequestTimeout    |overrideConfigBuilder.apiCallAttemptTimeout    |
|setSocketBufferSizeHints    |Not supported    |
|setSignerOverride    |Not done yet    |
|setProxyHost    |proxyConfigBuilder.endpoint    |
|setProxyPort    |set when setting proxy host with .endpoint    |
|setProxyUsername    |proxyConfigBuilder.username    |
|setProxyPassword    |proxyConfigBuilder.password    |
|setProxyDomain    |proxyConfigBuilder.ntlmDomain, not supported in async client    |
|setProxyWorkstation    |proxyConfigBuilder.ntlmWorkstation, not supported in async client    |
|setUserAgentPrefix    |overrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent);    |
|addHeader    |overrideConfigBuilder.putHeader    |
|setUseThrottleRetries    |not supported    |

### Endpoint and region configuration

Previously, if no endpoint and region was configured, fall back to using us-east-1. Set
withForceGlobalBucketAccessEnabled(true) which will allow access to buckets not in this region too.
Since the SDK V2 no longer supports cross region access, we need to set the region and
endpoint of the bucket. The behaviour has now been changed to:

* If no endpoint is specified, use s3.amazonaws.com.
* When setting the endpoint, also set the protocol (HTTP or HTTPS)
* When setting the region, first initiate a default S3 Client with region eu-west-2. Call headBucket
  using this client. If the bucket is also in eu-west-2, then this will return a successful
  response. Otherwise it will throw an error with status code 301 permanently moved. This error
  contains the region of the bucket in its header, which we can then use to configure the client.

### List Object:

There is no way to paginate the listObject V1 result, we are
doing [this](https://github.com/ahmarsuhail/hadoop/pull/23/files#diff-4050f95b7e3912145415b6e2f9cd3b0760fcf2ce96bf0980c6c30a6edad2d0fbR2745)
instead. We are trying to get pagination to listObject V1 in the SDK, but will have to use this
workaround for now.

### EncryptionSecretOperations:

Two new methods have been added, `getSSECustomerKey` and `getSSEAwsKMSKey`. Previously SDK V1 had
specific classes for these keys `SSECustomerKey` and `SSEAwsKeyManagementParams` . There are no such
classes with V2, and things need to be set manually. For this reason, we simply just return keys as
strings now. And will have to calculate and set md5’s ourselves when building the request.


### GetObjectMetadata:

* `RequestFactory.newGetObjectMetadataRequest` is now `RequestFactory.newHeadObjectRequestBuilder`.
* In `HeaderProcessing.retrieveHeaders()`, called by `getXAttrs()`,
  removed `maybeSetHeader(headers, XA_CONTENT_MD5, md.getContentMD5())` as S3 doesn’t ever actually
  return an md5 header, regardless of whether you set it during a putObject. It does return
  an `etag` which may or may not be an md5 depending on certain conditions. `getContentMD5()` is
  always empty, there does not seem to be a need to set this header.
* `RequestFactoryImpl.setOptionalGetObjectMetadataParameters` : Method has been removed and this
  logic has been moved to `RequestFactoryImpl.newHeadObjectRequestBuilder()`
* `RequestFactoryImpl.generateSSECustomerKey()` has been removed, and instead
  call `EncryptionSecretOperations.createSSECustomerKey` directly in `newHeadObjectRequestBuilder()`



### PutObject

* Previously, when creating the `putObjectRequest`, you would also give it the data to be uploaded.
  So it would be of the form `PutObjectRequest(bucket, key, file/inputstream)`, this is no longer
  the case. Instead, the data now needs to be passed in while making the `s3Client.putObject()`
  call. For this reason, the data is now part of
  the `S3AFileSystem.putObject(putObjectRequest, file, listener)`
  and `S3AFileSystem.putObjectDirect(putObjectRequest, putOptions, uploadData, isFile)`.
* `S3ADataBlocks`: Need to make this class public as it’s now used to pass in data
  to `putObjectDirect()`, sometimes from outside the package (`MagicCommitTracker`
  , `ITestS3AMiscOperations`).
* `ProgressableProgressListener`: You can no longer pass in the `Upload` while initialising the
  listener
  as `ProgressableProgressListener listener = new ProgressableProgressListener(this, key, upload, progress);`
  The upload is now only available after initialising the listener, since the listener needs to be
  initialised during creation of the Transfer Manager upload. Previously, you could create the
  listener after the starting the TM upload, and attach it.
* The `Upload` is now passed into the progress listener later,
  in `listener.uploadCompleted(uploadInfo.getFileUpload());`.
* `UploadInfo`: Previously, since the data to be uploaded was part of `putObjectRequest`, the
  transfer manager only returned a single `Upload` type, which could be used to track the upload.
  Now, depending on the upload type (eg: File or InputStream), it returns different types. This
  class has been updated to return FileUpload info, as it’s only ever used for file uploads
  currently. It can be extended to store different transfer types in the future.
* `WriteOperationHelper.createPutObjectRequest() `: Previously the data to be uploaded was part
  of `PutObjectRequest`, and so we required two methods to create the request. One for input streams
  and one for files. Since the data to be uploaded is no longer part of the request, but instead an
  argument in `putObject` , we only need one method now.
* `WriteOperationHelper.newObjectMetadata()`: This method has been removed, as standard metadata,
  instead of being part of the `ObjectMetadata`, is now just added while building the request, for
  example `putObjectRequestBuilder.serverSideEncryption().`
* `RequestFactory`: Similar to WriteOperationHelper, there is now a single putObjectRequest,
  and `newObjectMetadata` has been removed. Instead, all standard metadata is now set in the new
  method `buildPutObjectRequest`.
* `RequestFactoryImpl.newObjectMetadata()`:  Previously, object metadata was created
  using `newObjectMetadata()` and passed into the `newPutObjectRequest()` call. This method has been
  removed, as standard metadata, instead of being part of the `ObjectMetadata`, is now just added
  while building the request, in `putObjectRequestBuilder.serverSideEncryption().`  Content length
  and content encoding set in this method is now set in `buildPutObjectRequest()` , and SSE is set
  in `putEncryptionParameters()`.
* `RequestFactoryImpl.maybeSetMetadata()` : was a generic method to set user metadata on object
  metadata. user metadata now gets set on the request builder, so method has been removed.
* `RequestFactoryImpl.setOptionalPutRequestParameters()` : Method has been removed, and this logic
  has been moved to `putEncryptionParameters()` .

### CopyObject

* `RequestFactoryImpl.buildPutObjectRequest` : Destination metadata is no longer built
  using `newObjectMetadata()` and instead set on the request builder. The logic has a couple of
  differences:
  * content encoding is set in `buildCopyObjectRequest`,
    the `if (contentEncoding != null && !isDirectoryMarker)` can just
    be `if (contentEncoding != null)` for copy, as for this `isDirectoryMarker` was always false.
  * contentLength is not set, as this is a system defined header, and copied over automatically by
    S3 during copy.
* `HeaderProcessing.cloneObjectMetadata`: This was previously also setting a lot of system defined
  metadata, eg: `setHttpExpiresDate` and `setLastModified`. These have been removed as they are set
  by S3 during the copy. Have tested, and can see they are set automatically regardless of the
  metadataDirective (copy or replace).
* `RequestFactoryImpl. copyEncryptionParameters()` : Due to the changes
  in `EncryptionSecretOperations`, source and destination encryption params have to be set manually.

### MultipartUpload

* `RequestFactoryImpl.newObjectMetdata()` : Metadata is now set on the request builder. For MPU, only
content encoding needs to be set, as per per previous behaviour. Encryption params are set
in ` multipartUploadEncryptionParameters`.

### GetObject

* Previously, GetObject returned a `S3Object` response which exposed its content in a
  `S3ObjectInputStream` through the `getObjectContent()` method. In SDK v2, the response is
  directly a `ResponseInputStream<GetObjectResponse>` with the content, while the
  `GetObjectResponse` instance can be retrieved by calling `response()` on it.
* The above change simplifies managing the lifetime of the response input stream. In v1,
  `S3AInputStream` had to keep a reference to the `S3Object` while holding the wrapped
  `S3ObjectInputStream`. When upgraded to SDK v2, it can simply wrap the new
  `ResponseInputStream<GetObjectResponse>`, which handles lifetime correctly. Same applies
  to `SDKStreamDrainer`. Furthermore, the map in `S3ARemoteObject` associating input streams and
  `S3Object` instances is no longer needed.
* The range header on a `GetObject` request is now specified as a string, rather than a
  `start`-`end` pair. `S3AUtils.formatRange` was introduced to format it.

### DeleteObject

In SDK v1, bulk delete would throw a `com.amazonaws.services.s3.model.MultiObjectDeleteException`
in case of partial failure. In v2, instead, it returns a `DeleteObjectsResponse` containing a
list of errors. A new `MultiObjectDeleteException` class was introduced in
`org.apache.hadoop.fs.s3a` and is thrown when appropriate to reproduce the previous behaviour.
* `MultiObjectDeleteSupport.translateDeleteException` was moved into `MultiObjectDeleteException`.
* `ObjectIdentifier` replaces DeleteObjectsRequest.KeyVersion.

### Select

In SDK v2, Handling of select requests has changes significantly since SelectObjectContent is
only supported on the new async S3 client. In previous versions, the response to a
SelectObjectContent request exposed the results in a `SelectRecordsInputStream`, which S3A
could wrap in `SelectInputStream`. In v2, instead, the response needs to be handled by an object
implementing `SelectObjectContentResponseHandler`, which can receive an async publisher of
the "events" returned by the service (`SdkPublisher<SelectObjectContentEventStream>`).

In order to adapt the new API in S3A, three new classes have been introduced in
`org.apache.hadoop.fs.s3a.select`:

* `SelectObjectContentHelper`: wraps the `selectObjectContent()` call, provides a custom
  response handler to receive the response, and exposes a `SelectEventStreamPublisher`.
* `SelectEventStreamPublisher`: a publisher of select event stream events, which handles the
  future returned by the select call and wraps the original publisher. This class provides
  a `toRecordsInputStream()` method which returns an input stream containing the results,
  reproducing the behaviour of the old `SelectRecordsInputStream`.
* `BlockingEnumeration`: an adapter which lazily requests new elements from the publisher and
  exposes them through an `Enumeration` interface. Used in
  `SelectEventStreamPublisher.toRecordsInputStream()` to adapt the event publisher into
  an enumeration of input streams, eventually passed to a `SequenceInputStream`.
  Note that the "lazy" behaviour means that new elements are requested only on `read()` calls on
  the input stream.



### CredentialsProvider

* All credential provider classes implemented in Hadoop now implement V2's `AwsCredentialProvider`
* New adapter class `org.apache.hadoop.fs.s3a.adapter.V1ToV2AwsCredentialProviderAdapter` has been
  added. This converts SDK V1 credential providers to SDK V2’s which
  implement `AwsCredentialsProvider`.
* `AWSCredentialProviderList` also implements `AwsCredentialProvider`. But keeps existing
  constructors and add methods for V1 credential providers, and wraps V1 cred providers in the
  adapter here. This means that custom binding classes in delegation tokens, as well as any custom
  credential providers will continue to work.
* Added a new `getCredentials()` method in `AWSCredentialProviderList`, which ensured that custom
  binding classes which are calling `AWSCredentialProviderList.getCredentials()`, continue to work.
* The following values `fs.s3a.aws.credentials.provider` are mapped:
  as `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`, then map it to V2’s

|`fs.s3a.aws.credentials.provider` value    |Mapped to    |
|---|---|
|`com.amazonaws.auth.EnvironmentVariableCredentialsProvider`    |`software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider`    |
|`com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper`    |`org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`    |
|`com.amazonaws.auth.`InstanceProfileCredentialsProvider``    |`org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider`    |


### Auditing

The SDK v2 offers a new `ExecutionInterceptor`
[interface](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html)
which broadly replaces the `RequestHandler2` abstract class from v1.
Switching to the new mechanism in S3A brings:

* Simplification in `AWSAuditEventCallbacks` (and implementors) which can now extend
  `ExecutionInterceptor`
* "Registering" a Span with a request has moved from `requestCreated` to `beforeExecution`
  (where an `ExecutionAttributes` instance is first available)
* The ReferrerHeader is built and added to the http request in `modifyHttpRequest`,
  rather than in `beforeExecution`, where no http request is yet available
* Dynamic loading of interceptors has been implemented to reproduce previous behaviour
  with `RequestHandler2`s. The AWS SDK v2 offers an alternative mechanism, described
  [here](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html)
  under "Interceptor Registration", which could make it redundant.

In the Transfer Manager, `TransferListener` replaces `TransferStateChangeListener`. S3A code
has been updated and `AuditManagerS3A` implementations now provide an instance of the former to
switch to the active span, but registration of the new listeners is currently commented out because
it causes an incompatibility issue with the internal logger, resulting in `NoSuchMethodError`s,
at least in the current TransferManager Preview release.


### Metric Collection

`AwsStatisticsCollector` has been updated to implement the new `MetricPublisher` interface
and collect the metrics from a `MetricCollection` object.
The following table maps SDK v2 metrics to their equivalent in v1:

| v2 Metrics| com.amazonaws.util.AWSRequestMetrics.Field| Comment|
|-------------------------------------------------------------|---------------------------------------------|--------------------------------|
| CoreMetric.RETRY_COUNT| HttpClientRetryCount||
| CoreMetric.RETRY_COUNT| RequestCount| always HttpClientRetryCount+1|
| HttpMetric.HTTP_STATUS_CODE with HttpStatusCode.THROTTLING| ThrottleException| to be confirmed|
| CoreMetric.API_CALL_DURATION| ClientExecuteTime||
| CoreMetric.SERVICE_CALL_DURATION| HttpRequestTime||
| CoreMetric.MARSHALLING_DURATION| RequestMarshallTime||
| CoreMetric.SIGNING_DURATION| RequestSigningTime||
| CoreMetric.UNMARSHALLING_DURATION| ResponseProcessingTime| to be confirmed|

Note that none of the timing metrics (`*_DURATION`) are currently collected in S3A.

### Exception Handling

The code to handle exceptions thrown by the SDK has been updated to reflect the changes in v2:

* `com.amazonaws.SdkBaseException` and `com.amazonaws.AmazonClientException` changes:
  * These classes have combined and replaced with
    `software.amazon.awssdk.core.exception.SdkException`.
* `com.amazonaws.SdkClientException` changes:
  * This class has been replaced with `software.amazon.awssdk.core.exception.SdkClientException`.
  * This class now extends `software.amazon.awssdk.core.exception.SdkException`.
* `com.amazonaws.AmazonServiceException` changes:
  * This class has been replaced with
    `software.amazon.awssdk.awscore.exception.AwsServiceException`.
  * This class now extends `software.amazon.awssdk.core.exception.SdkServiceException`,
    a new exception type that extends `software.amazon.awssdk.core.exception.SdkException`.

See also the
[SDK changelog](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#3-exception-changes).


### Failure Injection

While using the SDK v1, failure injection was implemented in `InconsistentAmazonS3CClient`,
which extended the S3 client. In SDK v2, reproducing this approach would not be straightforward,
since the default S3 client is an internal final class. Instead, the same fault injection strategy
is now performed by a `FailureInjectionInterceptor` (see
[ExecutionInterceptor](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html))
registered on the default client by `InconsistentS3CClientFactory`.
`InconsistentAmazonS3CClient` has been removed. No changes to the user configuration are required.

