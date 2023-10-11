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

# Working with Third-party S3 Stores

The S3A connector works well with third-party S3 stores if the following requirements are met:

* It correctly implements the core S3 REST API, including support for uploads and the V2 listing API.
* The store supports the AWS V4 signing API *or* a custom signer is switched to.
  This release does not support the legacy v2 signing API.
* Errors are reported with the same HTTPS status codes as the S3 store. Error messages do not
  need to be consistent.
* The store is consistent.

There are also specific deployment requirements:
* The clock on the store and the client are close enough that signing works.
* The client is correctly configured to connect to the store *and not use unavailable features*
* If HTTPS authentication is used, the client/JVM TLS configurations allows it to authenticate the endpoint.

The features which may be unavailable include:

* Checksum-based server-side change detection during copy/read (`fs.s3a.change.detection.mode=server`)
* Object versioning and version-based change detection (`fs.s3a.change.detection.source=versionid` and `fs.s3a.versioned.store=true`)
* Bulk delete (`fs.s3a.multiobjectdelete.enable=true`)
* Encryption. (`fs.s3a.encryption.algorithm`)
* Storage class set in `fs.s3a.create.storage.class`
* Content encodings as set with `fs.s3a.object.content.encoding`.
* Optional Bucket Probes at startup (`fs.s3a.bucket.probe = 0`).
  This is now the default -do not change it.
* List API to use (`fs.s3a.list.version = 1`)

## Configuring s3a to connect to a third party store


### Connecting to a third party object store over HTTPS

The core setting for a third party store is to change the endpoint in `fs.s3a.endpoint`.

This can be a URL or a hostname/hostname prefix
For third-party stores without virtual hostname support, providing the URL is straightforward;
path style access must also be enabled in `fs.s3a.path.style.access`.

The v4 signing algorithm requires a region to be set in `fs.s3a.endpoint.region`.
A non-empty value is generally sufficient, though some deployments may require
a specific value.

Finally, assuming the credential source is the normal access/secret key
then these must be set, either in XML or (preferred) in a JCEKS file.

```xml

  <property>
    <name>fs.s3a.endpoint</name>
    <value>https://storeendpoint.example.com</value>
  </property>

  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.endpoint.region</name>
    <value>anything</value>
  </property>

  <property>
    <name>fs.s3a.access.key</name>
    <value>13324445</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <value>4C6B906D-233E-4E56-BCEA-304CC73A14F8</value>
  </property>

```

If per-bucket settings are used here, then third-party stores and credentials may be used alongside an AWS store.

# Troubleshooting

The most common problem when talking to third-party stores are

1. The S3A client is still configured to talk to the AWS S3 endpoint. This leads to authentication failures and/or reports that the bucket is unknown.
2. Path access has not been enabled, the client is generating a host name for the target bucket and it does not exist.
3. Invalid authentication credentials.
4. JVM HTTPS settings include the certificates needed to negotiate a TLS connection with the store.


## How to improve troubleshooting

### log more network info

There are some very low level logs.
```properties
# Log all HTTP requests made; includes S3 interaction. This may
# include sensitive information such as account IDs in HTTP headers.
log4j.logger.software.amazon.awssdk.request=DEBUG

# Turn on low level HTTP protocol debugging
log4j.logger.org.apache.http.wire=DEBUG

# async client
log4j.logger.io.netty.handler.logging=DEBUG
log4j.logger.io.netty.handler.codec.http2.Http2FrameLogger=DEBUG
```

### Cut back on retries, shorten timeouts

By default, there's a lot of retries going on in the AWS connector (which even retries on DNS failures)
and in the S3A code which invokes it.

Normally this helps prevent long-lived jobs from failing due to a transient network problem, however
it means that when trying to debug connectivity problems, the commands can hang for a long time
as they keep trying to reconnect to ports which are never going to be available.

```xml

  <property>
    <name>fs.iostatistics.logging.level</name>
    <value>info</value>
  </property>

  <property>
    <name>fs.s3a.bucket.nonexistent-bucket-example.attempts.maximum</name>
    <value>0</value>
  </property>

  <property>
   <name>fs.s3a.bucket.nonexistent-bucket-example.retry.limit</name>
   <value>1</value>
  </property>

  <property>
    <name>fs.s3a.bucket.nonexistent-bucket-example.connection.timeout</name>
    <value>500</value>
  </property>

  <property>
    <name>fs.s3a.bucket.nonexistent-bucket-example.connection.establish.timeout</name>
    <value>500</value>
  </property>
```
## Cloudstore's Storediag

There's an external utility, [cloudstore](https://github.com/steveloughran/cloudstore) whose [storediag](https://github.com/steveloughran/cloudstore#command-storediag) exists to debug the connection settings to hadoop cloud storage.

```bash
hadoop jar cloudstore-1.0.jar storediag s3a://nonexistent-bucket-example/
```

The main reason it's not an ASF release is that it allows for a rapid release cycle, sometimes hours; if anyone doesn't trust
third-party code then they can download and build it themselves.


# Problems

## S3A client still pointing at AWS endpoint

This is the most common initial problem, as it happens by default.

To fix, set `fs.s3a.endpoint` to the URL of the internal store.

### `org.apache.hadoop.fs.s3a.UnknownStoreException: `s3a://nonexistent-bucket-example/':  Bucket does not exist`

Either the bucket doesn't exist, or the bucket does exist but the endpoint is still set to an AWS endpoint.

```
stat: `s3a://nonexistent-bucket-example/':  Bucket does not exist
```
The hadoop filesystem commands don't log stack traces on failure -adding this adds too much risk
of breaking scripts, and the output is very uninformative

```
stat: nonexistent-bucket-example: getS3Region on nonexistent-bucket-example:
software.amazon.awssdk.services.s3.model.S3Exception: null
(Service: S3, Status Code: 403, Request ID: X26NWV0RJ1697SXF, Extended Request ID: bqq0rRm5Bdwt1oHSfmWaDXTfSOXoYvNhQxkhjjNAOpxhRaDvWArKCFAdL2hDIzgec6nJk1BVpJE=):null
```

It is possible to turn on debugging

```
log4j.logger.org.apache.hadoop.fs.shell=DEBUG
```

After which useful stack traces are logged.

```
org.apache.hadoop.fs.s3a.UnknownStoreException: `s3a://nonexistent-bucket-example/':  Bucket does not exist
    at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$null$3(S3AFileSystem.java:1075)
    at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:122)
    at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:376)
    at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:468)
    at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:372)
    at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:347)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$getS3Region$4(S3AFileSystem.java:1039)
    at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration(IOStatisticsBinding.java:543)
    at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.lambda$trackDurationOfOperation$5(IOStatisticsBinding.java:524)
    at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration(IOStatisticsBinding.java:445)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.trackDurationAndSpan(S3AFileSystem.java:2631)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.getS3Region(S3AFileSystem.java:1038)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.bindAWSClient(S3AFileSystem.java:982)
    at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:622)
    at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3452)
```

### `S3Exception: null (Service: S3, Status Code: 403...` or `AccessDeniedException`

* Endpoint is default
* Credentials were not issued by AWS .
* `fs.s3a.endpoint.region` unset.

If the client doesn't have any AWS credentials (from hadoop settings, environment variables or elsewhere) then
the binding will fail even before the existence of the bucket can be probed for.

```bash
hadoop fs -stat s3a://nonexistent-bucket-example
```


```
stat: nonexistent-bucket-example: getS3Region on nonexistent-bucket-example:
software.amazon.awssdk.services.s3.model.S3Exception: null (Service: S3, Status Code: 403,
 Request ID: X26NWV0RJ1697SXF, Extended Request ID: bqq0rRm5Bdwt1oHSfmWaDXTfSOXoYvNhQxkhjjNAOpxhRaDvWArKCFAdL2hDIzgec6nJk1BVpJE=):null
```

Or with a more detailed stack trace:

```
java.nio.file.AccessDeniedException: nonexistent-bucket-example: getS3Region on nonexistent-bucket-example: software.amazon.awssdk.services.s3.model.S3Exception: null (Service: S3, Status Code: 403, Request ID: X26NWV0RJ1697SXF, Extended Request ID: bqq0rRm5Bdwt1oHSfmWaDXTfSOXoYvNhQxkhjjNAOpxhRaDvWArKCFAdL2hDIzgec6nJk1BVpJE=):null
        at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:235)
        at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:124)
        at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$4(Invoker.java:376)
        at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:468)
        at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:372)
        at org.apache.hadoop.fs.s3a.Invoker.retry(Invoker.java:347)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$getS3Region$4(S3AFileSystem.java:1039)
        at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.invokeTrackingDuration(IOStatisticsBinding.java:543)
        at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.lambda$trackDurationOfOperation$5(IOStatisticsBinding.java:524)
        at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration(IOStatisticsBinding.java:445)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.trackDurationAndSpan(S3AFileSystem.java:2631)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.getS3Region(S3AFileSystem.java:1038)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.bindAWSClient(S3AFileSystem.java:982)
        at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:622)
        at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3452)
```

## `Received an UnknownHostException when attempting to interact with a service`


### Hypothesis 1: Region set, but not endpoint

The bucket `fs.s3a.endpoint.region` region setting is valid internally, but as the endpoint
is still AWS, this region is not recognised.
The S3A client's creation of an endpoint URL generates an unknown host.

```xml
  <property>
    <name>fs.s3a.bucket.nonexistent-bucket-example.endpoint.region</name>
    <value>internal</value>
  </property>
```


```
ls: software.amazon.awssdk.core.exception.SdkClientException:
    Received an UnknownHostException when attempting to interact with a service.
    See cause for the exact endpoint that is failing to resolve.
    If this is happening on an endpoint that previously worked, there may be
    a network connectivity issue or your DNS cache could be storing endpoints for too long.:
    nonexistent-bucket-example.s3.internal.amazonaws.com: nodename nor servname provided, or not known


```

### Hypothesis 2: region set, endpoint set, but `fs.s3a.path.style.access` is still set to `false`

* The bucket `fs.s3a.endpoint.region` region setting is valid internally,
* and `fs.s3a.endpoint` is set to a hostname (not a URL).
* `fs.s3a.path.style.access` set to `false`

```
ls: software.amazon.awssdk.core.exception.SdkClientException:
    Received an UnknownHostException when attempting to interact with a service.
    See cause for the exact endpoint that is failing to resolve.
    If this is happening on an endpoint that previously worked, there may be
    a network connectivity issue or your DNS cache could be storing endpoints for too long.:
    nonexistent-bucket-example.localhost: nodename nor servname provided, or not known
```

Fix: path style access

```xml
  <property>
    <name>fs.s3a.bucket.nonexistent-bucket-example.path.style.access</name>
    <value>true</value>
  </property>
```

# Connecting to Google Cloud Storage through the S3A connector

It *is* possible to connect to google cloud storage through the S3A connector.
However, Google provide their own [Cloud Storage connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage).
That is a well maintained Hadoop filesystem client which uses their XML API,
And except for some very unusual cases, that is the connector to use.

When interacting with a GCS container through the S3A connector may make sense
* The installation doesn't have the gcs-connector JAR.
* The different credential mechanism may be convenient.
* There's a desired to use S3A Delegation Tokens to pass secrets with a job.
* There's a desire to use an external S3A extension (delegation tokens etc.)

The S3A connector binding works through the Google Cloud [S3 Storage API](https://cloud.google.com/distributed-cloud/hosted/docs/ga/gdch/apis/storage-s3-rest-api),
which is a subset of the AWS API.


To get a compatible access and secret key, follow the instructions of
[Simple migration from Amazon S3 to Cloud Storage](https://cloud.google.com/storage/docs/aws-simple-migration#defaultproj).

Here are the per-bucket setings for an example bucket "gcs-container"
in Google Cloud Storage. Note the multiobject delete option must be disabled;
this makes renaming and deleting significantly slower.


```xml
<configuration>

  <property>
    <name>fs.s3a.bucket.gcs-container.access.key</name>
    <value>GOOG1EZ....</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.secret.key</name>
    <value>SECRETS</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.endpoint</name>
    <value>https://storage.googleapis.com</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.bucket.probe</name>
    <value>0</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.list.version</name>
    <value>1</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.multiobjectdelete.enable</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.select.enabled</name>
    <value>false</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.path.style.access</name>
    <value>true</value>
  </property>

  <property>
    <name>fs.s3a.bucket.gcs-container.endpoint.region</name>
    <value>dummy</value>
  </property>

</configuration>
```

This is a very rarely used configuration -however, it can be done, possibly as a way to interact with Google Cloud Storage in a deployment
which lacks the GCS connector.

It is also a way to regression test foundational S3A third-party store compatibility if you lack access to to any alternative.

```xml
<configuration>
  <property>
    <name>test.fs.s3a.encryption.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.scale.test.csvfile</name>
    <value></value>
  </property>
  <property>
    <name>test.fs.s3a.sts.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>test.fs.s3a.content.encoding.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

_Note_ If anyone is set up to test this reguarly, please let the hadoop developer team know if regressions do surface,
as it is not a common test configuration.