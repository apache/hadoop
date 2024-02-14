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

# Connecting to an Amazon S3 Bucket through the S3A Connector

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->


1. This document covers how to connect to and authenticate with S3 stores, primarily AWS S3.
2. There have been changes in this mechanism between the V1 and V2 SDK, in particular specifying
the region is now preferred to specifying the regional S3 endpoint.
3. For connecting to third-party stores, please read [Working with Third-party S3 Stores](third_party_stores.html) *after* reading this document.

## <a name="foundational"></a> Foundational Concepts

### <a name="regions"></a>  AWS Regions and Availability Zones

AWS provides storage, compute and other services around the world, in *regions*.

Data in S3 is stored *buckets*; each bucket is a single region.

There are some "special" regions: China, AWS GovCloud.
It is *believed* that the S3A connector works in these places, at least to the extent that nobody has complained about it not working.

### <a name="endpoints"></a> Endpoints

The S3A connector connects to Amazon S3 storage over HTTPS connections, either directly or through an HTTP proxy.
HTTP HEAD and GET, PUT, POST and DELETE requests are invoked to perform different read/write operations against the store.

There are multiple ways to connect to an S3 bucket

* To an [S3 Endpoint](https://docs.aws.amazon.com/general/latest/gr/s3.html); an HTTPS server hosted by amazon or a third party.
* To a FIPS-compliant S3 Endpoint.
* To an AWS S3 [Access Point](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points.html).
* Through a VPC connection, [AWS PrivateLink for Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html).
* AWS [Outposts](https://aws.amazon.com/outposts/).

The S3A connector supports all these; S3 Endpoints are the primary mechanism used -either explicitly declared or automatically determined from the declared region of the bucket.

Not supported:
* AWS [Snowball](https://aws.amazon.com/snowball/).

As of December 2023, AWS S3 uses Transport Layer Security (TLS) [version 1.2](https://aws.amazon.com/blogs/security/tls-1-2-required-for-aws-endpoints/) to secure the communications channel; the S3A client is does this through
the Apache [HttpClient library](https://hc.apache.org/index.html).

### <a name="third-party"></a> Third party stores

Third-party stores implementing the S3 API are also supported.
These often only implement a subset of the S3 API; not all features are available.
If TLS authentication is used, then the HTTPS certificates for the private stores
_MUST_ be installed on the JVMs on hosts within the Hadoop cluster.

See [Working with Third-party S3 Stores](third_party_stores.html) *after* reading this document.


## <a name="settings"></a> Connection Settings

There are three core settings to connect to an S3 store, endpoint, region and whether or not to use path style access.


```xml
<property>
  <name>fs.s3a.endpoint</name>
  <description>AWS S3 endpoint to connect to. An up-to-date list is
    provided in the AWS Documentation: regions and endpoints. Without this
    property, the endpoint/hostname of the S3 Store is inferred from
    the value of fs.s3a.endpoint.region, fs.s3a.endpoint.fips and more.
  </description>
</property>

<property>
  <name>fs.s3a.endpoint.region</name>
  <value>REGION</value>
  <description>AWS Region of the data</description>
</property>

<property>
  <name>fs.s3a.path.style.access</name>
  <value>false</value>
  <description>Enable S3 path style access by disabling the default virtual hosting behaviour.
    Needed for AWS PrivateLink, S3 AccessPoints, and, generally, third party stores.
    Default: false.
  </description>
</property>
```

Historically the S3A connector has preferred the endpoint as defined by the option `fs.s3a.endpoint`.
With the move to the AWS V2 SDK, there is more emphasis on the region, set by the `fs.s3a.endpoint.region` option.

Normally, declaring the region in `fs.s3a.endpoint.region` should be sufficient to set up the network connection to correctly connect to an AWS-hosted S3 store.

### <a name="s3_endpoint_region_details"></a> S3 endpoint and region settings in detail

* Configs `fs.s3a.endpoint` and `fs.s3a.endpoint.region` are used to set values
  for S3 endpoint and region respectively.
* If `fs.s3a.endpoint.region` is configured with valid AWS region value, S3A will
  configure the S3 client to use this value. If this is set to a region that does
  not match your bucket, you will receive a 301 redirect response.
* If `fs.s3a.endpoint.region` is not set and `fs.s3a.endpoint` is set with valid
  endpoint value, S3A will attempt to parse the region from the endpoint and
  configure S3 client to use the region value.
* If both `fs.s3a.endpoint` and `fs.s3a.endpoint.region` are not set, S3A will
  use `us-east-2` as default region and enable cross region access. In this case,
  S3A does not attempt to override the endpoint while configuring the S3 client.
* If `fs.s3a.endpoint` is not set and `fs.s3a.endpoint.region` is set to an empty
  string, S3A will configure S3 client without any region or endpoint override.
  This will allow fallback to S3 SDK region resolution chain. More details
  [here](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* If `fs.s3a.endpoint` is set to central endpoint `s3.amazonaws.com` and
  `fs.s3a.endpoint.region` is not set, S3A will use `us-east-2` as default region
  and enable cross region access. In this case, S3A does not attempt to override
  the endpoint while configuring the S3 client.
* If `fs.s3a.endpoint` is set to central endpoint `s3.amazonaws.com` and
  `fs.s3a.endpoint.region` is also set to some region, S3A will use that region
  value and enable cross region access. In this case, S3A does not attempt to
  override the endpoint while configuring the S3 client.

When the cross region access is enabled while configuring the S3 client, even if the
region set is incorrect, S3 SDK determines the region. This is done by making the
request, and if the SDK receives 301 redirect response, it determines the region at
the cost of a HEAD request, and caches it.

Please note that some endpoint and region settings that require cross region access
are complex and improving over time. Hence, they may be considered unstable.

If you are working with third party stores, please check [third party stores in detail](third_party_stores.html).

### <a name="timeouts"></a> Network timeouts

See [Timeouts](performance.html#timeouts).

### <a name="networking"></a> Low-level Network Options

```xml

<property>
  <name>fs.s3a.connection.maximum</name>
  <value>200</value>
  <description>Controls the maximum number of simultaneous connections to S3.
    This must be bigger than the value of fs.s3a.threads.max so as to stop
    threads being blocked waiting for new HTTPS connections.
  </description>
</property>

<property>
  <name>fs.s3a.connection.ssl.enabled</name>
  <value>true</value>
  <description>
    Enables or disables SSL connections to AWS services.
  </description>
</property>

<property>
  <name>fs.s3a.ssl.channel.mode</name>
  <value>Default_JSSE</value>
  <description>
    TLS implementation and cipher options.
    Values: OpenSSL, Default, Default_JSSE, Default_JSSE_with_GCM

    Default_JSSE is not truly the the default JSSE implementation because
    the GCM cipher is disabled when running on Java 8. However, the name
    was not changed in order to preserve backwards compatibility. Instead,
    new mode called Default_JSSE_with_GCM delegates to the default JSSE
    implementation with no changes to the list of enabled ciphers.

    OpenSSL requires the wildfly JAR on the classpath and a compatible installation of the openssl binaries.
    It is often faster than the JVM libraries, but also trickier to
    use.
  </description>
</property>

<property>
  <name>fs.s3a.socket.send.buffer</name>
  <value>8192</value>
  <description>
    Socket send buffer hint to amazon connector. Represented in bytes.
  </description>
</property>

<property>
  <name>fs.s3a.socket.recv.buffer</name>
  <value>8192</value>
  <description>
    Socket receive buffer hint to amazon connector. Represented in bytes.
  </description>
</property>
```

### <a name="proxies"></a> Proxy Settings

Connections to S3A stores can be made through an HTTP or HTTPS proxy.

```xml
<property>
  <name>fs.s3a.proxy.host</name>
  <description>
    Hostname of the (optional) proxy server for S3 connections.
  </description>
</property>

<property>
  <name>fs.s3a.proxy.ssl.enabled</name>
  <value>false</value>
  <description>
    Does the proxy use a TLS connection?
  </description>
</property>

<property>
  <name>fs.s3a.proxy.port</name>
  <description>
    Proxy server port. If this property is not set
    but fs.s3a.proxy.host is, port 80 or 443 is assumed (consistent with
    the value of fs.s3a.connection.ssl.enabled).
  </description>
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
```

Sometimes the proxy can be source of problems, especially if HTTP connections are kept
in the connection pool for some time.
Experiment with the values of `fs.s3a.connection.ttl` and `fs.s3a.connection.request.timeout`
if long-lived connections have problems.


##  <a name="per_bucket_endpoints"></a>Using Per-Bucket Configuration to access data round the world

S3 Buckets are hosted in different "regions", the default being "US-East-1".
The S3A client talks to this region by default, issuing HTTP requests
to the server `s3.amazonaws.com`.

S3A can work with buckets from any region. Each region has its own
S3 endpoint, documented [by Amazon](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region).

1. Applications running in EC2 infrastructure do not pay for IO to/from
*local S3 buckets*. They will be billed for access to remote buckets. Always
use local buckets and local copies of data, wherever possible.
2. With the V4 signing protocol, AWS requires the explicit region endpoint
to be used —hence S3A must be configured to use the specific endpoint. This
is done by setting the regon in the configuration option `fs.s3a.endpoint.region`,
or by explicitly setting `fs.s3a.endpoint` and `fs.s3a.endpoint.region`.
3. All endpoints other than the default region only support interaction
with buckets local to that S3 instance.
4. Standard S3 buckets support "cross-region" access where use of the original `us-east-1`
   endpoint allows access to the data, but newer storage types, particularly S3 Express are
   not supported.



If the wrong endpoint is used, the request will fail. This may be reported as a 301/redirect error,
or as a 400 Bad Request: take these as cues to check the endpoint setting of
a bucket.

The up to date list of regions is [Available online](https://docs.aws.amazon.com/general/latest/gr/s3.html).

This list can be used to specify the endpoint of individual buckets, for example
for buckets in the us-west-2 and EU/Ireland endpoints.


```xml
<property>
  <name>fs.s3a.bucket.us-west-2-dataset.endpoint.region</name>
  <value>us-west-2</value>
</property>

<property>
  <name>fs.s3a.bucket.eu-dataset.endpoint.region</name>
  <value>eu-west-1</value>
</property>
```


## <a name="privatelink"></a> AWS PrivateLink

[AWS PrivateLink for Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html) allows for a private connection to a bucket to be defined, with network access rules managing how a bucket can be accessed.


1. Follow the documentation to create the private link
2. retrieve the DNS name from the console, such as `vpce-f264a96c-6d27bfa7c85e.s3.us-west-2.vpce.amazonaws.com`
3. Convert this to an endpoint URL by prefixing "https://bucket."
4. Declare this as the bucket endpoint and switch to path-style access.
5. Declare the region: there is no automated determination of the region from
   the `vpce` URL.

```xml

<property>
  <name>fs.s3a.bucket.example-usw2.endpoint</name>
  <value>https://bucket.vpce-f264a96c-6d27bfa7c85e.s3.us-west-2.vpce.amazonaws.com/</value>
</property>

<property>
  <name>fs.s3a.bucket.example-usw2.path.style.access</name>
  <value>true</value>
</property>

<property>
  <name>fs.s3a.bucket.example-usw2.endpoint.region</name>
  <value>us-west-2</value>
</property>
```

## <a name="fips"></a> Federal Information Processing Standards (FIPS) Endpoints


It is possible to use [FIPs-compliant](https://www.nist.gov/itl/fips-general-information) endpoints which
support a restricted subset of TLS algorithms.

Amazon provide a specific set of [FIPS endpoints](https://aws.amazon.com/compliance/fips/)
to use so callers can be confident that the network communication is compliant with the standard:
non-compliant algorithms are unavailable.

The boolean option `fs.s3a.endpoint.fips` (default `false`) switches the S3A connector to using the FIPS endpoint of a region.

```xml
<property>
  <name>fs.s3a.endpoint.fips</name>
  <value>true</value>
  <description>Use the FIPS endpoint</description>
</property>
```

For a single bucket:
```xml
<property>
  <name>fs.s3a.bucket.noaa-isd-pds.endpoint.fips</name>
  <value>true</value>
  <description>Use the FIPS endpoint for the NOAA dataset</description>
</property>
```

If this option is `true`, the endpoint option `fs.s3a.endpoint` MUST NOT be set:

```
A custom endpoint cannot be combined with FIPS: https://s3.eu-west-2.amazonaws.com
```

The SDK calculates the FIPS-specific endpoint without any awareness as to whether FIPs is supported by a region. The first attempt to interact with the service will fail

```
java.net.UnknownHostException: software.amazon.awssdk.core.exception.SdkClientException:
Received an UnknownHostException when attempting to interact with a service.
    See cause for the exact endpoint that is failing to resolve.
    If this is happening on an endpoint that previously worked,
    there may be a network connectivity issue or your DNS cache
    could be storing endpoints for too long.:
    example-london-1.s3-fips.eu-west-2.amazonaws.com

```

*Important* OpenSSL and FIPS endpoints

Linux distributions with an FIPS-compliant SSL library may not be compatible with wildfly.
Always use with the JDK SSL implementation unless you are confident that the library
is compatible, or wish to experiment with the settings outside of production deployments.

```xml
<property>
  <name>fs.s3a.ssl.channel.mode</name>
  <value>Default_JSSE</value>
</property>
```

## <a name="accesspoints"></a>Configuring S3 AccessPoints usage with S3A

S3A supports [S3 Access Point](https://aws.amazon.com/s3/features/access-points/) usage which
improves VPC integration with S3 and simplifies your data's permission model because different
policies can be applied now on the Access Point level. For more information about why to use and
how to create them make sure to read the official documentation.

Accessing data through an access point, is done by using its ARN, as opposed to just the bucket name.
You can set the Access Point ARN property using the following per bucket configuration property:

```xml
<property>
  <name>fs.s3a.bucket.sample-bucket.accesspoint.arn</name>
  <value> {ACCESSPOINT_ARN_HERE} </value>
  <description>Configure S3a traffic to use this AccessPoint</description>
</property>
```

This configures access to the `sample-bucket` bucket for S3A, to go through the
new Access Point ARN. So, for example `s3a://sample-bucket/key` will now use your
configured ARN when getting data from S3 instead of your bucket.

_the name of the bucket used in the s3a:// URLs is irrelevant; it is not used when connecting with the store_

Example

```xml
<property>
  <name>fs.s3a.bucket.example-ap.accesspoint.arn</name>
  <value>arn:aws:s3:eu-west-2:152813717728:accesspoint/ap-example-london</value>
  <description>AccessPoint bound to bucket name example-ap</description>
</property>
```

The `fs.s3a.accesspoint.required` property can also require all access to S3 to go through Access
Points. This has the advantage of increasing security inside a VPN / VPC as you only allow access
to known sources of data defined through Access Points. In case there is a need to access a bucket
directly (without Access Points) then you can use per bucket overrides to disable this setting on a
bucket by bucket basis i.e. `fs.s3a.bucket.{YOUR-BUCKET}.accesspoint.required`.

```xml
<!-- Require access point only access -->
<property>
  <name>fs.s3a.accesspoint.required</name>
  <value>true</value>
</property>
<!-- Disable it on a per-bucket basis if needed -->
<property>
  <name>fs.s3a.bucket.example-bucket.accesspoint.required</name>
  <value>false</value>
</property>
```

Before using Access Points make sure you're not impacted by the following:
- The endpoint for S3 requests will automatically change to use
`s3-accesspoint.REGION.amazonaws.{com | com.cn}` depending on the Access Point ARN. While
considering endpoints, if you have any custom signers that use the host endpoint property make
sure to update them if needed;

## <a name="debugging"></a> Debugging network problems

The `storediag` command within the utility [cloudstore](https://github.com/exampleoughran/cloudstore)
JAR is recommended as the way to view and print settings.

If `storediag` doesn't connect to your S3 store, *nothing else will*.

## <a name="common-problems"></a> Common Sources of Connection Problems

Based on the experience of people who field support calls, here are
some of the main connectivity issues which cause problems.

### <a name="inconsistent-config"></a> Inconsistent configuration across a cluster

All hosts in the cluster need to have the configuration secrets;
local environment variables are not enough.

If HTTPS/TLS is used for a private store, the relevant certificates MUST be installed everywhere.

For applications such as distcp, the options need to be passed with the job.

### <a name="public-private-mixup"></a> Confusion between public/private S3 Stores.

If your cluster is configured to use a private store, AWS-hosted buckets are not visible.
If you wish to read access in a private store, you need to change the endpoint.

Private S3 stores generally expect path style access.

### <a name="region-misconfigure"></a> Region and endpoints misconfigured

These usually surface rapidly and with meaningful messages.

Region errors generally surface as
* `UnknownHostException`
* `AWSRedirectException` "Received permanent redirect response to region"

Endpoint configuration problems can be more varied, as they are just HTTPS URLs.

### <a name="wildfly"></a> Wildfly/OpenSSL Brittleness

When it works, it is fast. But it is fussy as to openSSL implementations, TLS protocols and more.
Because it uses the native openssl binaries, operating system updates can trigger regressions.

Disabling it should be the first step to troubleshooting any TLS problems.

### <a name="proxy-misconfiguration"></a> Proxy setup

If there is a proxy, set it up correctly.
