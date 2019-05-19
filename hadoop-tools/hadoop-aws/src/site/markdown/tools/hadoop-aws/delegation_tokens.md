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

# Working with Delegation Tokens

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

## <a name="introduction"></a> Introducing S3A Delegation Tokens.

The S3A filesystem client supports `Hadoop Delegation Tokens`.
This allows YARN application like MapReduce, Distcp, Apache Flink and Apache Spark to
obtain credentials to access S3 buckets and pass them pass these credentials to
jobs/queries, so granting them access to the service with the same access
permissions as the user.

Three different token types are offered.

*Full Delegation Tokens:* include the full login values of `fs.s3a.access.key`
and `fs.s3a.secret.key` in the token, so the recipient has access to
the data as the submitting user, with unlimited duration.
These tokens do not involve communication with the AWS STS service, so
can be used with other S3 installations.

*Session Delegation Tokens:* These contain an "STS Session Token" requested by
the S3A client from the AWS STS service. They have a limited duration
so restrict how long an application can access AWS on behalf of a user.
Clients with this token have the full permissions of the user.

*Role Delegation Tokens:* These contain an "STS Session Token" requested by by the
STS "Assume Role" API, so grant the caller to interact with S3 as specific AWS
role, *with permissions restricted to purely accessing the S3 bucket
and associated S3Guard data*.

Role Delegation Tokens are the most powerful. By restricting the access rights
of the granted STS token, no process receiving the token may perform
any operations in the AWS infrastructure other than those for the S3 bucket,
and that restricted by the rights of the requested role ARN.

All three tokens also marshall the encryption settings: The encryption mechanism
to use and the KMS key ID or SSE-C client secret. This allows encryption
policy and secrets to be uploaded from the client to the services.

This document covers how to use these tokens. For details on the implementation
see [S3A Delegation Token Architecture](delegation_token_architecture.html).

## <a name="background"></a> Background: Hadoop Delegation Tokens.

A Hadoop Delegation Token are is a byte array of data which is submitted to
a Hadoop services as proof that the caller has the permissions to perform
the operation which it is requesting —
and which can be passed between applications to *delegate* those permission.

Tokens are opaque to clients, clients who simply get a byte array
of data which they must to provide to a service when required.
This normally contains encrypted data for use by the service.

The service, which holds the password to encrypt/decrypt this data,
can decrypt the byte array and read the contents,
knowing that it has not been tampered with, then
use the presence of a valid token as evidence the caller has
at least temporary permissions to perform the requested operation.

Tokens have a limited lifespan.
They may be renewed, with the client making an IPC/HTTP request of a renewer service.
This renewal service can also be executed on behalf of the caller by
some other Hadoop cluster services, such as the YARN Resource Manager.

After use, tokens may be revoked: this relies on services holding tables of
valid tokens, either in memory or, for any HA service, in Apache Zookeeper or
similar. Revoking tokens is used to clean up after jobs complete.

Delegation support is tightly integrated with YARN: requests to launch
containers and applications can include a list of delegation tokens to
pass along. These tokens are serialized with the request, saved to a file
on the node launching the container, and then loaded in to the credentials
of the active user. Normally the HDFS cluster is one of the tokens used here,
added to the credentials through a call to `FileSystem.getDelegationToken()`
(usually via `FileSystem.addDelegationTokens()`).

Delegation Tokens are also supported with applications such as Hive: a query
issued to a shared (long-lived) Hive cluster can include the delegation
tokens required to access specific filesystems *with the rights of the user
submitting the query*.

All these applications normally only retrieve delegation tokens when security
is enabled. This is why the cluster configuration needs to enable Kerberos.
Production Hadoop clusters need Kerberos for security anyway.


## <a name="s3a-delegation-tokens"></a> S3A Delegation Tokens.

S3A now supports delegation tokens, so allowing a caller to acquire tokens
from a local S3A Filesystem connector instance and pass them on to
applications to grant them equivalent or restricted access.

These S3A Delegation Tokens are special in that they do not contain
password-protected data opaque to clients; they contain the secrets needed
to access the relevant S3 buckets and associated services.

They are obtained by requesting a delegation token from the S3A filesystem client.
Issued token mey be included in job submissions, passed to running applications,
etc. This token is specific to an individual bucket; all buckets which a client
wishes to work with must have a separate delegation token issued.

S3A implements Delegation Tokens in its `org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens`
class, which then supports multiple "bindings" behind it, so supporting
different variants of S3A Delegation Tokens.

Because applications only collect Delegation Tokens in secure clusters,
It does mean that to be able to submit delegation tokens in transient
cloud-hosted Hadoop clusters, _these clusters must also have Kerberos enabled_.


### <a name="session-tokens"></a> S3A Session Delegation Tokens

A Session Delegation Token is created by asking the AWS
[Security Token Service](http://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html)
to issue an AWS session password and identifier for a limited duration.
These AWS session credentials are valid until the end of that time period.
They are marshalled into the S3A Delegation Token.

Other S3A connectors can extract these credentials and use them to
talk to S3 and related services.

Issued tokens cannot be renewed or revoked.

See [GetSessionToken](http://docs.aws.amazon.com/STS/latest/APIReference/API_GetSessionToken.html)
for specifics details on the (current) token lifespan.

### <a name="role-tokens"></a> S3A Role Delegation Tokens

A Role Delegation Tokens is created by asking the AWS
[Security Token Service](http://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html)
for set of "Assumed Role" credentials, with a AWS account specific role for a limited duration..
This role is restricted to only grant access the S3 bucket, the S3Guard table
and all KMS keys,
They are marshalled into the S3A Delegation Token.

Other S3A connectors can extract these credentials and use them to
talk to S3 and related services.
They may only work with the explicit AWS resources identified when the token was generated.

Issued tokens cannot be renewed or revoked.


### <a name="full-credentials"></a> S3A Full-Credential Delegation Tokens

Full Credential Delegation Tokens tokens contain the full AWS login details
(access key and secret key) needed to access a bucket.

They never expire, so are the equivalent of storing the AWS account credentials
in a Hadoop, Hive, Spark configuration or similar.

They differences are:

1. They are automatically passed from the client/user to the application.
A remote application can use them to access data on behalf of the user.
1. When a remote application destroys the filesystem connector instances and
tokens of a user, the secrets are destroyed too.
1. Secrets in the `AWS_` environment variables on the client will be picked up
and automatically propagated.
1. They do not use the AWS STS service, so may work against third-party implementations
of the S3 protocol.


## <a name="enabling "></a> Using S3A Delegation Tokens

A prerequisite to using S3A filesystem delegation tokens is to run with
Hadoop security enabled —which inevitably means with Kerberos.
Even though S3A delegation tokens do not use Kerberos, the code in
applications which fetch DTs is normally only executed when the cluster is
running in secure mode; somewhere where the `core-site.xml` configuration
sets `hadoop.security.authentication` to to `kerberos` or another valid
authentication mechanism.

* Without enabling security at this level, delegation tokens will not
be collected.*

Once Kerberos enabled, the process for acquiring tokens is as follows:

1. Enable Delegation token support by setting `fs.s3a.delegation.token.binding`
to the classname of the token binding to use.
to use.
1. Add any other binding-specific settings (STS endpoint, IAM role, etc.)
1. Make sure the settings are the same in the service as well as the client.
1. In the client, switch to using a [Hadoop Credential Provider](hadoop-project-dist/hadoop-common/CredentialProviderAPI.html)
for storing your local credentials, *with a local filesystem store
 (`localjceks:` or `jcecks://file`), so as to keep the full secrets out of any
 job configurations.
1. Execute the client from a Kerberos-authenticated account
application configured with the login credentials for an AWS account able to issue session tokens.

### Configuration Parameters


| **Key** | **Meaning** | **Default** |
| --- | --- | --- |
| `fs.s3a.delegation.token.binding` | delegation token binding class |  `` |

### Warnings

##### Use Hadoop Credential Providers to keep secrets out of job configurations.

Hadoop MapReduce jobs copy their client-side configurations with the job.
If your AWS login secrets are set in an XML file then they are picked up
and passed in with the job, _even if delegation tokens are used to propagate
session or role secrets.

Spark-submit will take any credentials in the `spark-defaults.conf`file
and again, spread them across the cluster.
It wil also pick up any `AWS_` environment variables and convert them into
`fs.s3a.access.key`, `fs.s3a.secret.key` and `fs.s3a.session.key` configuration
options.

To guarantee that the secrets are not passed in, keep your secrets in
a [hadoop credential provider file on the local filesystem](index.html#hadoop_credential_providers").
Secrets stored here will not be propagated -the delegation tokens collected
during job submission will be the sole AWS secrets passed in.


##### Token Life

* S3A Delegation tokens cannot be renewed.

* S3A Delegation tokens cannot be revoked. It is possible for an administrator
to terminate *all AWS sessions using a specific role*
[from the AWS IAM console](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_control-access_disable-perms.html),
if desired.

* The lifespan of Session Delegation Tokens are limited to those of AWS sessions,
maximum of 36 hours.

* The lifespan of a Role Delegation Token is limited to 1 hour by default;
a longer duration of up to 12 hours can be enabled in the AWS console for
the specific role being used.

* The lifespan of Full Delegation tokens is unlimited: the secret needs
to be reset in the AWS Admin console to revoke it.

##### Service Load on the AWS Secure Token Service

All delegation tokens are issued on a bucket-by-bucket basis: clients
must request a delegation token from every S3A filesystem to which it desires
access.

For Session and Role Delegation Tokens, this places load on the AWS STS service,
which may trigger throttling amongst all users within the same AWS account using
the same STS endpoint.

* In experiments, a few hundred requests per second are needed to trigger throttling,
so this is very unlikely to surface in production systems.
* The S3A filesystem connector retries all throttled requests to AWS services, including STS.
* Other S3 clients with use the AWS SDK will, if configured, also retry throttled requests.

Overall, the risk of triggering STS throttling appears low, and most applications
will recover from what is generally an intermittently used AWS service.

### <a name="enabling-session-tokens"></a> Enabling Session Delegation Tokens

For session tokens, set `fs.s3a.delegation.token.binding`
to `org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding`


|  **Key** | **Value** |
| --- | --- |
| `fs.s3a.delegation.token.binding` | `org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding` |

There some further configuration options.

| **Key** | **Meaning** | **Default** |
| --- | --- | --- |
| `fs.s3a.assumed.role.session.duration` | Duration of delegation tokens |  `1h` |
| `fs.s3a.assumed.role.sts.endpoint` | URL to service issuing tokens |  (undefined) |
| `fs.s3a.assumed.role.sts.endpoint.region` | region for issued tokens |  (undefined) |

The XML settings needed to enable session tokens are:

```xml
<property>
  <name>fs.s3a.delegation.token.binding</name>
  <value>org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding</value>
</property>
<property>
  <name>fs.s3a.assumed.role.session.duration</name>
  <value>1h</value>
</property>
```

1. If the application requesting a token has full AWS credentials for the
relevant bucket, then a new session token will be issued.
1. If the application requesting a token is itself authenticating with
a session delegation token, then the existing token will be forwarded.
The life of the token will not be extended.
1. If the application requesting a token does not have either of these,
the the tokens cannot be issued: the operation will fail with an error.


The endpoint for STS requests are set by the same configuration
property as for the `AssumedRole` credential provider and for Role Delegation
tokens.

```xml
<!-- Optional -->
<property>
  <name>fs.s3a.assumed.role.sts.endpoint</name>
  <value>sts.amazonaws.com</value>
</property>
<property>
  <name>fs.s3a.assumed.role.sts.endpoint.region</name>
  <value>us-west-1</value>
</property>
```

If the `fs.s3a.assumed.role.sts.endpoint` option is set, or set to something
other than the central `sts.amazonaws.com` endpoint, then the region property
*must* be set.


Both the Session and the Role Delegation Token bindings use the option
`fs.s3a.aws.credentials.provider` to define the credential providers
to authenticate to the AWS STS with.

Here is the effective list of providers if none are declared:

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>
    org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,
    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
    com.amazonaws.auth.EnvironmentVariableCredentialsProvider,
    org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
  </value>
</property>
```

Not all these authentication mechanisms provide the full set of credentials
STS needs. The session token provider will simply forward any session credentials
it is authenticated with; the role token binding will fail.

#### Forwarding of existing AWS Session credentials.

When the AWS credentials supplied to the Session Delegation Token binding
through `fs.s3a.aws.credentials.provider` are themselves a set of
session credentials, generated delegation tokens with simply contain these
existing session credentials, a new set of credentials obtained from STS.
This is because the STS service does not let
callers authenticated with session/role credentials from requesting new sessions.

This feature is useful when generating tokens from an EC2 VM instance in one IAM
role and forwarding them over to VMs which are running in a different IAM role.
The tokens will grant the permissions of the original VM's IAM role.

The duration of the forwarded tokens will be exactly that of the current set of
tokens, which may be very limited in lifespan. A warning will appear
in the logs declaring this.

Note: Role Delegation tokens do not support this forwarding of session credentials,
because there's no way to explicitly change roles in the process.


### <a name="enabling-role-tokens"></a> Enabling Role Delegation Tokens

For role delegation tokens, set `fs.s3a.delegation.token.binding`
to `org.apache.hadoop.fs.s3a.auth.delegation.RoleTokenBinding`

|  **Key** | **Value** |
| --- | --- |
| `fs.s3a.delegation.token.binding` | `org.apache.hadoop.fs.s3a.auth.delegation.SessionToRoleTokenBinding` |


There are some further configuration options:

| **Key** | **Meaning** | **Default** |
| --- | --- | --- |
| `fs.s3a.assumed.role.session.duration"` | Duration of delegation tokens |  `1h` |
| `fs.s3a.assumed.role.arn` | ARN for role to request |  (undefined) |
| `fs.s3a.assumed.role.sts.endpoint.region` | region for issued tokens |  (undefined) |

The option `fs.s3a.assumed.role.arn` must be set to a role which the
user can assume. It must have permissions to access the bucket, any
associated S3Guard table and any KMS encryption keys. The actual
requested role will be this role, explicitly restricted to the specific
bucket and S3Guard table.

The XML settings needed to enable session tokens are:

```xml
<property>
  <name>fs.s3a.delegation.token.binding</name>
  <value>org.apache.hadoop.fs.s3a.auth.delegation.RoleTokenBinding</value>
</property>
<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value>ARN of role to request</value>
  <value>REQUIRED ARN</value>
</property>
<property>
  <name>fs.s3a.assumed.role.session.duration</name>
  <value>1h</value>
</property>
```

A JSON role policy for the role/session will automatically be generated which will
consist of
1. Full access to the S3 bucket for all operations used by the S3A client
(read, write, list, multipart operations, get bucket location, etc).
1. Full user access to any S3Guard DynamoDB table used by the bucket.
1. Full user access to KMS keys. This is to be able to decrypt any data
in the bucket encrypted with SSE-KMS, as well as encrypt new data if that
is the encryption policy.

If the client doesn't have S3Guard enabled, but the remote application does,
the issued role tokens will not have permission to access the S3Guard table.

### <a name="enabling-full-tokens"></a> Enabling Full Delegation Tokens

This passes the full credentials in, falling back to any session credentials
which were used to configure the S3A FileSystem instance.

For Full Credential Delegation tokens, set `fs.s3a.delegation.token.binding`
to `org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenBinding`

|  **Key** | **Value** |
| --- | --- |
| `fs.s3a.delegation.token.binding` | `org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenBinding` |

There are no other configuration options.

```xml
<property>
  <name>fs.s3a.delegation.token.binding</name>
  <value>org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenBinding</value>
</property>
```

Key points:

1. If the application requesting a token has full AWS credentials for the
relevant bucket, then a full credential token will be issued.
1. If the application requesting a token is itself authenticating with
a session delegation token, then the existing token will be forwarded.
The life of the token will not be extended.
1. If the application requesting a token does not have either of these,
the the tokens cannot be issued: the operation will fail with an error.

## <a name="managing_token_duration"></a> Managing the Delegation Tokens Duration

Full Credentials have an unlimited lifespan.

Session and role credentials have a lifespan defined by the duration
property `fs.s3a.assumed.role.session.duration`.

This can have a maximum value of "36h" for session delegation tokens.

For Role Delegation Tokens, the maximum duration of a token is
that of the role itself: 1h by default, though this can be changed to
12h [In the IAM Console](https://console.aws.amazon.com/iam/home#/roles),
or from the AWS CLI.

*Without increasing the duration of role, one hour is the maximum value;
the error message `The requested DurationSeconds exceeds the MaxSessionDuration set for this role`
is returned if the requested duration of a Role Delegation Token is greater
than that available for the role.


## <a name="testing"></a> Testing Delegation Token Support

The easiest way to test that delegation support is configured is to use
the `hdfs fetchdt` command, which can fetch tokens from S3A, Azure ABFS
and any other filesystem which can issue tokens, as well as HDFS itself.

This will fetch the token and save it to the named file (here, `tokens.bin`),
even if Kerberos is disabled.

```bash
# Fetch a token for the AWS landsat-pds bucket and save it to tokens.bin
$ hdfs fetchdt --webservice s3a://landsat-pds/  tokens.bin
```

If the command fails with `ERROR: Failed to fetch token` it means the
filesystem does not have delegation tokens enabled.

If it fails for other reasons, the likely causes are configuration and
possibly connectivity to the AWS STS Server.

Once collected, the token can be printed. This will show
the type of token, details about encryption and expiry, and the
host on which it was created.

```bash
$ bin/hdfs fetchdt --print tokens.bin

Token (S3ATokenIdentifier{S3ADelegationToken/Session; uri=s3a://landsat-pds;
timestamp=1541683947569; encryption=EncryptionSecrets{encryptionMethod=SSE_S3};
Created on vm1.local/192.168.99.1 at time 2018-11-08T13:32:26.381Z.};
Session credentials for user AAABWL expires Thu Nov 08 14:02:27 GMT 2018; (valid))
for s3a://landsat-pds
```
The "(valid)" annotation means that the AWS credentials are considered "valid":
there is both a username and a secret.

You can use the `s3guard bucket-info` command to see what the delegation
support for a specific bucket is.
If delegation support is enabled, it also prints the current
hadoop security level.

```bash
$ hadoop s3guard bucket-info s3a://landsat-pds/

Filesystem s3a://landsat-pds
Location: us-west-2
Filesystem s3a://landsat-pds is not using S3Guard
The "magic" committer is not supported

S3A Client
  Signing Algorithm: fs.s3a.signing-algorithm=(unset)
  Endpoint: fs.s3a.endpoint=s3.amazonaws.com
  Encryption: fs.s3a.server-side-encryption-algorithm=none
  Input seek policy: fs.s3a.experimental.input.fadvise=normal
  Change Detection Source: fs.s3a.change.detection.source=etag
  Change Detection Mode: fs.s3a.change.detection.mode=server
Delegation Support enabled: token kind = S3ADelegationToken/Session
Hadoop security mode: SIMPLE
```

Although the S3A delegation tokens do not depend upon Kerberos,
MapReduce and other applications only request tokens from filesystems when
security is enabled in Hadoop.


## <a name="troubleshooting"></a> Troubleshooting S3A Delegation Tokens

The `hadoop s3guard bucket-info` command will print information about
the delegation state of a bucket.

Consult [troubleshooting Assumed Roles](assumed_roles.html#troubleshooting)
for details on AWS error messages related to AWS IAM roles.

The [cloudstore](https://github.com/steveloughran/cloudstore) module's StoreDiag
utility can also be used to explore delegation token support


### Submitted job cannot authenticate

There are many causes for this; delegation tokens add some more.

### Tokens are not issued


* This user is not `kinit`-ed in to Kerberos. Use `klist` and
`hadoop kdiag` to see the Kerberos authentication state of the logged in user.
* The filesystem instance on the client has not had a token binding set in
`fs.s3a.delegation.token.binding`, so does not attempt to issue any.
* The job submission is not aware that access to the specific S3 buckets
are required. Review the application's submission mechanism to determine
how to list source and destination paths. For example, for MapReduce,
tokens for the cluster filesystem (`fs.defaultFS`) and all filesystems
referenced as input and output paths will be queried for
delegation tokens.

For Apache Spark, the cluster filesystem and any filesystems listed in the
property `spark.yarn.access.hadoopFileSystems` are queried for delegation
tokens in secure clusters.
See [Running on Yarn](https://spark.apache.org/docs/latest/running-on-yarn.html).


### Error `No AWS login credentials`

The client does not have any valid credentials to request a token
from the Amazon STS service.

### Tokens Expire before job completes

The default duration of session and role tokens as set in
`fs.s3a.assumed.role.session.duration` is one hour, "1h".

For session tokens, this can be increased to any time up to 36 hours.

For role tokens, it can be increased up to 12 hours, *but only if
the role is configured in the AWS IAM Console to have a longer lifespan*.


### Error `DelegationTokenIOException: Token mismatch`

```
org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenIOException:
 Token mismatch: expected token for s3a://example-bucket
 of type S3ADelegationToken/Session but got a token of type S3ADelegationToken/Full

  at org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupToken(S3ADelegationTokens.java:379)
  at org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.selectTokenFromActiveUser(S3ADelegationTokens.java:300)
  at org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.bindToExistingDT(S3ADelegationTokens.java:160)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.bindAWSClient(S3AFileSystem.java:423)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:265)
```

The value of `fs.s3a.delegation.token.binding` is different in the remote
service than in the local client. As a result, the remote service
cannot use the token supplied by the client to authenticate.

Fix: reference the same token binding class at both ends.


### Warning `Forwarding existing session credentials`

This message is printed when an S3A filesystem instance has been asked
for a Session Delegation Token, and it is itself only authenticated with
a set of AWS session credentials (such as those issued by the IAM metadata
service).

The created token will contain these existing credentials, credentials which
can be used until the existing session expires.

The duration of this existing session is unknown: the message is warning
you that it may expire without warning.

### Error `Cannot issue S3A Role Delegation Tokens without full AWS credentials`

An S3A filesystem instance has been asked for a Role Delegation Token,
but the instance is only authenticated with session tokens.
This means that a set of role tokens cannot be requested.

Note: no attempt is made to convert the existing set of session tokens into
a delegation token, unlike the Session Delegation Tokens. This is because
the role of the current session (if any) is unknown.


## <a name="implementation"></a> Implementation Details

### <a name="architecture"></a> Architecture

Concepts:

1. The S3A FileSystem can create delegation tokens when requested.
1. These can be marshalled as per other Hadoop Delegation Tokens.
1. At the far end, they can be retrieved, unmarshalled and used to authenticate callers.
1. DT binding plugins can then use these directly, or, somehow,
manage authentication and token issue through other services
(for example: Kerberos)
1. Token Renewal and Revocation are not supported.


There's support for different back-end token bindings through the
`org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokenManager`

Every implementation of this must return a subclass of
`org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier`
when asked to create a delegation token; this subclass must be registered
in `META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`
for unmarshalling.

This identifier must contain all information needed at the far end to
authenticate the caller with AWS services used by the S3A client: AWS S3 and
potentially AWS KMS (for SSE-KMS) and AWS DynamoDB (for S3Guard).

It must have its own unique *Token Kind*, to ensure that it can be distinguished
from the other token identifiers when tokens are being unmarshalled.

| Kind |  Token class |
|------|--------------|
| `S3ADelegationToken/Full` | `org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenIdentifier` |
| `S3ADelegationToken/Session` | `org.apache.hadoop.fs.s3a.auth.delegation.RoleTokenIdentifier`|
| `S3ADelegationToken/Role` | `org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenIdentifier` |

If implementing an external binding:

1. Follow the security requirements below.
1. Define a new token identifier; there is no requirement for the `S3ADelegationToken/`
prefix —but it is useful for debugging.
1. Token Renewal and Revocation is not integrated with the binding mechanism;
if the operations are supported, implementation is left as an exercise.
1. Be aware of the stability guarantees of the module "LimitedPrivate/Unstable".

### <a name="security"></a> Security

S3A DTs contain secrets valuable for a limited period (session secrets) or
long-lived secrets with no explicit time limit.

* The `toString()` operations on token identifiers MUST NOT print secrets; this
is needed to keep them out of logs.
* Secrets MUST NOT be logged, even at debug level.
* Prefer short-lived session secrets over long-term secrets.
* Try to restrict the permissions to what a client with the delegated token
  may perform to those needed to access data in the S3 bucket. This potentially
  includes a DynamoDB table, KMS access, etc.
* Implementations need to be resistant to attacks which pass in invalid data as
their token identifier: validate the types of the unmarshalled data; set limits
on the size of all strings and other arrays to read in, etc.

### <a name="resilience"></a> Resilience

Implementations need to handle transient failures of any remote authentication
service, and the risk of a large-cluster startup overloading it.

* All get/renew/cancel operations should be considered idempotent.
* And clients to repeat with backoff & jitter on recoverable connectivity failures.
* While failing fast on the unrecoverable failures (DNS, authentication).

### <a name="scalability"></a> Scalability limits of AWS STS service

There is currently no documented rate limit for token requests against the AWS
STS service.

We have two tests which attempt to generate enough requests for
delegation tokens that the AWS STS service will throttle requests for
tokens by that AWS account for that specific STS endpoint
(`ILoadTestRoleCredentials` and `ILoadTestSessionCredentials`).

In the initial results of these tests:

* A few hundred requests a second can be made before STS block the caller.
* The throttling does not last very long (seconds)
* Tt does not appear to affect any other STS endpoints.

If developers wish to experiment with these tests and provide more detailed
analysis, we would welcome this. Do bear in mind that all users of the
same AWS account in that region will be throttled. Your colleagues may
notice, especially if the applications they are running do not retry on
throttle responses from STS (it's not a common occurrence after all...).

## Implementing your own Delegation Token Binding

The DT binding mechanism is designed to be extensible: if you have an alternate
authentication mechanism, such as an S3-compatible object store with
Kerberos support —S3A Delegation tokens should support it.

*if it can't: that's a bug in the implementation which needs to be corrected*.

### Steps

1. Come up with a token "Kind"; a unique name for the delegation token identifier.
1. Implement a subclass of `AbstractS3ATokenIdentifier` which adds all information which
is marshalled from client to remote services. This must subclass the `Writable` methods to read
and write the data to a data stream: these subclasses must call the superclass methods first.
1. Add a resource `META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`
1. And list in it, the classname of your new identifier.
1. Implement a subclass of `AbstractDelegationTokenBinding`

### Implementing `AbstractS3ATokenIdentifier`

Look at the other examples to see what to do; `SessionTokenIdentifier` does
most of the work.

Having a `toString()` method which is informative is ideal for the `hdfs creds`
command as well as debugging: *but do not print secrets*

*Important*: Add no references to any AWS SDK class, to
ensure it can be safely deserialized whenever the relevant token
identifier is examined. Best practise is: avoid any references to
classes which may not be on the classpath of core Hadoop services,
especially the YARN Resource Manager and Node Managers.

### `AWSCredentialProviderList deployUnbonded()`

1. Perform all initialization needed on an "unbonded" deployment to authenticate with the store.
1. Return a list of AWS Credential providers which can be used to authenticate the caller.

**Tip**: consider *not* doing all the checks to verify that DTs can be issued.
That can be postponed until a DT is issued -as in any deployments where a DT is not actually
needed, failing at this point is overkill. As an example, `RoleTokenBinding` cannot issue
DTs if it only has a set of session credentials, but it will deploy without them, so allowing
`hadoop fs` commands to work on an EC2 VM with IAM role credentials.

**Tip**: The class `org.apache.hadoop.fs.s3a.auth.MarshalledCredentials` holds a set of
marshalled credentials and so can be used within your own Token Identifier if you want
to include a set of full/session AWS credentials in your token identifier.

### `AWSCredentialProviderList bindToTokenIdentifier(AbstractS3ATokenIdentifier id)`

The identifier passed in will be the one for the current filesystem URI and of your token kind.

1. Use `convertTokenIdentifier` to cast it to your DT type, or fail with a meaningful `IOException`.
1. Extract the secrets needed to authenticate with the object store (or whatever service issues
object store credentials).
1. Return a list of AWS Credential providers which can be used to authenticate the caller with
the extracted secrets.

### `AbstractS3ATokenIdentifier createEmptyIdentifier()`

Return an empty instance of your token identifier.

### `AbstractS3ATokenIdentifier createTokenIdentifier(Optional<RoleModel.Policy> policy,  EncryptionSecrets secrets)`

Create the delegation token.

If non-empty, the `policy` argument contains an AWS policy model to grant access to:

* The target S3 bucket.
* Any S3Guard DDB table it is bonded to.
* KMS key `"kms:GenerateDataKey` and `kms:Decrypt`permissions for all KMS keys.

This can be converted to a string and passed to the AWS `assumeRole` operation.

The `secrets` argument contains encryption policy and secrets:
this should be passed to the superclass constructor as is; it is retrieved and used
to set the encryption policy on the newly created filesystem.


*Tip*: Use `AbstractS3ATokenIdentifier.createDefaultOriginMessage()` to create an initial
message for the origin of the token —this is useful for diagnostics.


#### Token Renewal

There's no support in the design for token renewal; it would be very complex
to make it pluggable, and as all the bundled mechanisms don't support renewal,
untestable and unjustifiable.

Any token binding which wants to add renewal support will have to implement
it directly.

### Testing

Use the tests `org.apache.hadoop.fs.s3a.auth.delegation` as examples. You'll have to
copy and paste some of the test base classes over; `hadoop-common`'s test JAR is published
to Maven Central, but not the S3A one (a fear of leaking AWS credentials).


#### Unit Test `TestS3ADelegationTokenSupport`

This tests marshalling and unmarshalling of tokens identifiers.
*Test that every field is preserved.*


#### Integration Test `ITestSessionDelegationTokens`

Tests the lifecycle of session tokens.

#### Integration Test `ITestSessionDelegationInFileystem`.

This collects DTs from one filesystem, and uses that to create a new FS instance and
then perform filesystem operations. A miniKDC is instantiated

* Take care to remove all login secrets from the environment, so as to make sure that
the second instance is picking up the DT information.
* `UserGroupInformation.reset()` can be used to reset user secrets after every test
case (e.g. teardown), so that issued DTs from one test case do not contaminate the next.
* its subclass, `ITestRoleDelegationInFileystem` adds a check that the current credentials
in the DT cannot be used to access data on other buckets —that is, the active
session really is restricted to the target bucket.


#### Integration Test `ITestDelegatedMRJob`

It's not easy to bring up a YARN cluster with a secure HDFS and miniKDC controller in
test cases —this test, the closest there is to an end-to-end test,
uses mocking to mock the RPC calls to the YARN AM, and then verifies that the tokens
have been collected in the job context,

#### Load Test `ILoadTestSessionCredentials`

This attempts to collect many, many delegation tokens simultaneously and sees
what happens.

Worth doing if you have a new authentication service provider, or
implementing custom DT support.
Consider also something for going from DT to
AWS credentials if this is also implemented by your own service.
This is left as an exercise for the developer.

**Tip**: don't go overboard here, especially against AWS itself.
