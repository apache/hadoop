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

# S3A Delegation Token Architecture

This is an architecture document to accompany
[Working with Delegation Tokens](delegation_tokens.html)

## Background: Delegation Tokens

Delegation Tokens, "DTs" are a common feature of Hadoop Services.
They are opaque byte arrays which can be issued by services like
HDFS, HBase, YARN, and which can be used to authenticate a request with
that service.

### Tokens are Issued

In a Kerberized cluster, they are issued by the service after the caller
has authenticated, and so that principal is trusted to be who they say they are.
The issued DT can therefore attest that whoever is including that token
on a request is authorized to act on behalf of that principal —for the
specific set of operations which the DT grants.

As an example, an HDFS DT can be requested by a user, included in the
launch context of a YARN application -say DistCp, and that launched application
can then talk to HDFS as if they were that user.

### Tokens are marshalled

Tokens are opaque byte arrays. They are contained within a `Token<T extends TokenIdentifier>`
 class which includes an expiry time, the service identifier, and some other details.

`Token<>` instances can be serialized as a Hadoop Writable, or converted saved to/from a protobuf
format. This is how they are included in YARN application and container requests,
and elsewhere. They can even be saved to files through the `hadoop dt` command.

### Tokens can be unmarshalled


At the far end, tokens can be unmarshalled and converted into instances of
the java classes. This assumes that all the dependent classes are on the
classpath, obviously.

### Tokens can be used to authenticate callers

The Hadoop RPC layer and the web SPNEGO layer support tokens.

### Tokens can be renewed

DTs can be renewed by the specific principal declared at creation time as
"the renewer". In the example above, the YARN Resource Manager's principal
can be declared as the reviewer. Then, even while a token is attached
to a queued launch request in the RM, the RM can regularly request of HDFS
that the token is renewed.

There's an ultimate limit on how long tokens can be renewed for, but its
generally 72h or similar, so that medium-life jobs can access services
and data on behalf of a user.

### Tokens can be Revoked

When tokens are no longer needed, the service can be told to revoke a token.
Continuing the YARN example, after an application finishes the YARN RM
can revoke every token marshalled into the application launch request.
At which point there's no risk associated with that token being
compromised.


*This is all how "real" Hadoop tokens work*

The S3A Delegation Tokens are subtly different.

The S3A DTs actually include the AWS credentials within the token
data marshalled and shared across the cluster. The credentials can be one
of:

* The Full AWS (`fs.s3a.access.key`, `fs.s3a.secret.key`) login.
* A set of AWS session credentials
  (`fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.session.token`).

These credentials are obtained from the AWS Secure Token Service (STS) when the the token is issued.
* A set of AWS session credentials binding the user to a specific AWS IAM Role,
further restricted to only access the S3 bucket and matching S3Guard DynamoDB table.
Again, these credentials are requested when the token is issued.


*Tokens can be issued*

When an S3A Filesystem instance is asked to issue a token it can simply package
up the login secrets (The "Full" tokens), or talk to the AWS STS service
to get a set of session/assumed role credentials. These are marshalled within
the overall token, and then onwards to applications.

*Tokens can be marshalled*

The AWS secrets are held in a subclass of `org.apache.hadoop.security.token.TokenIdentifier`.
This class gets serialized to a byte array when the whole token is marshalled, and deserialized
when the token is loaded.

*Tokens can be used to authenticate callers*

The S3A FS does not hand the token to AWS services to authenticate the caller.
Instead it takes the AWS credentials included in the token identifier
and uses them to sign the requests.

*Tokens cannot be renewed*

The tokens contain the credentials; you cant use them to ask AWS for more.

For full credentials that is moot, but for the session and role credentials,
they will expire. At which point the application will be unable to
talk to the AWS infrastructure.

*Tokens cannot be revoked*

The AWS STS APIs don't let you revoke a single set of session credentials.

## Background: How Tokens are collected in MapReduce jobs


### `org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal()`

1. Calls `org.apache.hadoop.mapreduce.security.TokenCache.obtainTokensForNamenodes()`
for the job submission dir on the cluster FS (i.e. `fs.defaultFS`).
1. Reads in the property `mapreduce.job.hdfs-servers` and extracts DTs from them,
1. Tells the `FileInputFormat` and `FileOutputFormat` subclasses of the job
to collect their source and dest FS tokens.

All token collection is via `TokenCache.obtainTokensForNamenodes()`

### `TokenCache.obtainTokensForNamenodes(Credentials, Path[], Configuration) `

1. Returns immediately if security is off.
1. Retrieves all the filesystems in the list of paths.
1. Retrieves a token from each unless it is in the list of filesystems in `mapreduce.job.hdfs-servers.token-renewal.exclude`
1. Merges in any DTs stored in the file referenced under: `mapreduce.job.credentials.binary`
1. Calls `FileSystem.collectDelegationTokens()`, which, if there isn't any token already in the credential list, issues and adds a new token. *There is no check to see if that existing credential has expired*.


### `FileInputFormat.listStatus(JobConf job): FileStatus[]`

Enumerates source paths in (`mapreduce.input.fileinputformat.inputdir`) ; uses `TokenCache.obtainTokensForNamenodes()`
to collate a token for all of these paths.

This operation is called by the public interface method `FileInputFormat.getSplits()`.

### `FileOutputFormat.checkOutputSpecs()`

Calls `getOutputPath(job)` and asks for the DTs of that output path FS.


## Architecture of the S3A Delegation Token Support



1. The S3A FS client has the ability to be configured with a delegation
token binding, the "DT Binding", a class declared in the option `fs.s3a.delegation.token.binding`.
1. If set, when a filesystem is instantiated it asks the DT binding for its list of AWS credential providers.
(the list in `fs.s3a.aws.credentials.provider` are only used if the DT binding wishes to).
1. The DT binding scans for the current principal (`UGI.getCurrentUser()`/"the Owner") to see if they
have any token in their credential cache whose service name matches the URI of the filesystem.
1. If one is found, it is unmarshalled and then used to authenticate the caller via
some AWS Credential provider returned to the S3A FileSystem instance.
1. If none is found, the Filesystem is considered to have been deployed "Unbonded".
The DT binding has to return a list of the AWS credential providers to use.

When requests are made of AWS services, the created credential provider(s) are
used to sign requests.

When the filesystem is asked for a delegation token, the
DT binding will generate a token identifier containing the marshalled tokens.

If the Filesystem was deployed with a DT, that is, it was deployed "bonded", that existing
DT is returned.

If it was deployed unbonded, the DT Binding is asked to create a new DT.

It is up to the binding what it includes in the token identifier, and how it obtains them.
This new token identifier is included in a token which has a "canonical service name" of
the URI of the filesystem (e.g "s3a://landsat-pds").

The issued/reissued token identifier can be marshalled and reused.


### class `org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens`

This joins up the S3A Filesystem with the pluggable DT binding classes.

One is instantiated in the S3A Filesystem instance if a DT Binding class
has been instantiated. If so, it is invoked for

* Building up the authentication chain during filesystem initialization.
* Determining if the FS should declare that it has a canonical name
(in `getCanonicalServiceName()`).
* When asked for a DT (in `getDelegationToken(String renewer)`).

The `S3ADelegationTokens` has the task of instantiating the actual DT binding,
which must be a subclass of `AbstractDelegationTokenBinding`.

All the DT bindings, and `S3ADelegationTokens` itself are subclasses of
`org.apache.hadoop.service.AbstractService`; they follow the YARN service lifecycle
of: create -> init -> start -> stop. This means that a DT binding, may, if it chooses,
start worker threads when the service is started (`serviceStart()`); it must
then stop them in the `serviceStop` method. (Anyone doing this must be aware
that the owner FS is not fully initialized in serviceStart: they must not
call into the Filesystem).

The actions of this class are

* Lookup of DTs associated with this S3A FS (scanning credentials, unmarshalling).
* initiating the DT binding in bound/unbound state.
* issuing DTs, either serving up the existing one, or requesting the DT Binding for
a new instance of `AbstractS3ATokenIdentifier` and then wrapping a hadoop token
around it.
* General logging, debugging, and metrics. Delegation token metrics are
collected in (`S3AInstrumentation.DelegationTokenStatistics`)




### class `org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier`

All tokens returned are a subclass of `AbstractS3ATokenIdentifier`.

This class contains the following fields:

```java
  /** Canonical URI of the bucket. */
  private URI uri;

  /**
   * Encryption secrets to also marshall with any credentials.
   * Set during creation to ensure it is never null.
   */
  private EncryptionSecrets encryptionSecrets = new EncryptionSecrets();

  /**
   * Timestamp of creation.
   * This is set to the current time; it will be overridden when
   * deserializing data.
   */
  private long created = System.currentTimeMillis();

  /**
   * An origin string for diagnostics.
   */
  private String origin = "";

  /**
   * This marshalled UUID can be used in testing to verify transmission,
   * and reuse; as it is printed you can see what is happending too.
   */
  private String uuid = UUID.randomUUID().toString();
```

The `uuid` field is used for equality tests and debugging; the `origin` and
`created` fields are also for diagnostics.

The `encryptionSecrets` structure enumerates the AWS encryption mechanism
of the filesystem instance, and any declared key. This allows
the client-side secret for SSE-C encryption to be passed to the filesystem,
or the key name for SSE-KMS.

*The encryption settings and secrets of the S3A filesystem on the client
are included in the DT, so can be used to encrypt/decrypt data in the cluster.*

### class `SessionTokenIdentifier` extends `AbstractS3ATokenIdentifier`

This holds session tokens, and it also gets used as a superclass of
the other token identifiers.

It adds a set of `MarshalledCredentials` containing the session secrets.

Every token/token identifier must have a unique *Kind*; this is how token
identifier deserializers are looked up. For Session Credentials, it is
`S3ADelegationToken/Session`. Subclasses *must* have a different token kind,
else the unmarshalling and binding mechanism will fail.


### classes `RoleTokenIdentifier` and `FullCredentialsTokenIdentifier`

These are subclasses of `SessionTokenIdentifier` with different token kinds,
needed for that token unmarshalling.

Their kinds are `S3ADelegationToken/Role` and `S3ADelegationToken/Full`
respectively.

Having different possible token bindings raises the risk that a job is submitted
with one binding and yet the cluster is expecting another binding.
Provided the configuration option `fs.s3a.delegation.token.binding` is not
marked as final in the `core-site.xml` file, the value of that binding
set in the job should propagate with the binding: the choice of provider
is automatic. A cluster can even mix bindings across jobs.
However if a core-site XML file declares a specific binding for a single bucket and
the job only had the generic `fs.s3a.delegation.token.binding`` binding,
then there will be a mismatch.
Each binding must be rigorous about checking the Kind of any found delegation
token and failing meaningfully here.



### class `MarshalledCredentials`

Can marshall a set of AWS credentials (access key, secret key, session token)
as a Hadoop Writable.

These can be given to an instance of class `MarshalledCredentialProvider`
and used to sign AWS RPC/REST API calls.

## DT Binding: `AbstractDelegationTokenBinding`

The plugin point for this design is the DT binding, which must be a subclass
of `org.apache.hadoop.fs.s3a.auth.delegation.AbstractDelegationTokenBinding`.


This class

* Returns the *Kind* of these tokens.
* declares whether tokens will actually  be issued or not (the TokenIssuingPolicy).
* can issue a DT in

```java
  public abstract AWSCredentialProviderList deployUnbonded()
      throws IOException;
```

The S3A FS has been brought up with DTs enabled, but none have been found
for its service name. The DT binding is tasked with coming up with the
fallback list of AWS credential providers.

```java
public abstract AWSCredentialProviderList bindToTokenIdentifier(
    AbstractS3ATokenIdentifier retrievedIdentifier)
    throws IOException;
```

A DT for this FS instance been found. Bind to it and extract enough information
to authenticate with AWS. Return the list of providers to use.

```java
public abstract AbstractS3ATokenIdentifier createEmptyIdentifier();
```

Return an empty identifier.


```java
public abstract AbstractS3ATokenIdentifier createTokenIdentifier(
      Optional<RoleModel.Policy> policy,
      EncryptionSecrets encryptionSecrets)
```

This is the big one: creatw a new Token Identifier for this filesystem, one
which must include the encryption secrets, and which may make use of
the role policy.

## Token issuing

### How Full Delegation Tokens are issued.

If the client is only logged in with session credentials: fail.

Else: take the AWS access/secret key, store them in the MarshalledCredentials
in a new `FullCredentialsTokenIdentifier`, and return.


### How Session Delegation Tokens are issued.

If the client is only logged in with session credentials: return these.

This is taken from the Yahoo! patch: if a user is logged
in with a set of session credentials (including those from some 2FA login),
they just get wrapped up and passed in.

There's no clue as to how long they will last, so there's a warning printed.

If there is a full set of credentials, then an `SessionTokenBinding.maybeInitSTS()`
creates an STS client set up to communicate with the (configured) STS endpoint,
retrying with the same retry policy as the filesystem.

This client is then used to request a set of session credentials.

### How Role Delegation Tokens are issued.

If the client is only logged in with session credentials: fail.

We don't know whether this is a full user session or some role session,
and rather than pass in some potentially more powerful secrets with the job,
just fail.

Else: as with session delegation tokens, an STS client is created. This time
`assumeRole()` is invoked with the ARN of the role and an extra AWS role policy
set to restrict access to:

* CRUD access the specific bucket a token is being requested for
* CRUD access to the contents of any S3Guard DDB used (not admin rights though).
* access to all KMS keys (assumption: AWS KMS is where restrictions are set up)

*Example Generated Role Policy*


```json
{
  "Version" : "2012-10-17",
  "Statement" : [ {
    "Sid" : "7",
    "Effect" : "Allow",
    "Action" : [ "s3:GetBucketLocation", "s3:ListBucket*" ],
    "Resource" : "arn:aws:s3:::example-bucket"
  }, {
    "Sid" : "8",
    "Effect" : "Allow",
    "Action" : [ "s3:Get*", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload" ],
    "Resource" : "arn:aws:s3:::example-bucket/*"
  }, {
    "Sid" : "1",
    "Effect" : "Allow",
    "Action" : [ "kms:Decrypt", "kms:GenerateDataKey" ],
    "Resource" : "arn:aws:kms:*"
  }, {
    "Sid" : "9",
    "Effect" : "Allow",
    "Action" : [ "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem", "dynamodb:DeleteItem", "dynamodb:DescribeTable", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query", "dynamodb:UpdateItem" ],
    "Resource" : "arn:aws:dynamodb:eu-west-1:980678866fff:table/example-bucket"
  } ]
}
```

These permissions are sufficient for all operations the S3A client currently
performs on a bucket. If those requirements are expanded, these policies
may change.


## Testing.

Look in `org.apache.hadoop.fs.s3a.auth.delegation`


It's proven impossible to generate a full end-to-end test in an MR job.

1. MapReduce only collects DTs when kerberos is enabled in the cluster.
1. A Kerberized MiniYARN cluster refuses to start on a local file:// fs without the
native libraries, so it can set directory permissions.
1. A Kerberized MiniHDFS cluster and MiniYARN cluster refuse to talk to each
other reliably, at least in the week or so of trying.

The `ITestDelegatedMRJob` test works around this by using Mockito to mock
the actual YARN job submit operation in `org.apache.hadoop.mapreduce.protocol.ClientProtocol`.
The MR code does all the work of collecting tokens and attaching them to
the launch context, "submits" the job, which then immediately succeeds.
The job context is examined to verify that the source and destination filesystem
DTs were extracted.

To test beyond this requires a real Kerberized cluster, or someone able to fix
up Mini* clusters to run kerberized.
