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

# Working with IAM Assumed Roles

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

AWS ["IAM Assumed Roles"](http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
allows applications to change the AWS role with which to authenticate with AWS services.
The assumed roles can have different rights from the main user login.

The S3A connector supports assumed roles for authentication with AWS.
A full set of login credentials must be provided, which will be used
to obtain the assumed role and refresh it regularly.
By using per-filesystem configuration, it is possible to use different
assumed roles for different buckets.

## Using IAM Assumed Roles

### Before You Begin

This document assumes you know about IAM Assumed roles, what they
are, how to configure their policies, etc.

* You need a role to assume, and know its "ARN".
* You need a pair of long-lived IAM User credentials, not the root account set.
* Have the AWS CLI installed, and test that it works there.
* Give the role access to S3, and, if using S3Guard, to DynamoDB.


Trying to learn how IAM Assumed Roles work by debugging stack traces from
the S3A client is "suboptimal".

### <a name="how_it_works"></a> How the S3A connector support IAM Assumed Roles.

To use assumed roles, the client must be configured to use the
*Assumed Role Credential Provider*, `org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider`,
in the configuration option `fs.s3a.aws.credentials.provider`.

This AWS Credential provider will read in the `fs.s3a.assumed.role` options needed to connect to the
Session Token Service [Assumed Role API](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html),
first authenticating with the full credentials, then assuming the specific role
specified. It will then refresh this login at the configured rate of
`fs.s3a.assumed.role.session.duration`

To authenticate with the STS service both for the initial credential retrieval
and for background refreshes, a different credential provider must be
created, one which uses long-lived credentials (secret keys, environment variables).
Short lived credentials (e.g other session tokens, EC2 instance credentials) cannot be used.

A list of providers can be set in `s.s3a.assumed.role.credentials.provider`;
if unset the standard `BasicAWSCredentialsProvider` credential provider is used,
which uses `fs.s3a.access.key` and `fs.s3a.secret.key`.

Note: although you can list other AWS credential providers in  to the
Assumed Role Credential Provider, it can only cause confusion.

### <a name="using"></a> Using Assumed Roles

To use assumed roles, the S3A client credentials provider must be set to
the `AssumedRoleCredentialProvider`, and `fs.s3a.assumed.role.arn` to
the previously created ARN.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider</value>
</property>

<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value>arn:aws:iam::90066806600238:role/s3-restricted</value>
</property>
```

The STS service itself needs the caller to be authenticated, *which can
only be done with a set of long-lived credentials*.
This means the normal `fs.s3a.access.key` and `fs.s3a.secret.key`
pair, environment variables, or some other supplier of long-lived secrets.

The default is the `fs.s3a.access.key` and `fs.s3a.secret.key` pair.
If you wish to use a different authentication mechanism, set it in the property
`fs.s3a.assumed.role.credentials.provider`.

```xml
<property>
  <name>fs.s3a.assumed.role.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
</property>
```

Requirements for long-lived credentials notwithstanding, this option takes the
same values as `fs.s3a.aws.credentials.provider`.

The safest way to manage AWS secrets is via
[Hadoop Credential Providers](index.html#hadoop_credential_providers).

### <a name="configuration"></a>Assumed Role Configuration Options

Here are the full set of configuration options.

```xml
<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value />
  <description>
    AWS ARN for the role to be assumed.
    Requires the fs.s3a.aws.credentials.provider list to contain
    org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.session.name</name>
  <value />
  <description>
    Session name for the assumed role, must be valid characters according to
    the AWS APIs.
    If not set, one is generated from the current Hadoop/Kerberos username.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.session.duration</name>
  <value>30m</value>
  <description>
    Duration of assumed roles before a refresh is attempted.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.policy</name>
  <value/>
  <description>
    Extra policy containing more restrictions to apply to the role.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.sts.endpoint</name>
  <value/>
  <description>
    AWS Simple Token Service Endpoint. If unset, uses the default endpoint.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.credentials.provider</name>
  <value/>
  <description>
    Credential providers used to authenticate with the STS endpoint and retrieve
    the role tokens.
    If unset, uses "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".
  </description>
</property>
```

## <a name="troubleshooting"></a> Troubleshooting Assumed Roles

1. Make sure the role works and the user trying to enter it can do so from AWS
the command line before trying to use the S3A client.
1. Try to access the S3 bucket with reads and writes from the AWS CLI.
1. Then, with the hadoop settings updated, try to read data from the `hadoop fs` CLI:
`hadoop fs -ls -p s3a://bucket/`
1. Then, with the hadoop CLI, try to create a new directory with a request such as
`hadoop fs -mkdirs -p s3a://bucket/path/p1/`

### <a name="no_role"></a>IOException: "Unset property fs.s3a.assumed.role.arn"

The Assumed Role Credential Provider is enabled, but `fs.s3a.assumed.role.arn` is unset.

```
java.io.IOException: Unset property fs.s3a.assumed.role.arn
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:76)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
```

### <a name="not_authorized_for_assumed_role"></a>"Not authorized to perform sts:AssumeRole"

This can arise if the role ARN set in `fs.s3a.assumed.role.arn` is invalid
or one to which the caller has no access.

```
java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider
 on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
  Not authorized to perform sts:AssumeRole (Service: AWSSecurityTokenService; Status Code: 403;
   Error Code: AccessDenied; Request ID: aad4e59a-f4b0-11e7-8c78-f36aaa9457f6):AccessDenied
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
```

### <a name="root_account"></a> "Roles may not be assumed by root accounts"

You can't use assume a role with the root acount of an AWS account;
you need to create a new user and give it the permission to change into
the role.

```
java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider
 on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    Roles may not be assumed by root accounts. (Service: AWSSecurityTokenService; Status Code: 403; Error Code: AccessDenied;
    Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2):
    No AWS Credentials provided by AssumedRoleCredentialProvider :
    com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    Roles may not be assumed by root accounts. (Service: AWSSecurityTokenService;
     Status Code: 403; Error Code: AccessDenied; Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
  ... 22 more
Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
 Roles may not be assumed by root accounts.
  (Service: AWSSecurityTokenService; Status Code: 403; Error Code: AccessDenied;
   Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
```

### <a name="invalid_duration"></a> "Assume Role session duration should be in the range of 15min - 1Hr"

The value of `fs.s3a.assumed.role.session.duration` is out of range.

```
java.lang.IllegalArgumentException: Assume Role session duration should be in the range of 15min - 1Hr
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$Builder.withRoleSessionDurationSeconds(STSAssumeRoleSessionCredentialsProvider.java:437)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:86)
```


### <a name="malformed_policy"></a> `MalformedPolicyDocumentException` "The policy is not in the valid JSON format"


The policy set in `fs.s3a.assumed.role.policy` is not valid according to the
AWS specification of Role Policies.

```
rg.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
  The policy is not in the valid JSON format. (Service: AWSSecurityTokenService; Status Code: 400;
   Error Code: MalformedPolicyDocument; Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c):
   MalformedPolicyDocument: The policy is not in the valid JSON format.
   (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
    Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
Caused by: com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
 The policy is not in the valid JSON format.
  (Service: AWSSecurityTokenService; Status Code: 400;
   Error Code: MalformedPolicyDocument; Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 19 more
```

### <a name="malformed_policy"></a> `MalformedPolicyDocumentException` "Syntax errors in policy"

The policy set in `fs.s3a.assumed.role.policy` is not valid JSON.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
  Syntax errors in policy. (Service: AWSSecurityTokenService;
  Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45):
  MalformedPolicyDocument: Syntax errors in policy.
  (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
 (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 19 more
```

### <a name="recursive_auth"></a> `IOException`: "AssumedRoleCredentialProvider cannot be in fs.s3a.assumed.role.credentials.provider"

You can't use the Assumed Role Credential Provider as the provider in
`fs.s3a.assumed.role.credentials.provider`.

```
java.io.IOException: AssumedRoleCredentialProvider cannot be in fs.s3a.assumed.role.credentials.provider
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:86)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
```

### <a name="invalid_keypair"></a> `AWSBadRequestException`: "not a valid key=value pair"


There's an space or other typo in the `fs.s3a.access.key` or `fs.s3a.secret.key` values used for the
inner authentication which is breaking signature creation.

```
 org.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider
  on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
   'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign) in Authorization header:
    'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date.
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code:
    IncompleteSignature; Request ID: c4a8841d-f556-11e7-99f9-af005a829416):IncompleteSignature:
    'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign)
    in Authorization header: 'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date,
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: IncompleteSignature;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)

Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign)
    in Authorization header: 'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date,
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: IncompleteSignature;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 25 more
```

### <a name="invalid_token"></a> `AccessDeniedException/InvalidClientTokenId`: "The security token included in the request is invalid"

The credentials used to authenticate with the AWS Simple Token Service are invalid.

```
[ERROR] Failures:
[ERROR] java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
  The security token included in the request is invalid.
  (Service: AWSSecurityTokenService; Status Code: 403; Error Code: InvalidClientTokenId;
   Request ID: 74aa7f8a-f557-11e7-850c-33d05b3658d7):InvalidClientTokenId
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)

Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
The security token included in the request is invalid.
 (Service: AWSSecurityTokenService; Status Code: 403; Error Code: InvalidClientTokenId;
 Request ID: 74aa7f8a-f557-11e7-850c-33d05b3658d7)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 25 more
```

### <a name="invalid_session"></a> `AWSSecurityTokenServiceExceptiond`: "Member must satisfy regular expression pattern: `[\w+=,.@-]*`"


The session name, as set in `fs.s3a.assumed.role.session.name` must match the wildcard `[\w+=,.@-]*`.

If the property is unset, it is extracted from the current username and then sanitized to
match these constraints.
If set explicitly, it must be valid.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider on
    com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
    Request ID: 7c437acb-f55d-11e7-9ad8-3b5e4f701c20):ValidationError:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
  at org.apache.hadoop.fs.s3a.ITestAssumeRole.lambda$expectFileSystemFailure$0(ITestAssumeRole.java:70)
  at org.apache.hadoop.fs.s3a.ITestAssumeRole.lambda$interceptC$1(ITestAssumeRole.java:84)
  at org.apache.hadoop.test.LambdaTestUtils.intercept(LambdaTestUtils.java:491)
  at org.apache.hadoop.test.LambdaTestUtils.intercept(LambdaTestUtils.java:377)
  at org.apache.hadoop.test.LambdaTestUtils.intercept(LambdaTestUtils.java:446)
  at org.apache.hadoop.fs.s3a.ITestAssumeRole.interceptC(ITestAssumeRole.java:82)
  at org.apache.hadoop.fs.s3a.ITestAssumeRole.expectFileSystemFailure(ITestAssumeRole.java:68)
  at org.apache.hadoop.fs.s3a.ITestAssumeRole.testAssumeRoleBadSession(ITestAssumeRole.java:216)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:498)
  at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:47)
  at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
  at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:44)
  at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
  at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)
  at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
  at org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55)
  at org.junit.internal.runners.statements.FailOnTimeout$StatementThread.run(FailOnTimeout.java:74)
Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint:
    Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:135)
  at org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:124)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 26 more
```
