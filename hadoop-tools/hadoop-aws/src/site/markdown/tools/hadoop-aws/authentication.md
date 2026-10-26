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

# Authenticating with S3

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

Except when interacting with public S3 buckets, the S3A client
needs the credentials needed to interact with buckets.

The client supports multiple authentication mechanisms and can be configured as to
which mechanisms to use, and their order of use. Custom implementations
of `com.amazonaws.auth.AWSCredentialsProvider` may also be used.
However, with the upgrade to AWS Java SDK V2 in Hadoop 3.4.0, these classes will need to be
updated to implement `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`.
For more information see [Upcoming upgrade to AWS Java SDK V2](./aws_sdk_upgrade.html).

### Authentication properties

```xml
<property>
  <name>fs.s3a.access.key</name>
  <description>AWS access key ID used by S3A file system. Omit for IAM role-based or provider-based authentication.</description>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <description>AWS secret key used by S3A file system. Omit for IAM role-based or provider-based authentication.</description>
</property>

<property>
  <name>fs.s3a.session.token</name>
  <description>Session token, when using org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
    as one of the providers.
  </description>
</property>

<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>
    org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,
    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
    software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider,
    org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
  </value>
  <description>
    Comma-separated class names of credential provider classes which implement
    software.amazon.awssdk.auth.credentials.AwsCredentialsProvider.

    org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider is not included in
    the chain by default.

    When S3A delegation tokens are not enabled, this list will be used
    to directly authenticate with S3 and other AWS services.
    When S3A Delegation tokens are enabled, depending upon the delegation
    token binding it may be used
    to communicate wih the STS endpoint to request session/role
    credentials.
  </description>
</property>

<property>
  <name>fs.s3a.aws.credentials.provider.mapping</name>
  <description>
    Comma-separated key-value pairs of mapped credential providers that are
    separated by equal operator (=). The key can be used by
    fs.s3a.aws.credentials.provider config, and it will be translated into
    the specified value of credential provider class based on the key-value
    pair provided by this config.

    Example:
    com.amazonaws.auth.AnonymousAWSCredentials=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider,
    com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider,
    com.amazonaws.auth.InstanceProfileCredentialsProvider=org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider

    With the above key-value pairs, if fs.s3a.aws.credentials.provider specifies
    com.amazonaws.auth.AnonymousAWSCredentials, it will be remapped to
    org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider by S3A while
    preparing AWS credential provider list for any S3 access.
    We can use the same credentials provider list for both v1 and v2 SDK clients.
  </description>
</property>
```

### <a name="auth_env_vars"></a> Authenticating via the AWS Environment Variables

S3A supports configuration via [the standard AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment).

The core environment variables are for the access key and associated secret:

```bash
export AWS_ACCESS_KEY_ID=my.aws.key
export AWS_SECRET_ACCESS_KEY=my.secret.key
```

If the environment variable `AWS_SESSION_TOKEN` is set, session authentication
using "Temporary Security Credentials" is enabled; the Key ID and secret key
must be set to the credentials for that specific session.

```bash
export AWS_SESSION_TOKEN=SECRET-SESSION-TOKEN
export AWS_ACCESS_KEY_ID=SESSION-ACCESS-KEY
export AWS_SECRET_ACCESS_KEY=SESSION-SECRET-KEY
```

These environment variables can be used to set the authentication credentials
instead of properties in the Hadoop configuration.

*Important:*
These environment variables are generally not propagated from client to server when
YARN applications are launched. That is: having the AWS environment variables
set when an application is launched will not permit the launched application
to access S3 resources. The environment variables must (somehow) be set
on the hosts/processes where the work is executed.

### <a name="auth_providers"></a> Changing Authentication Providers

The standard way to authenticate is with an access key and secret key set in
the Hadoop configuration files.

By default, the S3A client follows the following authentication chain:

1. The options `fs.s3a.access.key`, `fs.s3a.secret.key` and `fs.s3a.sesson.key`
   are looked for in the Hadoop XML configuration/Hadoop credential providers,
   returning a set of session credentials if all three are defined.
1. The `fs.s3a.access.key` and `fs.s3a.secret.key` are looked for in the Hadoop
   XML configuration//Hadoop credential providers, returning a set of long-lived
   credentials if they are defined.
1. The [AWS environment variables](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-environment),
   are then looked for: these will return session or full credentials depending
   on which values are set.
1. An attempt is made to query the Amazon EC2 Instance/k8s container Metadata Service to
   retrieve credentials published to EC2 VMs.

S3A can be configured to obtain client authentication providers from classes
which integrate with the AWS SDK by implementing the
`software.amazon.awssdk.auth.credentials.AwsCredentialsProvider`
interface.
This is done by listing the implementation classes, in order of
preference, in the configuration option `fs.s3a.aws.credentials.provider`.
In previous hadoop releases, providers were required to
implement the AWS V1 SDK interface `com.amazonaws.auth.AWSCredentialsProvider`.
Consult the [Upgrading S3A to AWS SDK V2](./aws_sdk_upgrade.html) documentation
to see how to migrate credential providers.

*Important*: AWS Credential Providers are distinct from _Hadoop Credential Providers_.
As will be covered later, Hadoop Credential Providers allow passwords and other secrets
to be stored and transferred more securely than in XML configuration files.
AWS Credential Providers are classes which can be used by the Amazon AWS SDK to
obtain an AWS login from a different source in the system, including environment
variables, JVM properties and configuration files.

All Hadoop `fs.s3a.` options used to store login details can all be secured
in [Hadoop credential providers](../../../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html);
this is advised as a more secure way to store valuable secrets.

There are a number of AWS Credential Providers inside the `hadoop-aws` JAR:

| Hadoop module credential provider                              | Authentication Mechanism                         |
|----------------------------------------------------------------|--------------------------------------------------|
| `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`     | Session Credentials in configuration             |
| `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`        | Simple name/secret credentials in configuration  |
| `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider`     | Anonymous Login                                  |
| `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider`  | [Assumed Role credentials](./assumed_roles.html) |
| `org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider` | EC2/k8s instance credentials                     |
| `org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider`  | Session Credentials in profile file              |


There are also many in the Amazon SDKs, with the common ones being as follows

| classname                                                                        | description                  |
|----------------------------------------------------------------------------------|------------------------------|
| `software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider` | AWS Environment Variables    |
| `software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider`     | EC2 Metadata Credentials     |
| `software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider`           | EC2/k8s Metadata Credentials |
| `software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`| K8s Metadata Credentials     |



### <a name="auth_iam"></a> EC2 IAM Metadata Authentication with `InstanceProfileCredentialsProvider`

Applications running in EC2 may associate an IAM role with the VM and query the
[EC2 Instance Metadata Service](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)
for credentials to access S3.  Within the AWS SDK, this functionality is
provided by `InstanceProfileCredentialsProvider`, which internally enforces a
singleton instance in order to prevent throttling problem.

### <a name="auth_named_profile"></a> Using Named Profile Credentials with `ProfileCredentialsProvider`

You can configure Hadoop to authenticate to AWS using a [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html).

To authenticate with a named profile:

1. Declare `software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider` as the provider.
1. Set your profile via the `AWS_PROFILE` environment variable.
1. Due to a [bug in version 1 of the AWS Java SDK](https://github.com/aws/aws-sdk-java/issues/803),
   you'll need to remove the `profile` prefix from the AWS configuration section heading.

   Here's an example of what your AWS configuration files should look like:

    ```
    $ cat ~/.aws/config
    [user1]
    region = us-east-1
    $ cat ~/.aws/credentials
    [user1]
    aws_access_key_id = ...
    aws_secret_access_key = ...
    aws_session_token = ...
    aws_security_token = ...
    ```
Note:

1. The `region` setting is only used if `fs.s3a.endpoint.region` is set to the empty string.
1. For the credentials to be available to applications running in a Hadoop cluster, the
   configuration files MUST be in the `~/.aws/` directory on the local filesystem in
   all hosts in the cluster.

### <a name="auth_simple"></a> Credentials from profile with `ProfileAWSCredentialsProvider`*

This is a non-default provider that fetches credentials from a profile file,
acting as a Hadoop wrapper around [ProfileCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/ProfileCredentialsProvider.html). The profile file and
profile name are both resolved as follows.

1. If the configuration setting is specified, that takes priority ( `fs.s3a.auth.profile.file`
   for profile file and `fs.s3a.auth.profile.name` for profile name).
2. If a configuration setting is absent, but the environment variable for
   the setting( `AWS_SHARED_CREDENTIALS_FILE` for profile file and `AWS_PROFILE` for
   profile name) is defined, then the variable is used.
3. If neither configuration setting nor environment variable is present, then
   the values default to `~/.aws/credentials` for the profile file, and `default`
   for the profile name.


*Important*: This profile file must be on every node in the _cluster_.
If this is not the case, delegation tokens can be used to collect the current credentials and propagate them.

### <a name="auth_session"></a> Using Session Credentials with `TemporaryAWSCredentialsProvider`

[Temporary Security Credentials](http://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html)
can be obtained from the Amazon Security Token Service; these
consist of an access key, a secret key, and a session token.

To authenticate with these:

1. Declare `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider` as the
   provider.
1. Set the session key in the property `fs.s3a.session.token`,
   and the access and secret key properties to those of this temporary session.

Example:

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider</value>
</property>

<property>
  <name>fs.s3a.access.key</name>
  <value>SESSION-ACCESS-KEY</value>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <value>SESSION-SECRET-KEY</value>
</property>

<property>
  <name>fs.s3a.session.token</name>
  <value>SECRET-SESSION-TOKEN</value>
</property>
```

The lifetime of session credentials are fixed when the credentials
are issued; once they expire the application will no longer be able to
authenticate to AWS.

### <a name="auth_anon"></a> Anonymous Login with `AnonymousAWSCredentialsProvider`

Specifying `org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider` allows
anonymous access to a publicly accessible S3 bucket without any credentials.
It can be useful for accessing public data sets without requiring AWS credentials.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider</value>
</property>
```

Once this is done, there's no need to supply any credentials
in the Hadoop configuration or via environment variables.

This option can be used to verify that an object store does
not permit unauthenticated access: that is, if an attempt to list
a bucket is made using the anonymous credentials, it should fail —unless
explicitly opened up for broader access.

```bash
hadoop fs -ls \
 -D fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
 s3a://noaa-isd-pds/
```

1. Allowing anonymous access to an S3 bucket compromises
   security and therefore is unsuitable for most use cases.

1. If a list of credential providers is given in `fs.s3a.aws.credentials.provider`,
   then the Anonymous Credential provider *must* come last. If not, credential
   providers listed after it will be ignored.

### <a name="auth_simple"></a> Simple name/secret credentials with `SimpleAWSCredentialsProvider`*

This is the standard credential provider, which supports the secret
key in `fs.s3a.access.key` and token in `fs.s3a.secret.key`
values.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
</property>
```

This is the basic authenticator used in the default authentication chain.

This means that the default S3A authentication chain can be defined as

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>
    org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,
    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,
    software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
    org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider
  </value>
</property>
```

## <a name="auth_security"></a> Protecting the AWS Credentials

It is critical that you never share or leak your AWS credentials.
Loss of credentials can leak/lose all your data, run up large bills,
and significantly damage your organisation.

1. Never share your secrets.

1. Never commit your secrets into an SCM repository.
   The [git secrets](https://github.com/awslabs/git-secrets) can help here.

1. Never include AWS credentials in bug reports, files attached to them,
   or similar.

1. If you use the `AWS_` environment variables,  your list of environment variables
   is equally sensitive.

1. Never use root credentials.
   Use IAM user accounts, with each user/application having its own set of credentials.

1. Use IAM permissions to restrict the permissions individual users and applications
   have. This is best done through roles, rather than configuring individual users.

1. Avoid passing in secrets to Hadoop applications/commands on the command line.
   The command line of any launched program is visible to all users on a Unix system
   (via `ps`), and preserved in command histories.

1. Explore using [IAM Assumed Roles](assumed_roles.html) for role-based permissions
   management: a specific S3A connection can be made with a different assumed role
   and permissions from the primary user account.

1. Consider a workflow in which users and applications are issued with short-lived
   session credentials, configuring S3A to use these through
   the `TemporaryAWSCredentialsProvider`.

1. Have a secure process in place for cancelling and re-issuing credentials for
   users and applications. Test it regularly by using it to refresh credentials.

1. In installations where Kerberos is enabled, [S3A Delegation Tokens](delegation_tokens.html)
   can be used to acquire short-lived session/role credentials and then pass them
   into the shared application. This can ensure that the long-lived secrets stay
   on the local system.

When running in EC2, the IAM EC2 instance credential provider will automatically
obtain the credentials needed to access AWS services in the role the EC2 VM
was deployed as.
This AWS credential provider is enabled in S3A by default.

## Custom AWS Credential Providers and Apache Spark

Apache Spark employs two class loaders, one that loads "distribution" (Spark + Hadoop) classes and one that
loads custom user classes. If the user wants to load custom implementations of AWS credential providers,
custom signers, delegation token providers or any other dynamically loaded extension class
through user provided jars she will need to set the following configuration:

```xml
<property>
  <name>fs.s3a.classloader.isolation</name>
  <value>false</value>
</property>
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>CustomCredentialsProvider</value>
</property>
```

If the following property is not set or set to `true`, the following exception will be thrown:

```
java.io.IOException: From option fs.s3a.aws.credentials.provider java.lang.ClassNotFoundException: Class CustomCredentialsProvider not found
```

## <a name="auth_s3_access_grants"></a> S3 Authorization Using S3 Access Grants

[S3 Access Grants](https://aws.amazon.com/s3/features/access-grants/) can be used to grant accesses to S3 data using IAM Principals.
In order to enable S3 Access Grants, S3A utilizes the
[S3 Access Grants plugin](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2) on all S3 clients,
which is found within the AWS Java SDK bundle (v2.23.19+).

S3A supports both cross-region access (by default) and the
[fallback-to-IAM configuration](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2?tab=readme-ov-file#using-the-plugin)
which allows S3A to fallback to using the IAM role (and its permission sets directly) to access S3 data in the case that S3 Access Grants
is unable to authorize the S3 call.

The following is how to enable this feature:

```xml
<property>
  <name>fs.s3a.s3accessgrants.enabled</name>
  <value>true</value>
</property>
<property>
  <!--Optional: Defaults to False-->
  <name>fs.s3a.s3accessgrants.fallback.to.iam</name>
  <value>true</value>
</property>
```

Note:
1. S3A only enables the [S3 Access Grants plugin](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2) on the S3 clients
as part of this feature. Any usage issues or bug reporting should be done directly at the plugin's
[GitHub repo](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2/issues).

For more details on using S3 Access Grants, please refer to
[Managing access with S3 Access Grants](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants.html).
