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
For example, the [transfer manager for SDKV2](https://aws.amazon.com/blogs/developer/introducing-amazon-s3-transfer-manager-in-the-aws-sdk-for-java-2-x/)
is built using java bindings of the AWS Common Runtime S3
client (https://github.com/awslabs/aws-crt-java) (CRT).
CRT is a set of packages written in C, designed for maximising performance when interacting with AWS
services such as S3.
- New features such as [additional checksum algorithms](https://aws.amazon.com/blogs/aws/new-additional-checksum-algorithms-for-amazon-s3/)
which S3A will benefit from are not available in SDKV1.

## What's changing?

The [SDK V2](https://github.com/aws/aws-sdk-java-v2) for S3 is very different from
[SDK V1](https://github.com/aws/aws-sdk-java), and brings breaking changes for S3A.
A complete list of the changes can be found in the [Changelog](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#41-s3-changes).

The major changes and how this affects S3A are listed below.

### Package Change

Package names have changed, all classes in SDK V2 are under `software.amazon.awssdk`, SDK V1 classes
were under `com.amazonaws`.

### Credential Providers

- Interface change: [com.amazonaws.auth.AWSCredentialsProvider](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/AWSCredentialsProvider.java)
has been replaced by [software.amazon.awssdk.auth.credentials.AwsCredentialsProvider](https://github.com/aws/aws-sdk-java-v2/blob/master/core/auth/src/main/java/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.java).
- Credential provider class changes: the package and class names of credential providers have
changed.

The change in interface will mean that custom credential providers will need to be updated to now
implement `AwsCredentialsProvider` instead of `AWSCredentialProvider`.

Due to change in class names, references to SDK V1 credential providers
in `fs.s3a.aws.credentials.provider` will need to be updated to reference V2 providers.

### Delegation Tokens

Custom credential providers used in delegation token binding classes will also need to be updated.

### AmazonS3 replaced by S3Client

The s3 client is an instance of `S3Client` in V2 rather than `AmazonS3`.

For this reason, the `S3ClientFactory` will be deprecated and replaced by one that creates a V2
`S3Client`.

The `getAmazonS3ClientForTesting()` method will also be updated to return the `S3Client`.

### Signers

Interface change: [com.amazonaws.auth.Signer](https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/auth/Signer.java)
has been replaced by [software.amazon.awssdk.core.signer.Signer](https://github.com/aws/aws-sdk-java-v2/blob/master/core/sdk-core/src/main/java/software/amazon/awssdk/core/signer/Signer.java).

The change in signers will mean the custom signers will need to be updated to implement the new
interface.
