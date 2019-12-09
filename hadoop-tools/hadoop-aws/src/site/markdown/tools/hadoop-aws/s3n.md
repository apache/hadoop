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

# The S3N Client

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

S3N was a Hadoop filesystem client which can read or write data stored
in Amazon S3. It uses URLs with the schema `s3n://`.

- - -

**Hadoop's S3N client for Amazon S3 has been superceded by
the S3A connector**

**Please upgrade to S3A for a supported, higher-performance S3 Client**

- - -


## <a name="migrating"></a> How to migrate to to the S3A client

1. Keep the `hadoop-aws` JAR on your classpath.

1. Add the `aws-java-sdk-bundle.jar` JAR which Hadoop ships
with to your classpath.

1. Change the authentication keys

    | old key | new key |
    |---------|---------|
    | `fs.s3n.awsAccessKeyId` | `fs.s3a.access.key` |
    | `fs.s3n.awsSecretAccessKey` | `fs.s3a.secret.key` |

    Do make sure the property names are correct. For S3A, they are
    `fs.s3a.access.key` and `fs.s3a.secret.key` —you cannot just copy the S3N
    properties and replace `s3n` with `s3a`.

1. Replace URLs which began with `s3n://` with `s3a://`

1. You may now remove the `jets3t` JAR, as it is no longer needed.
