
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

# Working with Encrypted S3 Data

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->


## <a name="introduction"></a> Introduction

The S3A filesystem client supports Amazon S3's Server Side Encryption
for at-rest data encryption.
You should to read up on the [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html)
for S3 Server Side Encryption for up to date information on the encryption mechanisms.



When configuring an encryption method in the `core-site.xml`, this will apply cluster wide.
Any new file written will be encrypted with this encryption configuration.
When the S3A client reads a file, S3 will attempt to decrypt it using the mechanism
and keys with which the file was encrypted.

* It is **NOT** advised to mix and match encryption types in a bucket
* It is much simpler and safer to encrypt with just one type and key per bucket.
* You can use AWS bucket policies to mandate encryption rules for a bucket.
* You can use S3A per-bucket configuration to ensure that S3A clients use encryption
policies consistent with the mandated rules.
* You can use S3 Default Encryption to encrypt data without needing to
set anything in the client.
* Changing the encryption options on the client does not change how existing
files were encrypted, except when the files are renamed.
* For all mechanisms other than SSE-C, clients do not need any configuration
options set in order to read encrypted data: it is all automatically handled
in S3 itself.

## <a name="encryption_types"></a>How data is encrypted

AWS S3 supports server-side encryption inside the storage system itself.
When an S3 client uploading data requests data to be encrypted, then an encryption key is used
to encrypt the data as it saved to S3. It remains encrypted on S3 until deleted:
clients cannot change the encryption attributes of an object once uploaded.

The Amazon AWS SDK also offers client-side encryption, in which all the encoding
and decoding of data is performed on the client. This is *not* supported by
the S3A client.

The server-side "SSE" encryption is performed with symmetric AES256 encryption;
S3 offers different mechanisms for actually defining the key to use.


There are four key management mechanisms, which in order of simplicity of use,
are:

* S3 Default Encryption
* SSE-S3: an AES256 key is generated in S3, and saved alongside the data.
* SSE-KMS: an AES256 key is generated in S3, and encrypted with a secret key provided
by Amazon's Key Management Service, a key referenced by name in the uploading client.
* SSE-C : the client specifies an actual base64 encoded AES-256 key to be used
to encrypt and decrypt the data.


## <a name="sse-s3"></a> S3 Default Encryption

This feature allows the administrators of the AWS account to set the "default"
encryption policy on a bucket -the encryption to use if the client does
not explicitly declare an encryption algorithm.

[S3 Default Encryption for S3 Buckets](https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html)

This supports SSE-S3 and SSE-KMS.

There is no need to set anything up in the client: do it in the AWS console.


## <a name="sse-s3"></a> SSE-S3 Amazon S3-Managed Encryption Keys

In SSE-S3, all keys and secrets are managed inside S3. This is the simplest encryption mechanism.
There is no extra cost for storing data with this option.


### Enabling SSE-S3

To write S3-SSE encrypted files, the value of
`fs.s3a.server-side-encryption-algorithm` must be set to that of
the encryption mechanism used in `core-site`; currently only `AES256` is supported.

```xml
<property>
  <name>fs.s3a.server-side-encryption-algorithm</name>
  <value>AES256</value>
</property>
```

Once set, all new data will be stored encrypted. There is no need to set this property when downloading data — the data will be automatically decrypted when read using
the Amazon S3-managed key.

To learn more, refer to
[Protecting Data Using Server-Side Encryption with Amazon S3-Managed Encryption Keys (SSE-S3) in AWS documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html).


### <a name="sse-kms"></a> SSE-KMS: Amazon S3-KMS Managed Encryption Keys


Amazon offers a pay-per-use key management service, [AWS KMS](https://aws.amazon.com/documentation/kms/).
This service can be used to encrypt data on S3 by defining "customer master keys", CMKs,
which can be centrally managed and assigned to specific roles and IAM accounts.

The AWS KMS [can be used encrypt data on S3uploaded data](http://docs.aws.amazon.com/kms/latest/developerguide/services-s3.html).

> The AWS KMS service is **not** related to the Key Management Service built into Hadoop (*Hadoop KMS*). The *Hadoop KMS* primarily focuses on
  managing keys for *HDFS Transparent Encryption*. Similarly, HDFS encryption is unrelated to S3 data encryption.

When uploading data encrypted with SSE-KMS, the sequence is as follows.

1. The S3A client must declare a specific CMK in the property `fs.s3a.server-side-encryption.key`, or leave
it blank to use the default configured for that region.

1. The S3A client uploads all the data as normal, now including encryption information.

1. The S3 service encrypts the data with a symmetric key unique to the new object.

1. The S3 service retrieves the chosen CMK key from the KMS service, and, if the user has
the right to use it, uses it to encrypt the object-specific key.


When downloading SSE-KMS encrypted data, the sequence is as follows

1. The S3A client issues an HTTP GET request to read the data.
1. S3 sees that the data was encrypted with SSE-KMS, and looks up the specific key in the KMS service
1. If and only if the requesting user has been granted permission to use the CMS key does
the KMS service provide S3 with the key.
1. As a result, S3 will only decode the data if the user has been granted access to the key.


KMS keys can be managed by an organization's administrators in AWS, including
having access permissions assigned and removed from specific users, groups, and IAM roles.
Only those "principals" with granted rights to a key may access it,
hence only they may encrypt data with the key, *and decrypt data encrypted with it*.
This allows KMS to be used to provide a cryptographically secure access control mechanism for data stores on S3.


Each KMS server is region specific, and accordingly, so is each CMK configured.
A CMK defined in one region cannot be used with an S3 bucket in a different region.


Notes

* Callers are charged for every use of a key, both for encrypting the data in uploads
  and for decrypting it when reading it back.
* Random-access IO on files may result in multiple GET requests of an object during a read
sequence (especially for columnar data), so may require more than one key retrieval to process a single file,
* The KMS service is throttled: too many requests may cause requests to fail.
* As well as incurring charges, heavy I/O *may* reach IO limits for a customer. If those limits are reached,
they can be increased through the AWS console.


### Enabling SSE-KMS

To enable SSE-KMS, the property `fs.s3a.server-side-encryption-algorithm` must be set to `SSE-KMS` in `core-site`:

```xml
<property>
  <name>fs.s3a.server-side-encryption-algorithm</name>
  <value>SSE-KMS</value>
</property>
```

The ID of the specific key used to encrypt the data should also be set in the property `fs.s3a.server-side-encryption.key`:

```xml
<property>
  <name>fs.s3a.server-side-encryption.key</name>
  <value>arn:aws:kms:us-west-2:360379543683:key/071a86ff-8881-4ba0-9230-95af6d01ca01</value>
</property>
```

Organizations may define a default key in the Amazon KMS; if a default key is set,
then it will be used whenever SSE-KMS encryption is chosen and the value of `fs.s3a.server-side-encryption.key` is empty.

### the S3A `fs.s3a.encryption.key` key only affects created files

With SSE-KMS, the S3A client option `fs.s3a.server-side-encryption.key` sets the
key to be used when new files are created. When reading files, this key,
and indeed the value of `fs.s3a.server-side-encryption-algorithme` is ignored:
S3 will attempt to retrieve the key and decrypt the file based on the create-time settings.

This means that

* There's no need to configure any client simply reading data.
* It is possible for a client to read data encrypted with one KMS key, and
write it with another.


## <a name="sse-c"></a> SSE-C: Server side encryption with a client-supplied key.

In SSE-C, the client supplies the secret key needed to read and write data.
Every client trying to read or write data must be configured with the same
secret key.


SSE-C integration with Hadoop is still stabilizing; issues related to it are still surfacing.
It is already clear that SSE-C with a common key <b>must</b> be used exclusively within
a bucket if it is to be used at all. This is the only way to ensure that path and
directory listings do not fail with "Bad Request" errors.

### Enabling SSE-C

To use SSE-C, the configuration option `fs.s3a.server-side-encryption-algorithm`
must be set to `SSE-C`, and a base-64 encoding of the key placed in
`fs.s3a.server-side-encryption.key`.

```xml
<property>
  <name>fs.s3a.server-side-encryption-algorithm</name>
  <value>SSE-C</value>
</property>

<property>
  <name>fs.s3a.server-side-encryption.key</name>
  <value>SGVscCwgSSdtIHRyYXBwZWQgaW5zaWRlIGEgYmFzZS02NC1jb2RlYyE=</value>
</property>
```

All clients must share this same key.

### The `fs.s3a.encryption.key` value is used to read and write data

With SSE-C, the S3A client option `fs.s3a.server-side-encryption.key` sets the
key to be used for both reading *and* writing data.

When reading any file written with SSE-C, the same key must be set
in the property `fs.s3a.server-side-encryption.key`.

This is unlike SSE-S3 and SSE-KMS, where the information needed to
decode data is kept in AWS infrastructure.


### SSE-C Warning

You need to fully understand how SSE-C works in the S3
environment before using this encryption type.  Please refer to the Server Side
Encryption documentation available from AWS.  SSE-C is only recommended for
advanced users with advanced encryption use cases.  Failure to properly manage
encryption keys can cause data loss.  Currently, the AWS S3 API(and thus S3A)
only supports one encryption key and cannot support decrypting objects during
moves under a previous key to a new destination.  It is **NOT** advised to use
multiple encryption keys in a bucket, and is recommended to use one key per
bucket and to not change this key.  This is due to when a request is made to S3,
the actual encryption key must be provided to decrypt the object and access the
metadata.  Since only one encryption key can be provided at a time, S3A will not
pass the correct encryption key to decrypt the data.


## <a name="best_practises"></a> Encryption best practises


### <a name="bucket_policy"></a> Mandate encryption through policies

Because it is up to the clients to enable encryption on new objects, all clients
must be correctly configured in order to guarantee that data is encrypted.


To mandate that all data uploaded to a bucket is encrypted,
you can set a [bucket policy](https://aws.amazon.com/blogs/security/how-to-prevent-uploads-of-unencrypted-objects-to-amazon-s3/)
declaring that clients must provide encryption information with all data uploaded.


* Mandating an encryption mechanism on newly uploaded data does not encrypt existing data; existing data will retain whatever encryption (if any) applied at the time of creation*

Here is a policy to mandate `SSE-S3/AES265` encryption on all data uploaded to a bucket. This covers uploads as well as the copy operations which take place when file/directory rename operations are mimicked.


```json
{
  "Version": "2012-10-17",
  "Id": "EncryptionPolicy",
  "Statement": [
    {
      "Sid": "RequireEncryptionHeaderOnPut",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::BUCKET/*",
      "Condition": {
        "Null": {
          "s3:x-amz-server-side-encryption": true
        }
      }
    },
    {
      "Sid": "RequireAESEncryptionOnPut",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::BUCKET/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
      }
    }
  ]
}
```

To use SSE-KMS, a different restriction must be defined:


```json
{
  "Version": "2012-10-17",
  "Id": "EncryptionPolicy",
  "Statement": [
    {
      "Sid": "RequireEncryptionHeaderOnPut",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::BUCKET/*",
      "Condition": {
        "Null": {
          "s3:x-amz-server-side-encryption": true
        }
      }
    },
    {
      "Sid": "RequireKMSEncryptionOnPut",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::BUCKET/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "SSE-KMS"
        }
      }
    }
  ]
}
```

To use one of these policies:

1. Replace `BUCKET` with the specific name of the bucket being secured.
1. Locate the bucket in the AWS console [S3 section](https://console.aws.amazon.com/s3/home).
1. Select the "Permissions" tab.
1. Select the "Bucket Policy" tab in the permissions section.
1. Paste the edited policy into the form.
1. Save the policy.

### <a name="per_bucket_config"></a> Use S3a per-bucket configuration to control encryption settings

In an organisation which has embraced S3 encryption, different buckets inevitably have
different encryption policies, such as different keys for SSE-KMS encryption.
In particular, as different keys need to be named for different regions, unless
you rely on the administrator-managed "default" key for each S3 region, you
will need unique keys.

S3A's per-bucket configuration enables this.


Here, for example, are settings for a bucket in London, `london-stats`:


```xml
<property>
  <name>fs.s3a.bucket.london-stats.server-side-encryption-algorithm</name>
  <value>AES256</value>
</property>
```

This requests SSE-S; if matched with a bucket policy then all data will
be encrypted as it is uploaded.


A different bucket can use a different policy
(here SSE-KMS) and, when necessary, declare a key.

Here is an example bucket in S3 Ireland, which uses SSE-KMS and
a KMS key hosted in the AWS-KMS service in the same region.


```xml
<property>
  <name>fs.s3a.bucket.ireland-dev.server-side-encryption-algorithm</name>
  <value>SSE-KMS</value>
</property>

<property>
  <name>fs.s3a.bucket.ireland-dev.server-side-encryption.key</name>
  <value>arn:aws:kms:eu-west-1:98067faff834c:key/071a86ff-8881-4ba0-9230-95af6d01ca01</value>
</property>

```

Again the appropriate bucket policy can be used to guarantee that all callers
will use SSE-KMS; they can even mandate the name of the key used to encrypt
the data, so guaranteeing that access to thee data can be read by everyone
granted access to that key, and nobody without access to it.


###<a name="changing-encryption"></a> Use rename() to encrypt files with new keys

The encryption of an object is set when it is uploaded. If you want to encrypt
an unencrypted file, or change the SEE-KMS key of a file, the only way to do
so is by copying the object.

How can you do that from Hadoop? With `rename()`.

The S3A client mimics a real filesystem's' rename operation by copying all the
source files to the destination paths, then deleting the old ones.

Note: this does not work for SSE-C, because you cannot set a different key
for reading as for writing, and you must supply that key for reading. There
you need to copy one bucket to a different bucket, one with a different key.
Use `distCp`for this, with per-bucket encryption policies.


## <a name="troubleshooting"></a> Troubleshooting Encryption

The [troubleshooting](./troubleshooting_s3a.html) document covers
stack traces which may surface when working with encrypted data.
