---
title: "Transparent Data Encryption"
date: "2019-April-03"
summary: TDE allows data on the disks to be encrypted-at-rest and automatically decrypted during access. You can enable this per key or per bucket.
weight: 3
icon: lock
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Transparent Data Encryption
Ozone TDE setup process and usage are very similar to HDFS TDE.
The major difference is that Ozone TDE is enabled at Ozone bucket level
when a bucket is created.

### Setting up the Key Management Server

To use TDE, clients must setup a Key Management server and provide that URI to
Ozone/HDFS. Since Ozone and HDFS can use the same Key Management Server, this
 configuration can be provided via *hdfs-site.xml*.

Property| Value
-----------------------------------|-----------------------------------------
hadoop.security.key.provider.path  | KMS uri. e.g. kms://http@kms-host:9600/kms

### Using Transparent Data Encryption
If this is already configured for your cluster, then you can simply proceed
to create the encryption key and enable encrypted buckets.

To create an encrypted bucket, client need to:

   * Create a bucket encryption key with hadoop key CLI, which is similar to
  how you would use HDFS encryption zones.

  ```bash
  hadoop key create encKey
  ```
  The above command creates an encryption key for the bucket you want to protect.
  Once the key is created, you can tell Ozone to use that key when you are
  reading and writing data into a bucket.

   * Assign the encryption key to a bucket.

  ```bash
  ozone sh bucket create -k encKey /vol/encryptedBucket
  ```

After this command, all data written to the _encryptedBucket_ will be encrypted
via the encKey and while reading the clients will talk to Key Management
Server and read the key and decrypt it. In other words, the data stored
inside Ozone is always encrypted. The fact that data is encrypted at rest
will be completely transparent to the clients and end users.
