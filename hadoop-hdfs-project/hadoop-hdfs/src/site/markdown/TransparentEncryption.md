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

Transparent Encryption in HDFS
==============================

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

<a name="Overview"></a>Overview
--------

HDFS implements *transparent*, *end-to-end* encryption. Once configured, data read from and written to special HDFS directories is *transparently* encrypted and decrypted without requiring changes to user application code. This encryption is also *end-to-end*, which means the data can only be encrypted and decrypted by the client. HDFS never stores or has access to unencrypted data or unencrypted data encryption keys. This satisfies two typical requirements for encryption: *at-rest encryption* (meaning data on persistent media, such as a disk) as well as *in-transit encryption* (e.g. when data is travelling over the network).

<a name="Background"></a>Background
----------

Encryption can be done at different layers in a traditional data management software/hardware stack. Choosing to encrypt at a given layer comes with different advantages and disadvantages.

* **Application-level encryption**. This is the most secure and most flexible approach. The application has ultimate control over what is encrypted and can precisely reflect the requirements of the user. However, writing applications to do this is hard. This is also not an option for customers of existing applications that do not support encryption.

* **Database-level encryption**. Similar to application-level encryption in terms of its properties. Most database vendors offer some form of encryption. However, there can be performance issues. One example is that indexes cannot be encrypted.

* **Filesystem-level encryption**. This option offers high performance, application transparency, and is typically easy to deploy. However, it is unable to model some application-level policies. For instance, multi-tenant applications might want to encrypt based on the end user. A database might want different encryption settings for each column stored within a single file.

* **Disk-level encryption**. Easy to deploy and high performance, but also quite inflexible. Only really protects against physical theft.

HDFS-level encryption fits between database-level and filesystem-level encryption in this stack. This has a lot of positive effects. HDFS encryption is able to provide good performance and existing Hadoop applications are able to run transparently on encrypted data. HDFS also has more context than traditional filesystems when it comes to making policy decisions.

HDFS-level encryption also prevents attacks at the filesystem-level and below (so-called "OS-level attacks"). The operating system and disk only interact with encrypted bytes, since the data is already encrypted by HDFS.

<a name="Use_Cases"></a>Use Cases
---------

Data encryption is required by a number of different government, financial, and regulatory entities. For example, the health-care industry has HIPAA regulations, the card payment industry has PCI DSS regulations, and the US government has FISMA regulations. Having transparent encryption built into HDFS makes it easier for organizations to comply with these regulations.

Encryption can also be performed at the application-level, but by integrating it into HDFS, existing applications can operate on encrypted data without changes. This integrated architecture implies stronger encrypted file semantics and better coordination with other HDFS functions.

<a name="Architecture"></a>Architecture
------------

### <a name="Architecture_overview"></a>Overview

For transparent encryption, we introduce a new abstraction to HDFS: the *encryption zone*. An encryption zone is a special directory whose contents will be transparently encrypted upon write and transparently decrypted upon read. Each encryption zone is associated with a single *encryption zone key* which is specified when the zone is created. Each file within an encryption zone has its own unique *data encryption key (DEK)*. DEKs are never handled directly by HDFS. Instead, HDFS only ever handles an *encrypted data encryption key (EDEK)*. Clients decrypt an EDEK, and then use the subsequent DEK to read and write data. HDFS datanodes simply see a stream of encrypted bytes.

A very important use case of encryption is to "switch it on" and ensure all files across the entire filesystem are encrypted. To support this strong guarantee without losing the flexibility of using different encryption zone keys in different parts of the filesystem, HDFS allows *nested encryption zones*. After an encryption zone is created (e.g. on the root directory `/`), a user can create more encryption zones on its descendant directories (e.g. `/home/alice`) with different keys. The EDEK of a file will generated using the encryption zone key from the closest ancestor encryption zone.

A new cluster service is required to manage encryption keys: the Hadoop Key Management Server (KMS). In the context of HDFS encryption, the KMS performs three basic responsibilities:

1.  Providing access to stored encryption zone keys

2.  Generating new encrypted data encryption keys for storage on the NameNode

3.  Decrypting encrypted data encryption keys for use by HDFS clients

The KMS will be described in more detail below.

### <a name="Accessing_data_within_an_encryption_zone"></a>Accessing data within an encryption zone

When creating a new file in an encryption zone, the NameNode asks the KMS to generate a new EDEK encrypted with the encryption zone's key. The EDEK is then stored persistently as part of the file's metadata on the NameNode.

When reading a file within an encryption zone, the NameNode provides the client with the file's EDEK and the encryption zone key version used to encrypt the EDEK. The client then asks the KMS to decrypt the EDEK, which involves checking that the client has permission to access the encryption zone key version. Assuming that is successful, the client uses the DEK to decrypt the file's contents.

All of the above steps for the read and write path happen automatically through interactions between the DFSClient, the NameNode, and the KMS.

Access to encrypted file data and metadata is controlled by normal HDFS filesystem permissions. This means that if HDFS is compromised (for example, by gaining unauthorized access to an HDFS superuser account), a malicious user only gains access to ciphertext and encrypted keys. However, since access to encryption zone keys is controlled by a separate set of permissions on the KMS and key store, this does not pose a security threat.

### <a name="Key_Management_Server_KeyProvider_EDEKs"></a>Key Management Server, KeyProvider, EDEKs

The KMS is a proxy that interfaces with a backing key store on behalf of HDFS daemons and clients. Both the backing key store and the KMS implement the Hadoop KeyProvider API. See the [KMS documentation](../../hadoop-kms/index.html) for more information.

In the KeyProvider API, each encryption key has a unique *key name*. Because keys can be rolled, a key can have multiple *key versions*, where each key version has its own *key material* (the actual secret bytes used during encryption and decryption). An encryption key can be fetched by either its key name, returning the latest version of the key, or by a specific key version.

The KMS implements additional functionality which enables creation and decryption of *encrypted encryption keys (EEKs)*. Creation and decryption of EEKs happens entirely on the KMS. Importantly, the client requesting creation or decryption of an EEK never handles the EEK's encryption key. To create a new EEK, the KMS generates a new random key, encrypts it with the specified key, and returns the EEK to the client. To decrypt an EEK, the KMS checks that the user has access to the encryption key, uses it to decrypt the EEK, and returns the decrypted encryption key.

In the context of HDFS encryption, EEKs are *encrypted data encryption keys (EDEKs)*, where a *data encryption key (DEK)* is what is used to encrypt and decrypt file data. Typically, the key store is configured to only allow end users access to the keys used to encrypt DEKs. This means that EDEKs can be safely stored and handled by HDFS, since the HDFS user will not have access to unencrypted encryption keys.

<a name="Configuration"></a>Configuration
-------------

A necessary prerequisite is an instance of the KMS, as well as a backing key store for the KMS. See the [KMS documentation](../../hadoop-kms/index.html) for more information.

Once a KMS has been set up and the NameNode and HDFS clients have been correctly configured, an admin can use the `hadoop key` and `hdfs crypto` command-line tools to create encryption keys and set up new encryption zones. Existing data can be encrypted by copying it into the new encryption zones using tools like distcp.

### <a name="Configuring_the_cluster_KeyProvider"></a>Configuring the cluster KeyProvider

#### hadoop.security.key.provider.path

The KeyProvider to use when interacting with encryption keys used when reading and writing to an encryption zone.
HDFS clients will use the provider path returned from Namenode via getServerDefaults. If namenode doesn't support returning key provider uri then client's conf will be used.

### <a name="Selecting_an_encryption_algorithm_and_codec"></a>Selecting an encryption algorithm and codec

#### hadoop.security.crypto.codec.classes.EXAMPLECIPHERSUITE

The prefix for a given crypto codec, contains a comma-separated list of implementation classes for a given crypto codec (eg EXAMPLECIPHERSUITE). The first implementation will be used if available, others are fallbacks.

#### hadoop.security.crypto.codec.classes.aes.ctr.nopadding

Default: `org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec, org.apache.hadoop.crypto.JceAesCtrCryptoCodec`

Comma-separated list of crypto codec implementations for AES/CTR/NoPadding. The first implementation will be used if available, others are fallbacks.

#### hadoop.security.crypto.cipher.suite

Default: `AES/CTR/NoPadding`

Cipher suite for crypto codec.

#### hadoop.security.crypto.jce.provider

Default: None

The JCE provider name used in CryptoCodec.

#### hadoop.security.crypto.buffer.size

Default: `8192`

The buffer size used by CryptoInputStream and CryptoOutputStream.

### <a name="Namenode_configuration"></a>Namenode configuration

#### dfs.namenode.list.encryption.zones.num.responses

Default: `100`

When listing encryption zones, the maximum number of zones that will be returned in a batch. Fetching the list incrementally in batches improves namenode performance.

<a name="crypto_command-line_interface"></a>`crypto` command-line interface
-------------------------------

### <a name="createZone"></a>createZone

Usage: `[-createZone -keyName <keyName> -path <path>]`

Create a new encryption zone.

| | |
|:---- |:---- |
| *path* | The path of the encryption zone to create. It must be an empty directory. A trash directory is provisioned under this path.|
| *keyName* | Name of the key to use for the encryption zone. Uppercase key names are unsupported. |

### <a name="listZones"></a>listZones

Usage: `[-listZones]`

List all encryption zones. Requires superuser permissions.

### <a name="provisionTrash"></a>provisionTrash

Usage: `[-provisionTrash -path <path>]`

Provision a trash directory for an encryption zone.

| | |
|:---- |:---- |
| *path* | The path to the root of the encryption zone. |

### <a name="getFileEncryptionInfo"></a>getFileEncryptionInfo

Usage: `[-getFileEncryptionInfo -path <path>]`

Get encryption information from a file. This can be used to find out whether a file is being encrypted, and the key name / key version used to encrypt it.

| | |
|:---- |:---- |
| *path* | The path of the file to get encryption information. |

### <a name="reencryptZone"></a>reencryptZone

Usage: `[-reencryptZone <action> -path <zone>]`

Re-encrypts an encryption zone, by iterating through the encryption zone, and calling the KeyProvider's reencryptEncryptedKeys interface to batch-re-encrypt all files' EDEKs with the latest version encryption zone key in the key provider. Requires superuser permissions.

Note that re-encryption does not apply to snapshots, due to snapshots' immutable nature.

| | |
|:---- |:---- |
| *action* | The re-encrypt action to perform. Must be either `-start` or `-cancel`. |
| *path* | The path to the root of the encryption zone. |

Re-encryption is a NameNode-only operation in HDFS, so could potentially put intensive load to the NameNode. The following configurations can be changed to control the stress on the NameNode, depending on the acceptable throughput impact to the cluster.

| | |
|:---- |:---- |
| *dfs.namenode.reencrypt.batch.size* | The number of EDEKs in a batch to be sent to the KMS for re-encryption. Each batch is processed when holding the name system read/write lock, with throttling happening between batches. See configs below. |
| *dfs.namenode.reencrypt.throttle.limit.handler.ratio* | Ratio of read locks to be held during re-encryption. 1.0 means no throttling. 0.5 means re-encryption can hold the readlock at most 50% of its total processing time. Negative value or 0 are invalid. |
| *dfs.namenode.reencrypt.throttle.limit.updater.ratio* | Ratio of write locks to be held during re-encryption. 1.0 means no throttling. 0.5 means re-encryption can hold the writelock at most 50% of its total processing time. Negative value or 0 are invalid. |

### <a name="listReencryptionStatus"></a>listReencryptionStatus

Usage: `[-listReencryptionStatus]`

List re-encryption information for all encryption zones. Requires superuser permissions.

<a name="Example_usage"></a>Example usage
-------------

These instructions assume that you are running as the normal user or HDFS superuser as is appropriate. Use `sudo` as needed for your environment.

    # As the normal user, create a new encryption key
    hadoop key create mykey

    # As the super user, create a new empty directory and make it an encryption zone
    hadoop fs -mkdir /zone
    hdfs crypto -createZone -keyName mykey -path /zone

    # chown it to the normal user
    hadoop fs -chown myuser:myuser /zone

    # As the normal user, put a file in, read it out
    hadoop fs -put helloWorld /zone
    hadoop fs -cat /zone/helloWorld

    # As the normal user, get encryption information from the file
    hdfs crypto -getFileEncryptionInfo -path /zone/helloWorld
    # console output: {cipherSuite: {name: AES/CTR/NoPadding, algorithmBlockSize: 16}, cryptoProtocolVersion: CryptoProtocolVersion{description='Encryption zones', version=1, unknownValue=null}, edek: 2010d301afbd43b58f10737ce4e93b39, iv: ade2293db2bab1a2e337f91361304cb3, keyName: mykey, ezKeyVersionName: mykey@0}

<a name="Distcp_considerations"></a>Distcp considerations
---------------------

### <a name="Running_as_the_superuser"></a>Running as the superuser

One common usecase for distcp is to replicate data between clusters for backup and disaster recovery purposes. This is typically performed by the cluster administrator, who is an HDFS superuser.

To enable this same workflow when using HDFS encryption, we introduced a new virtual path prefix, `/.reserved/raw/`, that gives superusers direct access to the underlying block data in the filesystem. This allows superusers to distcp data without needing having access to encryption keys, and also avoids the overhead of decrypting and re-encrypting data. It also means the source and destination data will be byte-for-byte identical, which would not be true if the data was being re-encrypted with a new EDEK.

When using `/.reserved/raw` to distcp encrypted data, it's important to preserve extended attributes with the [-px](../../hadoop-distcp/DistCp.html#Command_Line_Options) flag. This is because encrypted file attributes (such as the EDEK) are exposed through extended attributes within `/.reserved/raw`, and must be preserved to be able to decrypt the file. This means that if the distcp is initiated at or above the encryption zone root, it will automatically create an encryption zone at the destination if it does not already exist. However, it's still recommended that the admin first create identical encryption zones on the destination cluster to avoid any potential mishaps.

### <a name="Copying_into_encrypted_locations"></a>Copying into encrypted locations

By default, distcp compares checksums provided by the filesystem to verify that the data was successfully copied to the destination. When copying from unencrypted or encrypted location into an encrypted location, the filesystem checksums will not match since the underlying block data is different because a new EDEK will be used to encrypt at destination. In this case, specify the [-skipcrccheck](../../hadoop-distcp/DistCp.html#Command_Line_Options) and [-update](../../hadoop-distcp/DistCp.html#Command_Line_Options) distcp flags to avoid verifying checksums.

<a name="Rename_and_Trash_considerations"></a>Rename and Trash considerations
---------------------

HDFS restricts file and directory renames across encryption zone boundaries. This includes renaming an encrypted file / directory into an unencrypted directory (e.g., `hdfs dfs mv /zone/encryptedFile /home/bob`), renaming an unencrypted file or directory into an encryption zone (e.g., `hdfs dfs mv /home/bob/unEncryptedFile /zone`), and renaming between two different encryption zones (e.g., `hdfs dfs mv /home/alice/zone1/foo /home/alice/zone2`). In these examples, `/zone`, `/home/alice/zone1`, and `/home/alice/zone2` are encryption zones, while `/home/bob` is not. A rename is only allowed if the source and destination paths are in the same encryption zone, or both paths are unencrypted (not in any encryption zone).

This restriction enhances security and eases system management significantly. All file EDEKs under an encryption zone are encrypted with the encryption zone key. Therefore, if the encryption zone key is compromised, it is important to identify all vulnerable files and re-encrypt them. This is fundamentally difficult if a file initially created in an encryption zone can be renamed to an arbitrary location in the filesystem.

To comply with the above rule, each encryption zone has its own `.Trash` directory under the "zone directory". E.g., after `hdfs dfs rm /zone/encryptedFile`, `encryptedFile` will be moved to `/zone/.Trash`, instead of the `.Trash` directory under the user's home directory. When the entire encryption zone is deleted, the "zone directory" will be moved to the `.Trash` directory under the user's home directory.

If the encryption zone is the root directory (e.g., `/` directory), the trash path of root directory is `/.Trash`, not the `.Trash` directory under the user's home directory, and the behavior of renaming sub-directories or sub-files in root directory will keep consistent with the behavior in a general encryption zone, such as `/zone` which is mentioned at the top of this section.

The `crypto` command before Hadoop 2.8.0 does not provision the `.Trash` directory automatically. If an encryption zone is created before Hadoop 2.8.0, and then the cluster is upgraded to Hadoop 2.8.0 or above, the trash directory can be provisioned using `-provisionTrash` option (e.g., `hdfs crypto -provisionTrash -path /zone`).
<a name="Attack_vectors"></a>Attack vectors
--------------

### <a name="Hardware_access_exploits"></a>Hardware access exploits

These exploits assume that attacker has gained physical access to hard drives from cluster machines, i.e. datanodes and namenodes.

1.  Access to swap files of processes containing data encryption keys.

    * By itself, this does not expose cleartext, as it also requires access to encrypted block files.

    * This can be mitigated by disabling swap, using encrypted swap, or using mlock to prevent keys from being swapped out.

2.  Access to encrypted block files.

    * By itself, this does not expose cleartext, as it also requires access to DEKs.

### <a name="Root_access_exploits"></a>Root access exploits

These exploits assume that attacker has gained root shell access to cluster machines, i.e. datanodes and namenodes. Many of these exploits cannot be addressed in HDFS, since a malicious root user has access to the in-memory state of processes holding encryption keys and cleartext. For these exploits, the only mitigation technique is carefully restricting and monitoring root shell access.

1.  Access to encrypted block files.

    * By itself, this does not expose cleartext, as it also requires access to encryption keys.

2.  Dump memory of client processes to obtain DEKs, delegation tokens, cleartext.

    * No mitigation.

3.  Recording network traffic to sniff encryption keys and encrypted data in transit.

    * By itself, insufficient to read cleartext without the EDEK encryption key.

4.  Dump memory of datanode process to obtain encrypted block data.

    * By itself, insufficient to read cleartext without the DEK.

5.  Dump memory of namenode process to obtain encrypted data encryption keys.

    * By itself, insufficient to read cleartext without the EDEK's encryption key and encrypted block files.

### <a name="HDFS_admin_exploits"></a>HDFS admin exploits

These exploits assume that the attacker has compromised HDFS, but does not have root or `hdfs` user shell access.

1.  Access to encrypted block files.

    * By itself, insufficient to read cleartext without the EDEK and EDEK encryption key.

2.  Access to encryption zone and encrypted file metadata (including encrypted data encryption keys), via -fetchImage.

    * By itself, insufficient to read cleartext without EDEK encryption keys.

### <a name="Rogue_user_exploits"></a>Rogue user exploits

A rogue user can collect keys of files they have access to, and use them later to decrypt the encrypted data of those files. As the user had access to those files, they already had access to the file contents. This can be mitigated through periodic key rolling policies. The [reencryptZone](#reencryptZone) command is usually required after key rolling, to make sure the EDEKs on existing files use the new version key.

Manual steps to a complete key rolling and re-encryption are listed below. These instructions assume that you are running as the key admin or HDFS superuser as is appropriate.

    # As the key admin, roll the key to a new version
    hadoop key roll exposedKey

    # As the super user, re-encrypt the encryption zone. Possibly list zones first.
    hdfs crypto -listZones
    hdfs crypto -reencryptZone -start -path /zone

    # As the super user, periodically check the status of re-encryption
    hdfs crypto -listReencryptionStatus

    # As the super user, get encryption information from the file and double check it's encryption key version
    hdfs crypto -getFileEncryptionInfo -path /zone/helloWorld
    # console output: {cipherSuite: {name: AES/CTR/NoPadding, algorithmBlockSize: 16}, cryptoProtocolVersion: CryptoProtocolVersion{description='Encryption zones', version=2, unknownValue=null}, edek: 2010d301afbd43b58f10737ce4e93b39, iv: ade2293db2bab1a2e337f91361304cb3, keyName: exposedKey, ezKeyVersionName: exposedKey@1}
