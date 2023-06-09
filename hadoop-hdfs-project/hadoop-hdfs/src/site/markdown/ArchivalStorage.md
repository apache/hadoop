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

Archival Storage, SSD & Memory
==============================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

*Archival Storage* is a solution to decouple growing storage capacity from compute capacity. Nodes with higher density and less expensive storage with low compute power are becoming available and can be used as cold storage in the clusters. Based on policy the data from hot can be moved to the cold. Adding more nodes to the cold storage can grow the storage independent of the compute capacity in the cluster.

The frameworks provided by Heterogeneous Storage and Archival Storage generalizes the HDFS architecture to include other kinds of storage media including *SSD* and *memory*. Users may choose to store their data in SSD or memory for a better performance.

Storage Types and Storage Policies
----------------------------------

### Storage Types: ARCHIVE, DISK, SSD, RAM\_DISK and NVDIMM

The first phase of [Heterogeneous Storage (HDFS-2832)](https://issues.apache.org/jira/browse/HDFS-2832) changed datanode storage model from a single storage, which may correspond to multiple physical storage medias, to a collection of storages with each storage corresponding to a physical storage media. It also added the notion of storage types, DISK and SSD, where DISK is the default storage type.

A new storage type *ARCHIVE*, which has high storage density (petabyte of storage) but little compute power, is added for supporting archival storage.

Another new storage type *RAM\_DISK* is added for supporting writing single replica files in memory.

From Hadoop 3.4, a new storage type *NVDIMM* is added for supporting writing replica files in non-volatile memory that has the capability to hold saved data even if the power is turned off.

### Storage Policies: Hot, Warm, Cold, All\_SSD, One\_SSD, Lazy\_Persist, Provided and All\_NVDIMM

A new concept of storage policies is introduced in order to allow files to be stored in different storage types according to the storage policy.

We have the following storage policies:

* **Hot** - for both storage and compute. The data that is popular and still being used for processing will stay in this policy. When a block is hot, all replicas are stored in DISK.
* **Cold** - only for storage with limited compute. The data that is no longer being used, or data that needs to be archived is moved from hot storage to cold storage. When a block is cold, all replicas are stored in ARCHIVE.
* **Warm** - partially hot and partially cold. When a block is warm, some of its replicas are stored in DISK and the remaining replicas are stored in ARCHIVE.
* **All\_SSD** - for storing all replicas in SSD.
* **One\_SSD** - for storing one of the replicas in SSD. The remaining replicas are stored in DISK.
* **Lazy\_Persist** - for writing blocks with single replica in memory. The replica is first written in RAM\_DISK and then it is lazily persisted in DISK.
* **Provided** - for storing data outside HDFS. See also [HDFS Provided Storage](./HdfsProvidedStorage.html).
* **All\_NVDIMM** - for storing all replicas in NVDIMM.

More formally, a storage policy consists of the following fields:

1.  Policy ID
2.  Policy name
3.  A list of storage types for block placement
4.  A list of fallback storage types for file creation
5.  A list of fallback storage types for replication

When there is enough space, block replicas are stored according to the storage type list specified in \#3. When some of the storage types in list \#3 are running out of space, the fallback storage type lists specified in \#4 and \#5 are used to replace the out-of-space storage types for file creation and replication, respectively.

The following is a typical storage policy table.

| **Policy** **ID** | **Policy** **Name** | **Block Placement** **(n  replicas)** | **Fallback storages** **for creation** | **Fallback storages** **for replication** |
|:---- |:---- |:---- |:---- |:---- |
| 15 | Lazy\_Persist | RAM\_DISK: 1, DISK: *n*-1 | DISK | DISK |
| 14 | All\_NVDIMM | NVDIMM: *n* | DISK | DISK |
| 12 | All\_SSD | SSD: *n* | DISK | DISK |
| 10 | One\_SSD | SSD: 1, DISK: *n*-1 | SSD, DISK | SSD, DISK |
| 7 | Hot (default) | DISK: *n* | \<none\> | ARCHIVE |
| 5 | Warm | DISK: 1, ARCHIVE: *n*-1 | ARCHIVE, DISK | ARCHIVE, DISK |
| 2 | Cold | ARCHIVE: *n* | \<none\> | \<none\> |
| 1 | Provided | PROVIDED: 1, DISK: *n*-1 | PROVIDED, DISK | PROVIDED, DISK |

Note 1: The Lazy\_Persist policy is useful only for single replica blocks. For blocks with more than one replicas, all the replicas will be written to DISK since writing only one of the replicas to RAM\_DISK does not improve the overall performance.

Note 2: For the erasure coded files with striping layout, the suitable storage policies are All\_SSD, Hot, Cold and All\_NVDIMM. So, if user sets the policy for striped EC files other than the mentioned policies, it will not follow that policy while creating or moving block.

### Storage Policy Resolution

When a file or directory is created, its storage policy is *unspecified*. The storage policy can be specified using the "[`storagepolicies -setStoragePolicy`](#Set_Storage_Policy)" command. The effective storage policy of a file or directory is resolved by the following rules.

1.  If the file or directory is specified with a storage policy, return it.

2.  For an unspecified file or directory, if it is the root directory, return the *default storage policy*. Otherwise, return its parent's effective storage policy.

The effective storage policy can be retrieved by the "[`storagepolicies -getStoragePolicy`](#Get_Storage_Policy)" command.

### Configuration

* **dfs.storage.policy.enabled** - for enabling/disabling the storage policy feature. The default value is `true`.
* **dfs.storage.default.policy** - Set the default storage policy with the policy name. The default value is `HOT`.  All possible policies are defined in enum StoragePolicy, including `LAZY_PERSIST` `ALL_SSD` `ONE_SSD` `HOT` `WARM` `COLD` `PROVIDED` and `ALL_NVDIMM`.
* **dfs.datanode.data.dir** - on each data node, the comma-separated storage locations should be tagged with their storage types. This allows storage policies to place the blocks on different storage types according to policy. For example:

    1.  A datanode storage location /grid/dn/disk0 on DISK should be configured with `[DISK]file:///grid/dn/disk0`
    2.  A datanode storage location /grid/dn/ssd0 on SSD should be configured with `[SSD]file:///grid/dn/ssd0`
    3.  A datanode storage location /grid/dn/archive0 on ARCHIVE should be configured with `[ARCHIVE]file:///grid/dn/archive0`
    4.  A datanode storage location /grid/dn/ram0 on RAM_DISK should be configured with `[RAM_DISK]file:///grid/dn/ram0`
    5.  A datanode storage location /grid/dn/nvdimm0 on NVDIMM should be configured with `[NVDIMM]file:///grid/dn/nvdimm0`

    The default storage type of a datanode storage location will be DISK if it does not have a storage type tagged explicitly.

    Sometimes, users can setup the DataNode data directory to point to multiple volumes with different storage types. It is important to check if the volume is mounted correctly before initializing the storage locations. The user has the option to enforce the filesystem for a storage key with the following key:
    **dfs.datanode.storagetype.*.filesystem** - replace the '*' to any storage type, for example, dfs.datanode.storagetype.ARCHIVE.filesystem=fuse_filesystem

Storage Policy Based Data Movement
----------------------------------

Setting a new storage policy on already existing file/dir will change the policy in Namespace, but it will not move the blocks physically across storage medias.
Following 2 options will allow users to move the blocks based on new policy set. So, once user change/set to a new policy on file/directory, user should also perform one of the following options to achieve the desired data movement. Note that both options cannot be allowed to run simultaneously.

### <u>S</u>torage <u>P</u>olicy <u>S</u>atisfier (SPS)

When user changes the storage policy on a file/directory, user can call `HdfsAdmin` API `satisfyStoragePolicy()` to move the blocks as per the new policy set.
The SPS tool running external to namenode periodically scans for the storage mismatches between new policy set and the physical blocks placed. This will only track the files/directories for which user invoked satisfyStoragePolicy. If SPS identifies some blocks to be moved for a file, then it will schedule block movement tasks to datanodes. If there are any failures in movement, the SPS will re-attempt by sending new block movement tasks.

SPS can be enabled as an external service outside Namenode or disabled dynamically without restarting the Namenode.

Detailed design documentation can be found at [Storage Policy Satisfier(SPS) (HDFS-10285)](https://issues.apache.org/jira/browse/HDFS-10285)

* **Note**: When user invokes `satisfyStoragePolicy()` API on a directory, SPS will scan all sub-directories and consider all the files for satisfy the policy..

* HdfsAdmin API :
        `public void satisfyStoragePolicy(final Path path) throws IOException`

* Arguments :

| | |
|:---- |:---- |
| `path` | A path which requires blocks storage movement. |

####Configurations:

*   **dfs.storage.policy.satisfier.mode** - Used to enable external service outside NN or disable SPS.
   Following string values are supported - `external`, `none`. Configuring `external` value represents SPS is enable and `none` to disable.
   The default value is `none`.

*   **dfs.storage.policy.satisfier.recheck.timeout.millis** - A timeout to re-check the processed block storage movement
   command results from Datanodes.

*   **dfs.storage.policy.satisfier.self.retry.timeout.millis** - A timeout to retry if no block movement results reported from
   Datanode in this configured timeout.

### Mover - A New Data Migration Tool

A new data migration tool is added for archiving data. The tool is similar to Balancer. It periodically scans the files in HDFS to check if the block placement satisfies the storage policy. For the blocks violating the storage policy, it moves the replicas to a different storage type in order to fulfill the storage policy requirement. Note that it always tries to move block replicas within the same node whenever possible. If that is not possible (e.g. when a node doesn’t have the target storage type) then it will copy the block replicas to another node over the network.

* Command:

        hdfs mover [-p <files/dirs> | -f <local file name>]

* Arguments:

| | |
|:---- |:---- |
| `-p <files/dirs>` | Specify a space separated list of HDFS files/dirs to migrate. |
| `-f <local file>` | Specify a local file containing a list of HDFS files/dirs to migrate. |

Note that, when both -p and -f options are omitted, the default path is the root directory.

####Administrator notes:

`StoragePolicySatisfier` and `Mover tool` cannot run simultaneously. If a Mover instance is already triggered and running, SPS will be disabled while starting. In that case, administrator should make sure, Mover execution finished and then enable external SPS service again. Similarly when SPS enabled already, Mover cannot be run. If administrator is looking to run Mover tool explicitly, then he/she should make sure to disable SPS first and then run Mover. Please look at the commands section to know how to enable external service outside NN or disable SPS dynamically.

Storage Policy Commands
-----------------------

### List Storage Policies

List out all the storage policies.

* Command:

        hdfs storagepolicies -listPolicies

* Arguments: none.

### Set Storage Policy

Set a storage policy to a file or a directory.

* Command:

        hdfs storagepolicies -setStoragePolicy -path <path> -policy <policy>

* Arguments:

| | |
|:---- |:---- |
| `-path <path>` | The path referring to either a directory or a file. |
| `-policy <policy>` | The name of the storage policy. |

### Unset Storage Policy

Unset a storage policy to a file or a directory. After the unset command the storage policy of the nearest ancestor will apply, and if there is no policy on any ancestor then the default storage policy will apply.

* Command:

        hdfs storagepolicies -unsetStoragePolicy -path <path>

* Arguments:

| | |
|:---- |:---- |
| `-path <path>` | The path referring to either a directory or a file. |

### Get Storage Policy

Get the storage policy of a file or a directory.

* Command:

        hdfs storagepolicies -getStoragePolicy -path <path>

* Arguments:

| | |
|:---- |:---- |
| `-path <path>` | The path referring to either a directory or a file. |

### Satisfy Storage Policy

Schedule blocks to move based on file's/directory's current storage policy.

* Command:

        hdfs storagepolicies -satisfyStoragePolicy -path <path>

* Arguments:

| | |
|:---- |:---- |
| `-path <path>` | The path referring to either a directory or a file. |


### Enable external service outside NN or Disable SPS without restarting Namenode
If administrator wants to switch modes of SPS feature while Namenode is running, first he/she needs to update the desired value(external or none) for the configuration item `dfs.storage.policy.satisfier.mode` in configuration file (`hdfs-site.xml`) and then run the following Namenode reconfig command

* Command:

       hdfs dfsadmin -reconfig namenode <host:ipc_port> start

### Start External SPS Service.
If administrator wants to start external sps, first he/she needs to configure property `dfs.storage.policy.satisfier.mode` with `external` value in configuration file (`hdfs-site.xml`) and then run Namenode reconfig command. Please ensure that network topology configurations in the configuration file are same as namenode, this cluster will be used for matching target nodes. After this, start external sps service using following command

* Command:

      hdfs --daemon start sps
