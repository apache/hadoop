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

Memory Storage Support in HDFS
==============================

* [Introduction](#Introduction)
* [Administrator Configuration](#Administrator_Configuration)
    * [Setup RAM Disks on Data Nodes](#Setup_RAM_Disks_on_Data_Nodes)
        * [Choosing `tmpfs` \(vs `ramfs`\)](#Choosing_`tmpfs`_\(vs_`ramfs`\))
        * [Mount RAM Disks](#Mount_RAM_Disks)
        * [Tag `tmpfs` volume with the RAM\_DISK Storage Type](#Tag_`tmpfs`_volume_with_the_RAM\_DISK_Storage_Type)
        * [Ensure Storage Policies are enabled](#Ensure_Storage_Policies_are_enabled)
* [Application Usage](#Application_Usage)
    * [Use the LAZY\_PERSIST Storage Policy](#Use_the_LAZY\_PERSIST_Storage_Policy)
        * [Invoke `hdfs storagepolicies` command for directories](#Invoke_hdfs_storagepolicies_command_for_directories)
        * [Call `setStoragePolicy` method for directories](#Call_`setStoragePolicy`_method_for_directories)
        * [Pass `LAZY_PERSIST` `CreateFlag` for new files](#Pass_`LAZY_PERSIST`_`CreateFlag`_for_new_files)

Introduction
------------

HDFS supports writing to off-heap memory managed by the Data Nodes. The Data Nodes will flush in-memory data to disk asynchronously thus removing expensive disk IO and checksum computations from the performance-sensitive IO path, hence we call such writes *Lazy Persist* writes. HDFS provides best-effort persistence guarantees for Lazy Persist Writes. Rare data loss is possible in the event of a node restart before replicas are persisted to disk. Applications can choose to use Lazy Persist Writes to trade off some durability guarantees in favor of reduced latency.

This feature is available starting with Apache Hadoop 2.6.0 and was developed under Jira [HDFS-6581](https://issues.apache.org/jira/browse/HDFS-6581).

![Lazy Persist Writes](images/LazyPersistWrites.png)

The target use cases are applications that would benefit from writing relatively low amounts of data (from a few GB up to tens of GBs depending on available memory) with low latency. Memory storage is for applications that run within the cluster and collocated with HDFS Data Nodes. We have observed that the latency overhead from network replication negates the benefits of writing to memory.

Applications that use Lazy Persist Writes will continue to work by falling back to DISK storage if memory is insufficient or unconfigured.

Administrator Configuration
---------------------------

This section enumerates the administrative steps required before applications can start using the feature in a cluster.

## Setup RAM Disks on Data Nodes

Initialize a RAM disk on each Data Node. The choice of RAM Disk allows better data persistence across Data Node process restarts. The following setup will work on most Linux distributions. Using RAM disks on other platforms is not currently supported.

### Choosing `tmpfs` \(vs `ramfs`\)

Linux supports using two kinds of RAM disks - `tmpfs` and `ramfs`. The size of `tmpfs` is limited by the Linux kernel while `ramfs` grows to fill all available system memory. There is a downside to `tmpfs` since its contents can be swapped to disk under memory pressure. However many performance-sensitive deployments run with swapping disabled so we do not expect this to be an issue in practice.

HDFS currently supports using `tmpfs` partitions. Support for adding `ramfs` is in progress (See [HDFS-8584](https://issues.apache.org/jira/browse/HDFS-8584)).

### Mount RAM Disks

Mount the RAM Disk partition with the Unix `mount` command. E.g. to mount a 32 GB `tmpfs` partition under `/mnt/dn-tmpfs/`

        sudo mount -t tmpfs -o size=32g tmpfs /mnt/dn-tmpfs/

It is recommended you create an entry in the `/etc/fstab` so the RAM Disk is recreated automatically on node restarts. Another option is to use a sub-directory under `/dev/shm` which is a `tmpfs` mount available by default on most Linux distributions. Ensure that the size of the mount is greater than or equal to your `dfs.datanode.max.locked.memory` setting else override it in `/etc/fstab`. Using more than one `tmpfs` partition per Data Node for Lazy Persist Writes is not recommended.

### Tag `tmpfs` volume with the RAM\_DISK Storage Type

Tag the `tmpfs` directory with the RAM_DISK storage type via the `dfs.datanode.data.dir` configuration setting in `hdfs-site.xml`. E.g. On a Data Node with three hard disk volumes `/grid/0`, `/grid/1` and `/grid/2` and a `tmpfs` mount `/mnt/dn-tmpfs`, `dfs.datanode.data.dir` must be set as follows:

        <property>
          <name>dfs.datanode.data.dir</name>
          <value>/grid/0,/grid/1,/grid/2,[RAM_DISK]/mnt/dn-tmpfs</value>
        </property>

This step is crucial. Without the RAM_DISK tag, HDFS will treat the `tmpfs` volume as non-volatile storage and data will not be saved to persistent storage. You will lose data on node restart.

### Ensure Storage Policies are enabled

Ensure that the global setting to turn on Storage Policies is enabled [as documented here](ArchivalStorage.html#Configuration). This setting is on by default.


Application Usage
-----------------

## Use the LAZY\_PERSIST Storage Policy

Applications indicate that HDFS can use Lazy Persist Writes for a file with the `LAZY_PERSIST` storage policy. Administrative privileges are *not* required to set the policy and it can be set in one of three ways.

### Invoke `hdfs storagepolicies` command for directories

Setting the policy on a directory causes it to take effect for all new files in the directory. The `hdfs storagepolicies` command can be used to set the policy as described in the [Storage Policies documentation](ArchivalStorage.html#Storage_Policy_Commands).

        hdfs storagepolicies -setStoragePolicy -path <path> -policy LAZY_PERSIST

### Call `setStoragePolicy` method for directories

Starting with Apache Hadoop 2.8.0, an application can programmatically set the Storage Policy with `FileSystem.setStoragePolicy`. E.g.

        fs.setStoragePolicy(path, "LAZY_PERSIST");

### Pass `LAZY_PERSIST` `CreateFlag` for new files

An application can pass `CreateFlag#LAZY_PERSIST` when creating a new file with `FileSystem#create` API. E.g.

        FSDataOutputStream fos =
            fs.create(
                path,
                FsPermission.getFileDefault(),
                EnumSet.of(CreateFlag.CREATE, CreateFlag.LAZY_PERSIST),
                bufferLength,
                replicationFactor,
                blockSize,
                null);
