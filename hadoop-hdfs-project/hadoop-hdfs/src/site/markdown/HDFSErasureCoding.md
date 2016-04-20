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

HDFS Erasure Coding
===================

* [HDFS Erasure Coding](#HDFS_Erasure_Coding)
    * [Purpose](#Purpose)
    * [Background](#Background)
    * [Architecture](#Architecture)
    * [Deployment](#Deployment)
        * [Cluster and hardware configuration](#Cluster_and_hardware_configuration)
        * [Configuration keys](#Configuration_keys)
        * [Administrative commands](#Administrative_commands)

Purpose
-------
  Replication is expensive -- the default 3x replication scheme in HDFS has 200% overhead in storage space and other resources (e.g., network bandwidth).
  However, for warm and cold datasets with relatively low I/O activities, additional block replicas are rarely accessed during normal operations, but still consume the same amount of resources as the first replica.

  Therefore, a natural improvement is to use Erasure Coding (EC) in place of replication, which provides the same level of fault-tolerance with much less storage space. In typical Erasure Coding (EC) setups, the storage overhead is no more than 50%.

Background
----------

  In storage systems, the most notable usage of EC is Redundant Array of Inexpensive Disks (RAID). RAID implements EC through striping, which divides logically sequential data (such as a file) into smaller units (such as bit, byte, or block) and stores consecutive units on different disks. In the rest of this guide this unit of striping distribution is termed a striping cell (or cell). For each stripe of original data cells, a certain number of parity cells are calculated and stored -- the process of which is called encoding. The error on any striping cell can be recovered through decoding calculation based on surviving data and parity cells.

  Integrating EC with HDFS can improve storage efficiency while still providing similar data durability as traditional replication-based HDFS deployments.
  As an example, a 3x replicated file with 6 blocks will consume 6*3 = 18 blocks of disk space. But with EC (6 data, 3 parity) deployment, it will only consume 9 blocks of disk space.

Architecture
------------
  In the context of EC, striping has several critical advantages. First, it enables online EC (writing data immediately in EC format), avoiding a conversion phase and immediately saving storage space. Online EC also enhances sequential I/O performance by leveraging multiple disk spindles in parallel; this is especially desirable in clusters with high end networking. Second, it naturally distributes a small file to multiple DataNodes and eliminates the need to bundle multiple files into a single coding group. This greatly simplifies file operations such as deletion, quota reporting, and migration between federated namespaces.

  In typical HDFS clusters, small files can account for over 3/4 of total storage consumption. To better support small files, in this first phase of work HDFS supports EC with striping. In the future, HDFS will also support a contiguous EC layout. See the design doc and discussion on [HDFS-7285](https://issues.apache.org/jira/browse/HDFS-7285) for more information.

 *  **NameNode Extensions** - Striped HDFS files are logically composed of block groups, each of which contains a certain number of internal blocks.
    To reduce NameNode memory consumption from these additional blocks, a new hierarchical block naming protocol was introduced. The ID of a block group can be inferred from the ID of any of its internal blocks. This allows management at the level of the block group rather than the block.

 *  **Client Extensions** - The client read and write paths were enhanced to work on multiple internal blocks in a block group in parallel.
    On the output / write path, DFSStripedOutputStream manages a set of data streamers, one for each DataNode storing an internal block in the current block group. The streamers mostly
    work asynchronously. A coordinator takes charge of operations on the entire block group, including ending the current block group, allocating a new block group, and so forth.
    On the input / read path, DFSStripedInputStream translates a requested logical byte range of data as ranges into internal blocks stored on DataNodes. It then issues read requests in
    parallel. Upon failures, it issues additional read requests for decoding.

 *  **DataNode Extensions** - The DataNode runs an additional ErasureCodingWorker (ECWorker) task for background recovery of failed erasure coded blocks. Failed EC blocks are detected by the NameNode, which then chooses a DataNode to do the recovery work. The recovery task is passed as a heartbeat response. This process is similar to how replicated blocks are re-replicated on failure. Reconstruction performs three key tasks:

      1. _Read the data from source nodes:_ Input data is read in parallel from source nodes using a dedicated thread pool.
        Based on the EC policy, it schedules the read requests to all source targets and reads only the minimum number of input blocks for reconstruction.

      1. _Decode the data and generate the output data:_ New data and parity blocks are decoded from the input data. All missing data and parity blocks are decoded together.

      1. _Transfer the generated data blocks to target nodes:_ Once decoding is finished, the recovered blocks are transferred to target DataNodes.

 *  **ErasureCoding policy**
    To accommodate heterogeneous workloads, we allow files and directories in an HDFS cluster to have different replication and EC policies.
    Information on how to encode/decode a file is encapsulated in an ErasureCodingPolicy class. Each policy is defined by the following 2 pieces of information:

      1. _The ECSchema:_ This includes the numbers of data and parity blocks in an EC group (e.g., 6+3), as well as the codec algorithm (e.g., Reed-Solomon).

      1. _The size of a striping cell._ This determines the granularity of striped reads and writes, including buffer sizes and encoding work.

    Currently, HDFS supports the Reed-Solomon and XOR erasure coding algorithms. Additional algorithms are planned as future work.
    The system default scheme is Reed-Solomon (6, 3) with a cell size of 64KB.


Deployment
----------

### Cluster and hardware configuration

  Erasure coding places additional demands on the cluster in terms of CPU and network.

  Encoding and decoding work consumes additional CPU on both HDFS clients and DataNodes.

  Erasure coded files are also spread across racks for rack fault-tolerance.
  This means that when reading and writing striped files, most operations are off-rack.
  Network bisection bandwidth is thus very important.

  For rack fault-tolerance, it is also important to have at least as many racks as the configured EC stripe width.
  For the default EC policy of RS (6,3), this means minimally 9 racks, and ideally 10 or 11 to handle planned and unplanned outages.
  For clusters with fewer racks than the stripe width, HDFS cannot maintain rack fault-tolerance, but will still attempt
  to spread a striped file across multiple nodes to preserve node-level fault-tolerance.

### Configuration keys

  The codec implementation for Reed-Solomon and XOR can be configured with the following client and DataNode configuration keys:
  `io.erasurecode.codec.rs-default.rawcoder` for the default RS codec,
  `io.erasurecode.codec.rs-legacy.rawcoder` for the legacy RS codec,
  `io.erasurecode.codec.xor.rawcoder` for the XOR codec.
  The default implementations for all of these codecs are pure Java.

  Erasure coding background recovery work on the DataNodes can also be tuned via the following configuration parameters:

  1. `dfs.datanode.stripedread.timeout.millis` - Timeout for striped reads. Default value is 5000 ms.
  1. `dfs.datanode.stripedread.threads` - Number of concurrent reader threads. Default value is 20 threads.
  1. `dfs.datanode.stripedread.buffer.size` - Buffer size for reader service. Default value is 256KB.

### Administrative commands

  HDFS provides an `erasurecode` subcommand to perform administrative commands related to erasure coding.

       hdfs erasurecode [generic options]
         [-setPolicy [-p <policyName>] <path>]
         [-getPolicy <path>]
         [-listPolicies]
         [-usage [cmd ...]]
         [-help [cmd ...]]

Below are the details about each command.

 *  `[-setPolicy [-p <policyName>] <path>]`

    Sets an ErasureCoding policy on a directory at the specified path.

      `path`: An directory in HDFS. This is a mandatory parameter. Setting a policy only affects newly created files, and does not affect existing files.

      `policyName`: The ErasureCoding policy to be used for files under this directory. This is an optional parameter, specified using ‘-s’ flag. If no policy is specified, the system default ErasureCodingPolicy will be used.

 *  `[-getPolicy <path>]`

     Get details of the ErasureCoding policy of a file or directory at the specified path.

 *  `[-listPolicies]`

     Lists all supported ErasureCoding policies. These names are suitable for use with the `setPolicy` command.
