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
    * [Hardware resources](#Hardware_resources)
    * [Deployment](#Deployment)
        * [Configuration details](#Configuration_details)
        * [Deployment details](#Deployment_details)
        * [Administrative commands](#Administrative_commands)

Purpose
-------
  Replication is expensive -- the default 3x replication scheme has 200% overhead in storage space and other resources (e.g., network bandwidth).
  However, for “warm” and “cold” datasets with relatively low I/O activities, secondary block replicas are rarely accessed during normal operations, but still consume the same amount of resources as the primary ones.

  Therefore, a natural improvement is to use Erasure Coding (EC) in place of replication, which provides the same level of fault tolerance with much less storage space. In typical Erasure Coding(EC) setups, the storage overhead is ≤ 50%.

Background
----------

  In storage systems, the most notable usage of EC is Redundant Array of Inexpensive Disks (RAID). RAID implements EC through striping, which divides logically sequential data (such as a file) into smaller units (such as bit, byte, or block) and stores consecutive units on different disks. In the rest of this guide this unit of striping distribution is termed a striping cell (or cell). For each stripe of original data cells, a certain number of parity cells are calculated and stored -- the process of which is called encoding. The error on any striping cell can be recovered through decoding calculation based on surviving data and parity cells.

  Integrating the EC function with HDFS could get storage efficient deployments. It can provide similar data tolerance as traditional HDFS replication based deployments but it stores only one original replica data and parity cells.
  In a typical case, A file with 6 blocks will actually be consume space of 6*3 = 18 blocks with replication factor 3. But with EC (6 data,3 parity) deployment, it will only consume space of 9 blocks.

Architecture
------------
  In the context of EC, striping has several critical advantages. First, it enables online EC which bypasses the conversion phase and immediately saves storage space. Online EC also enhances sequential I/O performance by leveraging multiple disk spindles in parallel; this is especially desirable in clusters with high end networking  . Second, it naturally distributes a small file to multiple DataNodes and eliminates the need to bundle multiple files into a single coding group. This greatly simplifies file operations such as deletion, quota reporting, and migration between federated namespaces.

  As in general HDFS clusters, small files could account for over 3/4 of total storage consumption. So, In this first phase of erasure coding work, HDFS supports striping model. In the near future, HDFS will supports contiguous layout as second second phase work. So this guide focuses more on striping model EC.

 *  **NameNode Extensions** - Under the striping layout, a HDFS file is logically composed of block groups, each of which contains a certain number of   internal blocks.
   To eliminate the need for NameNode to monitor all internal blocks, a new hierarchical block naming protocol is introduced, where the ID of a block group can be inferred from any of its internal blocks. This allows each block group to be managed as a new type of BlockInfo named BlockInfoStriped, which tracks its own internal blocks by attaching an index to each replica location.

 *  **Client Extensions** - The basic principle behind the extensions is to allow the client node to work on multiple internal blocks in a block group in
    parallel.
    On the output / write path, DFSStripedOutputStream manages a set of data streamers, one for each DataNode storing an internal block in the current block group. The streamers mostly
    work asynchronously. A coordinator takes charge of operations on the entire block group, including ending the current block group, allocating a new block group, and so forth.
    On the input / read path, DFSStripedInputStream translates a requested logical byte range of data as ranges into internal blocks stored on DataNodes. It then issues read requests in
    parallel. Upon failures, it issues additional read requests for decoding.

 *  **DataNode Extensions** - ErasureCodingWorker(ECWorker) is for reconstructing erased erasure coding blocks and runs along with the Datanode process. Erased block details would have been found out by Namenode ReplicationMonitor thread and sent to Datanode via its heartbeat responses as discussed in the previous sections. For each reconstruction task,
   i.e. ReconstructAndTransferBlock, it will start an internal daemon thread that performs 3 key tasks:

      _1.Read the data from source nodes:_ For reading the data blocks from different source nodes, it uses a dedicated thread pool.
         The thread pool is initialized when ErasureCodingWorker initializes. Based on the EC policy, it schedules the read requests to all source targets and ensures only to read
         minimum required input blocks for reconstruction.

      _2.Decode the data and generate the output data:_ Actual decoding/encoding is done by using RawErasureEncoder API currently.
        All the erased data and/or parity blocks will be recovered together.

     _3.Transfer the generated data blocks to target nodes:_ Once decoding is finished, it will encapsulate the output data to packets and send them to
        target Datanodes.
   To accommodate heterogeneous workloads, we allow files and directories in an HDFS cluster to have different replication and EC policies.
*   **ErasureCodingPolicy**
    Information on how to encode/decode a file is encapsulated in an ErasureCodingPolicy class. Each policy is defined by the following 2 pieces of information:
    _1.The ECScema: This includes the numbers of data and parity blocks in an EC group (e.g., 6+3), as well as the codec algorithm (e.g., Reed-Solomon).

    _2.The size of a striping cell.

   Client and Datanode uses EC codec framework directly for doing the endoing/decoding work.

 *  **Erasure Codec Framework**
     We support a generic EC framework which allows system users to define, configure, and deploy multiple coding schemas such as conventional Reed-Solomon, HitchHicker and
     so forth.
     ErasureCoder is provided to encode or decode for a block group in the middle level, and RawErasureCoder is provided to perform the concrete algorithm calculation in the low level. ErasureCoder can
     combine and make use of different RawErasureCoders for tradeoff. We abstracted coder type, data blocks size, parity blocks size into ECSchema. A default system schema using RS (6, 3) is built-in.
     For the system default codec Reed-Solomon we implemented both RSRawErasureCoder in pure Java and NativeRawErasureCoder based on Intel ISA-L. Below is the performance
     comparing for different coding chunk size. We can see that the native coder can outperform the Java coder by up to 35X.

     _Intel® Storage Acceleration-Library(Intel® ISA-L)_ ISA-L is an Open Source Version and is a collection of low-level functions used in storage applications.
     The open source version contains fast erasure codes that implement a general Reed-Solomon type encoding for blocks of data that helps protect against
     erasure of whole blocks. The general ISA-L library contains an expanded set of functions used for data protection, hashing, encryption, etc. By
     leveraging instruction sets like SSE, AVX and AVX2, the erasure coding functions are much optimized and outperform greatly on IA platforms. ISA-L
     supports Linux, Windows and other platforms as well. Additionally, it also supports incremental coding so applications don’t have to wait all source
     blocks to be available before to perform the coding, which can be used in HDFS.

Hardware resources
------------------
  For using EC feature, you need to prepare for the following.
    Depending on the ECSchemas used, we need to have minimum number of Datanodes available in the cluster. Example if we use ReedSolomon(6, 3) ECSchema,
    then minimum nodes required is 9 to succeed the write. It can tolerate up to 3 failures.

Deployment
----------

### Configuration details

  In the EC feature, raw coders are configurable. So, users need to decide the RawCoder algorithms.
  Configure the customized algorithms with configuration key "*io.erasurecode.codecs*".

  Default Reed-Solomon based raw coders available in built, which can be configured by using the configuration key "*io.erasurecode.codec.rs.rawcoder*".
  And also another default raw coder available if XOR based raw coder. Which could be configured by using "*io.erasurecode.codec.xor.rawcoder*"

  _EarasureCodingWorker Confugurations:_
    dfs.datanode.stripedread.threshold.millis - Threshold time for polling timeout for read service. Default value is 5000
    dfs.datanode.stripedread.threads – Number striped read thread pool threads. Default value is 20
    dfs.datanode.stripedread.buffer.size - Buffer size for reader service. Default value is 256 * 1024

### Deployment details

  With the striping model, client machine is responsible for do the EC endoing and tranferring data to the datanodes.
  So, EC with striping model expects client machines with hghg end configurations especially of CPU and network.

### Administrative commands
 ErasureCoding command-line is provided to perform administrative commands related to ErasureCoding. This can be accessed by executing the following command.

       hdfs erasurecode [generic options]
         [-setPolicy [-s <policyName>] <path>]
         [-getPolicy <path>]
         [-listPolicies]
         [-usage [cmd ...]]
         [-help [cmd ...]]

Below are the details about each command.

*  **SetPolicy command**: `[-setPolicy [-s <policyName>] <path>]`

    SetPolicy command is used to set an ErasureCoding policy on a directory at the specified path.

      `path`: Refer to a pre-created directory in HDFS. This is a mandatory parameter.

      `policyName`: This is an optional parameter, specified using ‘-s’ flag. Refer to the name of ErasureCodingPolicy to be used for encoding files under this directory. If not specified the system default ErasureCodingPolicy will be used.

*  **GetPolicy command**: `[-getPolicy <path>]`

     GetPolicy command is used to get details of the ErasureCoding policy of a file or directory at the specified path.

*  **ListPolicies command**:  `[-listPolicies]`

     Lists all supported ErasureCoding policies. For setPolicy command, one of these policies' name should be provided.