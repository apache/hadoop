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

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------
  Replication is expensive -- the default 3x replication scheme in HDFS has 200% overhead in storage space and other resources (e.g., network bandwidth).
  However, for warm and cold datasets with relatively low I/O activities, additional block replicas are rarely accessed during normal operations, but still consume the same amount of resources as the first replica.

  Therefore, a natural improvement is to use Erasure Coding (EC) in place of replication, which provides the same level of fault-tolerance with much less storage space. In typical Erasure Coding (EC) setups, the storage overhead is no more than 50%.
  Replication factor of an EC file is meaningless. It is always 1 and cannot be changed via -setrep command.

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

      2. _Decode the data and generate the output data:_ New data and parity blocks are decoded from the input data. All missing data and parity blocks are decoded together.

      3. _Transfer the generated data blocks to target nodes:_ Once decoding is finished, the recovered blocks are transferred to target DataNodes.

 *  **Erasure coding policies**
    To accommodate heterogeneous workloads, we allow files and directories in an HDFS cluster to have different replication and erasure coding policies.
    The erasure coding policy encapsulates how to encode/decode a file. Each policy is defined by the following pieces of information:

      1. _The EC schema:_ This includes the numbers of data and parity blocks in an EC group (e.g., 6+3), as well as the codec algorithm (e.g., Reed-Solomon, XOR).

      2. _The size of a striping cell._ This determines the granularity of striped reads and writes, including buffer sizes and encoding work.

    Policies are named *codec*-*num data blocks*-*num parity blocks*-*cell size*. Currently, six built-in policies are supported: `RS-3-2-1024k`, `RS-6-3-1024k`, `RS-10-4-1024k`, `RS-LEGACY-6-3-1024k`, `XOR-2-1-1024k` and `REPLICATION`.

    `REPLICATION` is a special policy. It can only be set on directory, to force the directory to adopt 3x replication scheme, instead of inheriting its ancestor's erasure coding policy. This policy makes it possible to interleave 3x replication scheme directory with erasure coding directory.

    `REPLICATION` policy is always enabled. For other built-in policies, unless they are configured in `dfs.namenode.ec.policies.enabled` property, otherwise they are disabled by default.

    Similar to HDFS storage policies, erasure coding policies are set on a directory. When a file is created, it inherits the EC policy of its nearest ancestor directory.

    Directory-level EC policies only affect new files created within the directory. Once a file has been created, its erasure coding policy can be queried but not changed. If an erasure coded file is renamed to a directory with a different EC policy, the file retains its existing EC policy. Converting a file to a different EC policy requires rewriting its data; do this by copying the file (e.g. via distcp) rather than renaming it.

    We allow users to define their own EC policies via an XML file, which must have the following three parts:

       1. _layoutversion:_ This indicates the version of EC policy XML file format.

       2. _schemas:_ This includes all the user defined EC schemas.

       3. _policies:_ This includes all the user defined EC policies, and each policy consists of schema id and the size of a striping cell (cellsize).

    A sample EC policy XML file named user_ec_policies.xml.template is in the Hadoop conf directory, which user can reference.

 *  **Intel ISA-L**
    Intel ISA-L stands for Intel Intelligent Storage Acceleration Library. ISA-L is an open-source collection of optimized low-level functions designed for storage applications. It includes fast block Reed-Solomon type erasure codes optimized for Intel AVX and AVX2 instruction sets.
    HDFS erasure coding can leverage ISA-L to accelerate encoding and decoding calculation. ISA-L supports most major operating systems, including Linux and Windows.
    ISA-L is not enabled by default. See the instructions below for how to enable ISA-L.

Deployment
----------

### Cluster and hardware configuration

  Erasure coding places additional demands on the cluster in terms of CPU and network.

  Encoding and decoding work consumes additional CPU on both HDFS clients and DataNodes.

  Erasure coded files are also spread across racks for rack fault-tolerance.
  This means that when reading and writing striped files, most operations are off-rack.
  Network bisection bandwidth is thus very important.

  For rack fault-tolerance, it is also important to have at least as many racks as the configured EC stripe width.
  For EC policy RS (6,3), this means minimally 9 racks, and ideally 10 or 11 to handle planned and unplanned outages.
  For clusters with fewer racks than the stripe width, HDFS cannot maintain rack fault-tolerance, but will still attempt
  to spread a striped file across multiple nodes to preserve node-level fault-tolerance.

### Configuration keys

  The set of enabled erasure coding policies can be configured on the NameNode via `dfs.namenode.ec.policies.enabled` configuration. This restricts
  what EC policies can be set by clients. It does not affect the behavior of already set file or directory-level EC policies.

  By default, all built-in erasure coding policies are disabled. Typically, the cluster administrator will enable set of policies by including them
  in the `dfs.namenode.ec.policies.enabled` configuration based on the size of the cluster and the desired fault-tolerance properties. For instance,
  for a cluster with 9 racks, a policy like `RS-10-4-1024k` will not preserve rack-level fault-tolerance, and `RS-6-3-1024k` or `RS-3-2-1024k` might
  be more appropriate. If the administrator only cares about node-level fault-tolerance, `RS-10-4-1024k` would still be appropriate as long as
  there are at least 14 DataNodes in the cluster.

  A system default EC policy can be configured via 'dfs.namenode.ec.system.default.policy' configuration. With this configuration,
  the default EC policy will be used when no policy name is passed as an argument in the '-setPolicy' command.

  By default, the 'dfs.namenode.ec.system.default.policy' is "RS-6-3-1024k".

  The codec implementations for Reed-Solomon and XOR can be configured with the following client and DataNode configuration keys:
  `io.erasurecode.codec.rs.rawcoders` for the default RS codec,
  `io.erasurecode.codec.rs-legacy.rawcoders` for the legacy RS codec,
  `io.erasurecode.codec.xor.rawcoders` for the XOR codec.
  User can also configure self-defined codec with configuration key like:
  `io.erasurecode.codec.self-defined-codec.rawcoders`.
  The values for these key are lists of coder names with a fall-back mechanism. These codec factories are loaded in the order specified by the configuration values, until a codec is loaded successfully. The default RS and XOR codec configuration prefers native implementation over the pure Java one. There is no RS-LEGACY native codec implementation so the default is pure Java implementation only.
  All these codecs have implementations in pure Java. For default RS codec, there is also a native implementation which leverages Intel ISA-L library to improve the performance of codec. For XOR codec, a native implementation which leverages Intel ISA-L library to improve the performance of codec is also supported. Please refer to section "Enable Intel ISA-L" for more detail information.
  The default implementation for RS Legacy is pure Java, and the default implementations for default RS and XOR are native implementations using Intel ISA-L library.

  Erasure coding background recovery work on the DataNodes can also be tuned via the following configuration parameters:

  1. `dfs.datanode.ec.reconstruction.stripedread.timeout.millis` - Timeout for striped reads. Default value is 5000 ms.
  1. `dfs.datanode.ec.reconstruction.stripedread.threads` - Number of concurrent reader threads. Default value is 20 threads.
  1. `dfs.datanode.ec.reconstruction.stripedread.buffer.size` - Buffer size for reader service. Default value is 64KB.
  1. `dfs.datanode.ec.reconstruction.stripedblock.threads.size` - Number of threads used by the Datanode for background reconstruction work. Default value is 8 threads.

### Enable Intel ISA-L

  HDFS native implementation of default RS codec leverages Intel ISA-L library to improve the encoding and decoding calculation. To enable and use Intel ISA-L, there are three steps.
  1. Build ISA-L library. Please refer to the official site "https://github.com/01org/isa-l/" for detail information.
  2. Build Hadoop with ISA-L support. Please refer to "Intel ISA-L build options" section in "Build instructions for Hadoop" in (BUILDING.txt) in the source code.
  3. Use `-Dbundle.isal` to copy the contents of the `isal.lib` directory into the final tar file. Deploy Hadoop with the tar file. Make sure ISA-L is available on HDFS clients and DataNodes.

  To verify that ISA-L is correctly detected by Hadoop, run the `hadoop checknative` command.

### Administrative commands

  HDFS provides an `ec` subcommand to perform administrative commands related to erasure coding.

       hdfs ec [generic options]
         [-setPolicy -path <path> [-policy <policyName>] [-replicate]]
         [-getPolicy -path <path>]
         [-unsetPolicy -path <path>]
         [-listPolicies]
         [-addPolicies -policyFile <file>]
         [-listCodecs]
         [-enablePolicy -policy <policyName>]
         [-disablePolicy -policy <policyName>]
         [-help [cmd ...]]

Below are the details about each command.

 *  `[-setPolicy -path <path> [-policy <policyName>] [-replicate]]`

    Sets an erasure coding policy on a directory at the specified path.

      `path`: An directory in HDFS. This is a mandatory parameter. Setting a policy only affects newly created files, and does not affect existing files.

      `policyName`: The erasure coding policy to be used for files under this directory.
      This parameter can be omitted if a 'dfs.namenode.ec.system.default.policy' configuration is set.
      The EC policy of the path will be set with the default value in configuration.

      `-replicate` apply the special `REPLICATION` policy on the directory, force the directory to adopt 3x replication scheme.

      `-replicate` and `-policy <policyName>` are optional arguments. They cannot be specified at the same time.


 *  `[-getPolicy -path <path>]`

     Get details of the erasure coding policy of a file or directory at the specified path.

 *  `[-unsetPolicy -path <path>]`

     Unset an erasure coding policy set by a previous call to `setPolicy` on a directory. If the directory inherits the erasure coding policy from an ancestor directory, `unsetPolicy` is a no-op. Unsetting the policy on a directory which doesn't have an explicit policy set will not return an error.

 *  `[-listPolicies]`

     Lists the set of enabled erasure coding policies. These names are suitable for use with the `setPolicy` command.

 *  `[-addPolicies -policyFile <file>]`

     Add a list of erasure coding policies. Please refer etc/hadoop/user_ec_policies.xml.template for the example policy file. The maximum cell size is defined in property 'dfs.namenode.ec.policies.max.cellsize' with the default value 4MB.

 *  `[-listCodecs]`

     Get the list of supported erasure coding codecs and coders in system. A coder is an implementation of a codec. A codec can have different implementations, thus different coders. The coders for a codec are listed in a fall back order.

*  `[-removePolicy -policy <policyName>]`

     Remove an erasure coding policy.

*  `[-enablePolicy -policy <policyName>]`

     Enable an erasure coding policy.

*  `[-disablePolicy -policy <policyName>]`

     Disable an erasure coding policy.

Limitations
-----------

Certain HDFS file write operations, i.e., `hflush`, `hsync` and `append`,
are not supported on erasure coded files due to substantial technical
challenges.

* `append()` on an erasure coded file will throw `IOException`.
* `hflush()` and `hsync()` on `DFSStripedOutputStream` are no-op. Thus calling
`hflush()` or `hsync()` on an erasure coded file can not guarantee data
being persistent.

A client can use [`StreamCapabilities`](../hadoop-common/filesystem/filesystem.html#interface_StreamCapabilities)
API to query whether a `OutputStream` supports `hflush()` and `hsync()`.
If the client desires data persistence via `hflush()` and `hsync()`, the current
remedy is creating such files as regular 3x replication files in a
non-erasure-coded directory, or using `FSDataOutputStreamBuilder#replicate()`
API to create 3x replication files in an erasure-coded directory.
