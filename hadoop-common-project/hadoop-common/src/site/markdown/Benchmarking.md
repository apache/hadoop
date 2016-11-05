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

# Hadoop Benchmarking

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

This page is to discuss benchmarking Hadoop using tools it provides.

## NNThroughputBenchmark

### Overview

**NNThroughputBenchmark**, as its name indicates, is a name-node throughput benchmark, which runs a series of client threads on a single node against a name-node. If no name-node is configured, it will firstly start a name-node in the same process (_standalone mode_), in which case each client repetitively performs the same operation by directly calling the respective name-node methods. Otherwise, the benchmark will perform the operations against a remote name-node via client protocol RPCs (_remote mode_). Either way, all clients are running locally in a single process rather than remotely across different nodes. The reason is to avoid communication overhead caused by RPC connections and serialization, and thus reveal the upper bound of pure name-node performance.

The benchmark first generates inputs for each thread so that the input generation overhead does not effect the resulting statistics. The number of operations performed by threads is practically the same. Precisely, the difference between the number of operations performed by any two threads does not exceed 1. Then the benchmark executes the specified number of operations using the specified number of threads and outputs the resulting stats by measuring the number of operations performed by the name-node per second.

### Commands

The general command line syntax is:

`hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark [genericOptions] [commandOptions]`

#### Generic Options

This benchmark honors the [Hadoop command-line Generic Options](CommandsManual.html#Generic_Options) to alter its behavior. The benchmark, as other tools, will rely on the `fs.defaultFS` config, which is overridable by `-fs` command option, to run standalone mode or remote mode. If the `fs.defaultFS` scheme is not specified or is `file` (local), the benchmark will run in _standalone mode_. Specially, the _remote_ name-node config `dfs.namenode.fs-limits.min-block-size` should be set as 16 while in _standalone mode_ the benchmark turns off minimum block size verification for its internal name-node.

#### Command Options

The following are all supported command options:

| COMMAND\_OPTION    | Description |
|:---- |:---- |
|`-op` | Specify the operation. This option must be provided and should be the first option. |
|`-logLevel` | Specify the logging level when the benchmark runs. The default logging level is ERROR. |
|`-UGCacheRefreshCount` | After every specified number of operations, the benchmark purges the name-node's user group cache. By default the refresh is never called. |
|`-keepResults` | If specified, do not clean up the name-space after execution. By default the name-space will be removed after test. |

##### Operations Supported

Following are all the operations supported along with their respective operation-specific parameters (all optional) and default values.

| OPERATION\_OPTION    | Operation-specific parameters |
|:---- |:---- |
|`all` | _options for other operations_ |
|`create` | [`-threads 3`] [`-files 10`] [`-filesPerDir 4`] [`-close`] |
|`mkdirs` | [`-threads 3`] [`-dirs 10`] [`-dirsPerDir 2`] |
|`open` | [`-threads 3`] [`-files 10`] [`-filesPerDir 4`] [`-useExisting`] |
|`delete` | [`-threads 3`] [`-files 10`] [`-filesPerDir 4`] [`-useExisting`] |
|`fileStatus` | [`-threads 3`] [`-files 10`] [`-filesPerDir 4`] [`-useExisting`] |
|`rename` | [`-threads 3`] [`-files 10`] [`-filesPerDir 4`] [`-useExisting`] |
|`blockReport` | [`-datanodes 10`] [`-reports 30`] [`-blocksPerReport 100`] [`-blocksPerFile 10`] |
|`replication` | [`-datanodes 10`] [`-nodesToDecommission 1`] [`-nodeReplicationLimit 100`] [`-totalBlocks 100`] [`-replication 3`] |
|`clean` | N/A |

##### Operation Options

When running benchmarks with the above operation(s), please provide operation-specific parameters illustrated as following.

| OPERATION\_SPECIFIC\_OPTION    | Description |
|:---- |:---- |
|`-threads` | Number of total threads to run the respective operation. |
|`-files` | Number of total files for the respective operation. |
|`-dirs` | Number of total directories for the respective operation. |
|`-filesPerDir` | Number of files per directory. |
|`-close` | Close the files after creation. |
|`-dirsPerDir` | Number of directories per directory. |
|`-useExisting` | If specified, do not recreate the name-space, use existing data. |
|`-datanodes` | Total number of simulated data-nodes. |
|`-reports` | Total number of block reports to send. |
|`-blocksPerReport` | Number of blocks per report. |
|`-blocksPerFile` | Number of blocks per file. |
|`-nodesToDecommission` | Total number of simulated data-nodes to decommission. |
|`-nodeReplicationLimit` | The maximum number of outgoing replication streams for a data-node. |
|`-totalBlocks` | Number of total blocks to operate. |
|`-replication` | Replication factor. Will be adjusted to number of data-nodes if it is larger than that. |

### Reports

The benchmark measures the number of operations performed by the name-node per second. Specifically, for each operation tested, it reports the total running time in seconds (_Elapsed Time_), operation throughput (_Ops per sec_), and average time for the operations (_Average Time_). The higher, the better.

Following is a sample reports by running following commands that opens 100K files with 1K threads against a remote name-node. See [HDFS scalability: the limits to growth](https://www.usenix.org/legacy/publications/login/2010-04/openpdfs/shvachko.pdf) for real-world benchmark stats.

```
$ hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://nameservice:9000 -op open -threads 1000 -files 100000

--- open inputs ---
nrFiles = 100000
nrThreads = 1000
nrFilesPerDir = 4
--- open stats  ---
# operations: 100000
Elapsed Time: 9510
 Ops per sec: 10515.247108307045
Average Time: 90
```
