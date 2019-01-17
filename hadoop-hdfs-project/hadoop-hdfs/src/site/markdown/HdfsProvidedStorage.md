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

HDFS Provided Storage
=====================

Provided storage allows data *stored outside HDFS* to be mapped to and addressed
from HDFS. It builds on [heterogeneous storage](./ArchivalStorage.html) by
introducing a new storage type, `PROVIDED`, to the set of media in a datanode.
Clients accessing data in
`PROVIDED` storages can cache replicas in local media, enforce HDFS invariants
(e.g., security, quotas), and address more data than the cluster could persist
in the storage attached to DataNodes. This architecture is particularly useful
in scenarios where HDFS clusters are ephemeral (e.g., cloud scenarios), and/or
require to read data that lives in other storage systems (e.g., blob stores).

Provided storage is an experimental feature in HDFS.

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

As of this writing, support for mounting external storage as `PROVIDED` blocks
is limited to creating a *read-only image* of a remote namespace that implements the
`org.apache.hadoop.fs.FileSystem` interface, and starting a NameNode
to serve the image. Specifically, reads from a snapshot of a remote namespace are
supported. Adding a remote namespace to an existing/running namenode, refreshing the
remote snapshot, unmounting, and writes are not available in this release. One
can use [ViewFs](./ViewFs.html) and [RBF](../hadoop-hdfs-rbf/HDFSRouterFederation.html) to
integrate namespaces with `PROVIDED` storage into an existing deployment.

Creating HDFS Clusters with `PROVIDED` Storage
----------------------------------------------

One can create snapshots of the remote namespace using the `fs2img` tool. Given
a path to a remote `FileSystem`, the tool creates an _image_ mirroring the
namespace and an _alias map_ that maps blockIDs in the generated image to a
`FileRegion` in the remote filesystem. A `FileRegion` contains sufficient information to
address a fixed sequence of bytes in the remote `FileSystem` (e.g., file, offset, length)
and a nonce to verify that the region is unchanged since the image was generated.

After the NameNode image and alias map are created, the NameNode and DataNodes
must be configured to consistently reference this address space. When a DataNode
registers with an attached, `PROVIDED` storage, the NameNode considers all the
external blocks to be addressable through that DataNode, and may begin to direct
clients to it. Symmetrically, the DataNode must be able to map every block in
the `PROVIDED` storage to remote data.

Deployment details vary depending on the configured alias map implementation.

### `PROVIDED` Configuration

Each NameNode supports one alias map. When `PROVIDED` storage is enabled,
the storage ID configured on the NameNode and DataNodes must match.
All other details are internal to the alias map implementation.

The configuration to enable `PROVIDED` storage is as follows.
The configuration options available for the alias map implementations are
available below.

```xml
<configuration>

  <property>
    <name>dfs.namenode.provided.enabled</name>
    <value>true</value>
    <description>Enabled provided storage on the Namenode</description>
  </property>

  <property>
     <name>dfs.datanode.data.dir</name>
     <value>[DISK]/local/path/to/blocks/, [PROVIDED]remoteFS://remoteFS-authority/path/to/data/</value>
  </property>

  <property>
      <name>dfs.provided.storage.id</name>
      <value>DS-PROVIDED</value>
      <description>The storage ID used for provided storages in the cluster.</description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.class</name>
    <value>org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap</value>
  </property>

</configuration>
```

### fs2img tool

The `fs2img` tool "walks" over a remote namespace by recursively enumerating
children of a remote URI to produce an FSImage. Some attributes can be
controlled by plugins, such as owner/group mappings from the remote filesystem
to HDFS and the mapping of files to HDFS blocks.

The various options available in running the tool are:

| Option                  | Property                    | Default           | Description |
|:------------------------|:--------------------------- |:----------------- |:---- |
| `-o`, `--outdir`        | dfs.namenode.name.dir       | file://${hadoop.tmp.dir}/dfs/name | Output directory |
| `-b`, `--blockclass`    | dfs.provided.aliasmap.class | NullBlocksMap     | Block output class |
| `-u`, `--ugiclass`      | hdfs.image.writer.ugi.class | SingleUGIResolver | UGI resolver class |
| `-i`, `--blockidclass`  | hdfs.image.writer.blockresolver.class | FixedBlockResolver | Block resolver class |
| `-c`, `--cachedirs`     | hdfs.image.writer.cache.entries | 100           | Max active dirents |
| `-cid`, `--clusterID`   |                             |                   | Cluster ID |
| `-bpid`, `--blockPoolID`|                             |                   | Block pool ID |

#### Examples

Assign all files to be owned by "rmarathe", write to gzip compressed text:
```
hadoop org.apache.hadoop.hdfs.server.namenode.FileSystemImage \
  -Dhdfs.image.writer.ugi.single.user=rmarathe \
  -Ddfs.provided.aliasmap.text.codec=gzip \
  -Ddfs.provided.aliasmap.text.write.dir=file:///tmp/
  -b org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap \
  -u org.apache.hadoop.hdfs.server.namenode.SingleUGIResolver \
  -o file:///tmp/name \
  hdfs://afreast/projects/ydau/onan
```

Assign ownership based on a custom `UGIResolver`, in LevelDB:
```
hadoop org.apache.hadoop.hdfs.server.namenode.FileSystemImage \
  -Ddfs.provided.aliasmap.leveldb.path=/path/to/leveldb/map/dingos.db \
  -b org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.LevelDBFileRegionAliasMap \
  -o file:///tmp/name \
  -u CustomResolver \
  hdfs://enfield/projects/ywqmd/incandenza
```


Alias Map Implementations
-------------------------

The alias map implementation to use is configured using the `dfs.provided.aliasmap.class` parameter.
Currently, the following two types of alias maps are supported.

### InMemoryAliasMap

This is a LevelDB-based alias map that runs as a separate server in Namenode.
The alias map itself can be created using the `fs2img` tool using the option
`-Ddfs.provided.aliasmap.leveldb.path=file:///path/to/leveldb/map/dingos.db -b org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.LevelDBFileRegionAliasMap`
as in the example above.

Datanodes contact this alias map using the `org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol` protocol.

#### Configuration


```xml
<configuration>
  <property>
    <name>dfs.provided.aliasmap.inmemory.batch-size</name>
    <value>500</value>
    <description>
      The batch size when iterating over the database backing the aliasmap
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.inmemory.dnrpc-address</name>
    <value>namenode:rpc-port</value>
    <description>
      The address where the aliasmap server will be running
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.inmemory.leveldb.dir</name>
    <value>/path/to/leveldb/map/dingos.db</value>
    <description>
      The directory where the leveldb files will be kept
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.inmemory.enabled</name>
    <value>true</value>
    <description>Enable the inmemory alias map on the NameNode. Defaults to false.</description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.class</name>
    <value>org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.InMemoryLevelDBAliasMapClient</value>
  </property>
</configuration>
```

### TextFileRegionAliasMap

This alias map implementation stores the mapping from `blockID`s to `FileRegion`s
in a delimited text file. This format is useful for test environments,
particularly single-node.

#### Configuration
```xml
<configuration>
  <property>
    <name>dfs.provided.aliasmap.text.delimiter</name>
    <value>,</value>
    <description>
        The delimiter used when the alias map is specified as
        a text file.
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.text.read.file</name>
    <value>file:///path/to/aliasmap/blocks_blocPoolID.csv</value>
    <description>
        The path specifying the alias map as a text file,
        specified as a URI.
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.text.codec</name>
    <value></value>
    <description>
        The codec used to de-compress the alias map. Default value is empty.
    </description>
  </property>

  <property>
    <name>dfs.provided.aliasmap.text.write.dir</name>
    <value>file:///path/to/aliasmap/</value>
    <description>
        The path to which the alias map should be written as a text
        file, specified as a URI.
    </description>
  </property>
</configuration>
```

