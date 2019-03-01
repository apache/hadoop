---
title: Architecture
date: "2017-10-10"
menu: main
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

Ozone is a redundant, distributed object store build by
leveraging primitives present in HDFS. The primary design point of ozone is scalability, and it aims to scale to billions of objects.

Ozone consists of volumes, buckets, and keys. A volume is similar to a home directory in the ozone world. Only an administrator can create it. Volumes are used to store buckets. Once a volume is created users can create as many buckets as needed. Ozone stores data as keys which live inside these buckets.

Ozone namespace is composed of many storage volumes. Storage volumes are also used as the basis for storage accounting.

To access a key, an Ozone URL has the following format:

```
http://servername:port/volume/bucket/key
```

Where the server name is the name of a data node, the port is the data node HTTP port. The volume represents the name of the ozone volume; bucket is an ozone bucket created by the user and key represents the file.

Please look at the [command line interface]({{< ref "CommandShell.md#shell" >}})  for more info.

Ozone supports both (S3 compatible) REST and RPC protocols. Clients can choose either of these protocols to communicate with Ozone. Please see the [client documentation]({{< ref "JavaApi.md" >}}) for more details.

Ozone separates namespace management and block space management; this helps
ozone to scale much better. The namespace is managed by a daemon called
[Ozone Manager ]({{< ref "OzoneManager.md" >}}) (OM),  and block space is
managed by [Storage Container Manager] ({{< ref "Hdds.md" >}}) (SCM).

The data nodes provide replication and ability to store blocks; these blocks are stored in groups to reduce the metadata pressure on SCM. This groups of blocks are called storage containers. Hence the block manager is called storage container
manager.

Ozone Overview
--------------

 The following diagram is a high-level overview of the core components of Ozone.  

![Architecture diagram](../../OzoneOverview.svg)

The main elements of Ozone are :

### Ozone Manager 

[Ozone Manager]({{< ref "OzoneManager.md" >}}) (OM) takes care of the Ozone's namespace.
All ozone objects like volumes, buckets, and keys are managed by OM. In Short, OM is the metadata manager for Ozone.
OM talks to blockManager(SCM) to get blocks and passes it on to the Ozone
client.  Ozone client writes data to these blocks.
OM will eventually be replicated via Apache Ratis for High Availability. 

### Storage Container Manager

[Storage Container Manager]({{< ref "Hdds.md" >}}) (SCM) is the block and cluster manager for Ozone.
SCM along with data nodes offer a service called 'storage containers'.
A storage container is a group unrelated of blocks that are managed together as a single entity.

SCM offers the following abstractions.  

![SCM Abstractions](../../SCMBlockDiagram.png)

### Blocks
Blocks are similar to blocks in HDFS. They are replicated store of data. Client writes data to blocks.

### Containers
A collection of blocks replicated and managed together.

### Pipelines
SCM allows each storage container to choose its method of replication.
For example, a storage container might decide that it needs only one copy of a  block
and might choose a stand-alone pipeline. Another storage container might want to have a very high level of reliability and pick a RATIS based pipeline. In other words, SCM allows different kinds of replication strategies to co-exist. The client while writing data, chooses a storage container with required properties.

### Pools
A group of data nodes is called a pool. For scaling purposes,
we define a pool as a set of machines. This makes management of data nodes easier.

### Nodes
The data node where data is stored. SCM monitors these nodes via heartbeat.

### Clients
Ozone ships with a set of clients. Ozone [CLI]({{< ref "CommandShell.md#shell" >}}) is the command line interface like 'hdfs' command.  [Freon] ({{< ref "Freon.md" >}}) is a  load generation tool for Ozone. 

## S3 gateway

Ozone provides and [S3 compatible REST gateway server]({{< ref "S3.md">}}). All of the main S3 features are supported and any S3 compatible client library can be used.

### Ozone File System
[Ozone file system]({{< ref "OzoneFS.md">}}) is a Hadoop compatible file system. This allows Hadoop services and applications like Hive and Spark to run against
Ozone without any change. (For example: you can use `hdfs dfs -ls o3fs://...` instead of `hdfs dfs -ls hdfs://...`)

### Ozone Client
This is similar to DFSClient in HDFS. This is the standard client to talk to Ozone. All other components that we have discussed so far rely on Ozone client. Ozone client supports RPC protocol.
