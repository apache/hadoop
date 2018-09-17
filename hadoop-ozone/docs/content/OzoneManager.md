---
title: "Ozone Manager"
date: "2017-09-14"
menu:
   main:
       parent: Architecture
weight: 11
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

OM Overview
-------------

Ozone Manager or OM is the namespace manager for Ozone. The clients (RPC clients, Rest proxy, Ozone file system, etc.) communicate with OM to create and delete various ozone objects.

Each ozone volume is the root of a namespace under OM. This is very different from HDFS which provides a single rooted file system.

Ozone's namespace is a collection of volumes or is a forest instead of a
single rooted tree as in HDFS. This property makes it easy to deploy multiple
 OMs for scaling, this feature is under development.

OM Metadata
-----------------

Conceptually, OM maintains a list of volumes, buckets, and keys. For each user, it maintains a list of volumes. For each volume, the list of buckets and for each bucket the list of keys.

Right now, OM is a single instance service. Ozone already relies on Apache Ratis (A Replicated State Machine based on Raft protocol). OM will be extended to replicate all its metadata via Ratis. With that, OM will be highly available.

OM UI
------------

OM supports a simple UI for the time being. The default port of OM is 9874. To access the OM UI, the user can connect to http://OM:port or for a concrete example,
```
http://omserver:9874/
```
OM UI primarily tries to measure load and latency of OM. The first section of OM UI relates to the number of operations seen by the cluster broken down by the object, operation and whether the operation was successful.

The latter part of the UI is focused on latency and number of operations that OM is performing.

One of the hardest problems in HDFS world is discovering the numerous settings offered to tune HDFS. Ozone solves that problem by tagging the configs. To discover settings, click on "Common Tools"->Config.  This will take you to the ozone config UI.

Config UI
------------

The ozone config UI is a matrix with row representing the tags, and columns representing All, OM and SCM.

Suppose a user wanted to discover the required settings for ozone. Then the user can tick the checkbox that says "Required."
This will filter out all "Required" settings along with the description of what each setting does.

The user can combine different checkboxes and UI will combine the results. That is, If you have more than one row selected, then all keys for those chosen tags are displayed together.

We are hopeful that this leads to a more straightforward way of discovering settings that manage ozone.


OM and SCM
-------------------
[Storage container manager]({{< ref "Hdds.md" >}}) or (SCM) is the block manager
 for ozone. When a client requests OM for a set of data nodes to write data, OM talk to SCM and gets a block.

A block returned by SCM contains a pipeline, which is a set of nodes that we participate in that block replication.

So OM is dependent on SCM for reading and writing of Keys. However, OM is independent of SCM while doing metadata operations like ozone volume or bucket operations.
