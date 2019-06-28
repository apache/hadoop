---
title: "Ozone Manager"
date: "2017-09-14"
weight: 2
summary: Ozone Manager is the principal name space service of Ozone. OM manages the life cycle of volumes, buckets and Keys.
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

Ozone Manager or OM is the namespace manager for Ozone.

This means that when you want to write some data, you ask Ozone
manager for a block and Ozone Manager gives you a block and remembers that
information. When you want to read the that file back, you need to find the
address of the block and Ozone manager returns it you.

Ozone manager also allows users to organize keys under a volume and bucket.
Volumes and buckets are part of the namespace and managed by Ozone Manager.

Each ozone volume is the root of an independent namespace under OM.
This is very different from HDFS which provides a single rooted file system.

Ozone's namespace is a collection of volumes or is a forest instead of a
single rooted tree as in HDFS. This property makes it easy to deploy multiple
OMs for scaling.

## Ozone Manager Metadata

OM maintains a list of volumes, buckets, and keys.
For each user, it maintains a list of volumes.
For each volume, the list of buckets and for each bucket the list of keys.

Ozone Manager will use Apache Ratis(A Raft protocol implementation) to
replicate Ozone Manager state. This will ensure High Availability for Ozone.


## Ozone Manager and Storage Container Manager

The relationship between Ozone Manager and Storage Container Manager is best
understood if we trace what happens during a key write and key read.

### Key Write

* To write a key to Ozone, a client tells Ozone manager that it would like to
write a key into a bucket that lives inside a specific volume. Once Ozone
manager determines that you are allowed to write a key to specified bucket,
OM needs to allocate a block for the client to write data.

* To allocate a block, Ozone manager sends a request to Storage Container
Manager or SCM; SCM is the manager of data nodes. SCM picks three data nodes
into which client can write data. SCM allocates the block and returns the
block ID to Ozone Manager.

* Ozone manager records this block information in its metadata and returns the
block and a block token (a security permission to write data to the block)
the client.

* The client uses the block token to prove that it is allowed to write data to
the block and writes data to the data node.

* Once the write is complete on the data node, the client will update the block
information on
Ozone manager.


### Key Reads

* Key reads are simpler, the client requests the block list from the Ozone
Manager
* Ozone manager will return the block list and block tokens which
allows the client to read the data from nodes.
* Client connects to the data  node and presents the block token and reads
the data from the data node.
