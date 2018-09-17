---
title: "Hadoop Distributed Data Store"
date: "2017-09-14"
menu:
   main:
       parent: Architecture
weight: 10
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

SCM Overview
------------

Storage Container Manager or SCM is a very important component of ozone. SCM
offers block and container-based services to Ozone Manager.  A container is a
collection of unrelated blocks under ozone. SCM and data nodes work together
to maintain the replication levels needed by the cluster.

It is easier to look at a putKey operation to understand the role that SCM plays.

To put a key, a client makes a call to KSM with the following arguments.

-- putKey(keyName, data, pipeline type, replication count)

1. keyName - refers to the file name.
2. data - The data that the client wants to write.
3. pipeline type - Allows the client to select the pipeline type.  A pipeline
 refers to the replication strategy used for replicating a block.  Ozone
 currently supports Stand Alone and Ratis as two different pipeline types.
4. replication count - This specifies how many copies of the block replica should be maintained.

In most cases, the client does not specify the pipeline type and  replication
 count. The default pipeline type and replication count are used.


Ozone Manager when it receives the putKey call, makes a call to SCM asking
for a pipeline instance with the specified property. So if the client asked
for RATIS replication strategy and a replication count of three, then OM
requests SCM to return a set of data nodes that meet this capability.

If SCM can find this a pipeline ( that is a set of data nodes) that can meet
the requirement from the client, then those nodes are returned to OM. OM will
persist this info and return a tuple consisting of {BlockID, ContainerName, and Pipeline}.

If SCM is not able to find a pipeline, then SCM creates a logical pipeline and then returns it.


SCM manages blocks, containers, and pipelines.  To return healthy pipelines,
SCM also needs to understand the node health. So SCM listens to heartbeats
from data nodes and acts as the node manager too.
