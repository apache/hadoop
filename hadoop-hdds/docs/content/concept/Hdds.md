---
title: "Storage Container Manager"
date: "2017-09-14"
weight: 3
summary:  Storage Container Manager or SCM is the core metadata service of Ozone. SCM provides a distributed block layer for Ozone.
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

Storage container manager provides multiple critical functions for the Ozone
cluster.  SCM acts as the cluster manager, Certificate authority, Block
manager and the replica manager.

{{<card title="Cluster Management" icon="tasks">}}
SCM is in charge of creating an Ozone cluster. When an SCM is booted up via <kbd>init</kbd> command, SCM creates the cluster identity and root certificates needed for the SCM certificate authority. SCM manages the life cycle of a data node in the cluster.
{{</card>}}

{{<card title="Service Identity Management" icon="eye-open">}}
SCM's Ceritificate authority is in
charge of issuing identity certificates for each and every
service in the cluster. This certificate infrastructre makes
it easy to enable mTLS at network layer and also the block
token infrastructure depends on this certificate infrastructure.
{{</card>}}

{{<card title="Block Management" icon="th">}}
SCM is the block manager. SCM
allocates blocks and assigns them to data nodes. Clients
read and write these blocks directly.
{{</card>}}


{{<card title="Replica Management" icon="link">}}
SCM keeps track of all the block
replicas. If there is a loss of data node or a disk, SCM
detects it and instructs data nodes make copies of the
missing blocks to ensure high avialablity.
{{</card>}}
