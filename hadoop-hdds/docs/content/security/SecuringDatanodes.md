---
title: "Securing Datanodes"
date: "2019-April-03"
weight: 2
summary:  Explains different modes of securing data nodes. These range from kerberos to auto approval.
icon: th
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


Datanodes under Hadoop is traditionally secured by creating a Keytab file on
the data nodes. With Ozone, we have moved away to using data node
certificates. That is, Kerberos on data nodes is not needed in case of a
secure Ozone cluster.

However, we support the legacy Kerberos based Authentication to make it easy
for the current set of users.The HDFS configuration keys are the following
that is setup in  hdfs-site.xml.

Property|Example Value|Comment
--------|--------------|--------------
dfs.datanode.keytab.file| /keytab/dn.service.keytab| Keytab file.
dfs.datanode.kerberos.principal| dn/_HOST@REALM.TLD|  principal name.

## How a data node becomes secure.

Under Ozone, when a data node boots up and discovers SCM's address, the first
thing that data node does is to create a private key and send a certificate
request to the SCM.

<h3>Certificate Approval via Kerberos <span class="badge badge-secondary">Current Model</span></h3>
SCM has a built-in CA, and SCM has to approve this request. If the data node
already has a Kerberos key tab, then SCM will trust Kerberos credentials and
issue a certificate automatically.


<h3>Manual Approval <span class="badge badge-primary">In Progress</span></h3>
If these are band new data nodes and Kerberos key tabs are not present at the
data nodes, then this request for the data nodes identity certificate is
queued up for approval from the administrator(This is work in progress,
not committed in Ozone yet). In other words, the web of trust is established
by the administrator of the cluster.

<h3>Automatic Approval <span class="badge badge-secondary">In Progress</span></h3>
If you running under an container orchestrator like  Kubernetes, we rely on
Kubernetes to create a one-time token that will be given to data node during
boot time to prove the identity of the data node container (This is also work
in progress.)


Once a certificate is issued, a Data node is secure and Ozone manager can
issue block tokens. If there is no data node certificates or the SCM's root
certificate is not present in the data node, then data node will register
itself and down load the SCM's root certificate as well get the certificates
for itself.
