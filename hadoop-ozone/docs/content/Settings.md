---
title: Configuration
weight: 1
menu:
   main:
      parent: Starting
      weight: 2
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




If you are feeling adventurous, you can setup ozone in a real cluster.
Setting up a real cluster requires us to understand the components of Ozone.
Ozone is designed to work concurrently with HDFS. However, Ozone is also
capable of running independently. The components of ozone are the same in both approaches.

## Ozone Components

1. Ozone Manager - Is the server that is in charge of the namespace of Ozone. Ozone Manager is responsible for all volume, bucket and key operations.
2. Storage Container Manager - Acts as the block manager. Ozone Manager
requests blocks from SCM, to which clients can write data.
3. Datanodes - Ozone data node code runs inside the HDFS datanode or in the independent deployment case runs an ozone datanode daemon.




## Setting up an Ozone only cluster

* Please untar the  ozone-0.2.1-SNAPSHOT to the directory where you are going
to run Ozone from. We need Ozone jars on all machines in the cluster. So you
need to do this on all machines in the cluster.

* Ozone relies on a configuration file called ```ozone-site.xml```. To
generate a template that you can replace with proper values, please run the
following command. This will generate a template called ```ozone-site.xml``` at
the specified path (directory).

{{< highlight bash >}}
ozone genconf -output <path>
{{< /highlight >}}

Let us look at the settings inside the generated file (ozone-site.xml)  and
how they control ozone. Once the right values are defined, this file
needs to be copied to ```ozone directory/etc/Hadoop```.


* **ozone.enabled** This is the most critical setting for ozone.
Ozone is a work in progress and users have to enable this service explicitly.
By default, Ozone is disabled. Setting this flag to `true` enables ozone in the
HDFS or Ozone cluster.

Here is an example,

{{< highlight xml >}}
    <property>
       <name>ozone.enabled</name>
       <value>True</value>
    </property>
{{< /highlight >}}

* **ozone.metadata.dirs** Allows Administrators to specify where the
 metadata must reside. Usually you pick your fastest disk (SSD if
 you have them on your nodes). OzoneManager, SCM and datanode will  write the
 metadata to this path. This is a required setting, if this is missing Ozone
 will fail to come up.

  Here is an example,

{{< highlight xml >}}
   <property>
      <name>ozone.metadata.dirs</name>
      <value>/data/disk1/meta</value>
   </property>
{{< /highlight >}}

*  **ozone.scm.names**  Storage container manager(SCM) is a distributed block
  service which is used by ozone. This property allows data nodes to discover
   SCM's address. Data nodes send heartbeat to SCM.
   Until HA  feature is  complete, we configure ozone.scm.names to be a
   single machine.

  Here is an example,

  {{< highlight xml >}}
      <property>
        <name>ozone.scm.names</name>
        <value>scm.hadoop.apache.org</value>
      </property>
  {{< /highlight >}}

 * **ozone.scm.datanode.id** Data nodes generate a Unique ID called Datanode
 ID. This identity is written to the file specified by this path. *Data nodes
  will create this path if it doesn't exist already.*

Here is an  example,
{{< highlight xml >}}
   <property>
      <name>ozone.scm.datanode.id</name>
      <value>/data/disk1/meta/node/datanode.id</value>
   </property>
{{< /highlight >}}

* **ozone.om.address** OM server address. This is used by OzoneClient and
Ozone File System.

Here is an  example,
{{< highlight xml >}}
    <property>
       <name>ozone.om.address</name>
       <value>ozonemanager.hadoop.apache.org</value>
    </property>
{{< /highlight >}}


### Ozone Settings Summary

| Setting                        | Value                        | Comment |
|--------------------------------|------------------------------|------------------------------------------------------------------|
| ozone.enabled                  | true                         | This enables SCM and  containers in HDFS cluster.                |
| ozone.metadata.dirs            | file path                    | The metadata will be stored here.                                |
| ozone.scm.names                | SCM server name              | Hostname:port or IP:port address of SCM.                      |
| ozone.scm.block.client.address | SCM server name and port     | Used by services like OM                                         |
| ozone.scm.client.address       | SCM server name and port     | Used by client-side                                              |
| ozone.scm.datanode.address     | SCM server name and port     | Used by datanode to talk to SCM                                  |
| ozone.om.address               | OM server name               | Used by Ozone handler and Ozone file system.                     |
