---
title: Running concurrently with HDFS
linktitle: Runing with HDFS
weight: 1
summary: Ozone is designed to run concurrently with HDFS. This page explains how to deploy Ozone in a exisiting HDFS cluster.
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

Ozone is designed to work with HDFS. So it is easy to deploy ozone in an
existing HDFS cluster.

The container manager part of Ozone can run inside DataNodes as a pluggable module
or as a standalone component. This document describe how can it be started as
a HDFS datanode plugin.

To activate ozone you should define the service plugin implementation class.

<div class="alert alert-warning" role="alert">
<b>Important</b>: It should be added to the <b>hdfs-site.xml</b> as the plugin should
be activated as part of the normal HDFS Datanode bootstrap.
</div>

{{< highlight xml >}}
<property>
   <name>dfs.datanode.plugins</name>
   <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
</property>
{{< /highlight >}}

You also need to add the ozone-datanode-plugin jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/hadoop/ozoneplugin/hadoop-ozone-datanode-plugin.jar
{{< /highlight >}}



To start ozone with HDFS you should start the the following components:

 1. HDFS Namenode (from Hadoop distribution)
 2. HDFS Datanode (from the Hadoop distribution with the plugin on the
 classpath from the Ozone distribution)
 3. Ozone Manager (from the Ozone distribution)
 4. Storage Container manager (from the Ozone distribution)

Please check the log of the datanode whether the HDDS/Ozone plugin is started or
not. Log of datanode should contain something like this:

```
2018-09-17 16:19:24 INFO  HddsDatanodeService:158 - Started plug-in org.apache.hadoop.ozone.web.OzoneHddsDatanodeService@6f94fb9d
```

<div class="alert alert-warning" role="alert">
<b>Note:</b> The current version of Ozone is tested with Hadoop 3.1.
</div>
