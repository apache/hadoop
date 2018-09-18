---
title: Ozone File System
date: 2017-09-14
menu: main
menu:
   main:
      parent: Client
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

There are many Hadoop compatible files systems under Hadoop. Hadoop compatible file systems ensures that storage backends like Ozone can easily be integrated into Hadoop eco-system.

## Setting up the Ozone file system

To create an ozone file system, we have to choose a bucket where the file system would live. This bucket will be used as the backend store for OzoneFileSystem. All the files and directories will be stored as keys in this bucket.

Please run the following commands to create a volume and bucket, if you don't have them already.

{{< highlight bash >}}
ozone sh volume create /volume
ozone sh bucket create /volume/bucket
{{< /highlight >}}

Once this is created, please make sure that bucket exists via the listVolume or listBucket commands.

Please add the following entry to the core-site.xml.

{{< highlight xml >}}
<property>
  <name>fs.o3.impl</name>
  <value>org.apache.hadoop.fs.ozone.OzoneFileSystem</value>
</property>
<property>
  <name>fs.default.name</name>
  <value>o3://localhost:9864/volume/bucket</value>
</property>
{{< /highlight >}}

This will make this bucket to be the default file system for HDFS dfs commands and register the o3 file system type..

You also need to add the ozone-filesystem.jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/hadoop/ozonefs/hadoop-ozone-filesystem.jar:$HADOOP_CLASSPATH
{{< /highlight >}}




Once the default Filesystem has been setup, users can run commands like ls, put, mkdir, etc.
For example,

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

or

{{< highlight bash >}}
hdfs dfs -mkdir /users
{{< /highlight >}}


Or put command etc. In other words, all programs like Hive, Spark, and Distcp will work against this file system.
Please note that any keys created/deleted in the bucket using methods apart from OzoneFileSystem will show up as diectories and files in the Ozone File System.
