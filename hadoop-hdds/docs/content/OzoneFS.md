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
  <name>fs.o3fs.impl</name>
  <value>org.apache.hadoop.fs.ozone.OzoneFileSystem</value>
</property>
<property>
  <name>fs.defaultFS</name>
  <value>o3fs://bucket.volume</value>
</property>
{{< /highlight >}}

This will make this bucket to be the default file system for HDFS dfs commands and register the o3fs file system type.

You also need to add the ozone-filesystem.jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/hadoop-ozone-filesystem-lib-current*.jar:$HADOOP_CLASSPATH
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
Please note that any keys created/deleted in the bucket using methods apart from OzoneFileSystem will show up as directories and files in the Ozone File System.

Note: Bucket and volume names are not allowed to have a period in them.
Moreover, the filesystem URI can take a fully qualified form with the OM host and port as a part of the path following the volume name.
For example,

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:5678/key
{{< /highlight >}}


## Legacy mode

There are two ozonefs files which includes all the dependencies:

 * share/ozone/lib/hadoop-ozone-filesystem-lib-current-VERSION.jar
 * share/ozone/lib/hadoop-ozone-filesystem-lib-legacy-VERSION.jar

 The first one contains all the required dependency to use ozonefs with a
 compatible hadoop version (hadoop 3.2 / 3.1).

 The second one contains all the dependency in an internal, separated directory,
 and a special class loader is used to load all the classes from the location.

 With this method the hadoop-ozone-filesystem-lib-legacy.jar can be used from
 any older hadoop version (eg. hadoop 2.7 or spark+hadoop 2.7)