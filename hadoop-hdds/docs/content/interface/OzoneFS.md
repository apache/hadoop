---
title: Ozone File System
date: 2017-09-14
weight: 2
summary: Hadoop Compatible file system allows any application that expects an HDFS like interface to work against Ozone with zero changes. Frameworks like Apache Spark, YARN and Hive work against Ozone without needing any change.
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

The Hadoop compatible file system interface allpws storage backends like Ozone
to be easily integrated into Hadoop eco-system.  Ozone file system is an
Hadoop compatible file system.

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


## Supporting older Hadoop version (Legacy jar, BasicOzoneFilesystem)

There are two ozonefs files, both of them include all the dependencies:

 * share/ozone/lib/hadoop-ozone-filesystem-lib-current-VERSION.jar
 * share/ozone/lib/hadoop-ozone-filesystem-lib-legacy-VERSION.jar

The first one contains all the required dependency to use ozonefs with a
 compatible hadoop version (hadoop 3.2).

The second one contains all the dependency in an internal, separated directory,
 and a special class loader is used to load all the classes from the location.

With this method the hadoop-ozone-filesystem-lib-legacy.jar can be used from
 any older hadoop version (eg. hadoop 3.1, hadoop 2.7 or spark+hadoop 2.7)

Similar to the dependency jar, there are two OzoneFileSystem implementation.

For hadoop 3.0 and newer, you can use `org.apache.hadoop.fs.ozone.OzoneFileSystem`
 which is a full implementation of the Hadoop compatible File System API.

For Hadoop 2.x you should use the Basic version: `org.apache.hadoop.fs.ozone.BasicOzoneFileSystem`.

This is the same implementation but doesn't include the features/dependencies which are added with
 Hadoop 3.0. (eg. FS statistics, encryption zones).

### Summary

The following table summarize which jar files and implementation should be used:

Hadoop version | Required jar            | OzoneFileSystem implementation
---------------|-------------------------|----------------------------------------------------
3.2            | filesystem-lib-current  | org.apache.hadoop.fs.ozone.OzoneFileSystem
3.1            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.OzoneFileSystem
2.9            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.BasicOzoneFileSystem
2.7            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.BasicOzoneFileSystem
 With this method the hadoop-ozone-filesystem-lib-legacy.jar can be used from
 any older hadoop version (eg. hadoop 2.7 or spark+hadoop 2.7)
