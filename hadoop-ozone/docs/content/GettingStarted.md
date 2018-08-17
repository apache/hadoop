---
title: Getting started
weight: -2
menu: main
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone - Object store for Apache Hadoop


## Introduction

Ozone is a scalable distributed object store for Hadoop. Ozone supports RPC
and REST APIs for working with Volumes, Buckets and Keys.

Existing Hadoop applications can use Ozone transparently via a Hadoop Compatible
FileSystem shim.

### Basic terminology
1. **Volumes** - Volumes are a notion similar to accounts. Volumes can be
created or deleted only by administrators.
1. **Buckets** - A volume can contain zero or more buckets.
1. **Keys** - Keys are unique within a given bucket.

### Services in a minimal Ozone cluster
1. **Ozone Manager (OM)** - stores Ozone Metadata namely Volumes,
Buckets and Key names.
1. **Storage Container Manager (SCM)** - handles Storage Container lifecycle.
Containers are the unit of replication in Ozone and not exposed to users.
1. **DataNodes** - These are HDFS DataNodes which understand how to store
Ozone Containers. Ozone has been designed to efficiently share storage space
with HDFS blocks.

## Getting Started

Ozone is currently work-in-progress and lives in the Hadoop source tree.
The sub-projects (`hadoop-ozone` and `hadoop-hdds`) are part of
the Hadoop source tree but they are not compiled by default and not
part of official Apache Hadoop releases.

To use Ozone, you have to build a package by yourself and deploy a cluster.

### Building Ozone

To build Ozone, please checkout the Hadoop sources from the
[Apache Hadoop git repo](https://git-wip-us.apache.org/repos/asf?p=hadoop.git).
Then checkout the `trunk` branch and build it with the `hdds` profile enabled.

`
git checkout trunk
mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true -Pdist -Phdds -Dtar -DskipShade
`

`skipShade` is just to make compilation faster and not required.

This builds a tarball in your distribution directory which can be used to deploy your
Ozone cluster. The tarball path is `hadoop-dist/target/ozone-${project.version}.tar.gz`.

At this point you can either setup a physical cluster or run Ozone via
docker.

### Running Ozone via Docker

This is the quickest way to bring up an Ozone cluster for development/testing
or if you just want to get a feel for Ozone. It assumes that you have docker installed
on the machine.

Go to the directory where the docker compose files exist and tell
`docker-compose` to start Ozone. This will start SCM, OM and a single datanode
in the background.
```
cd hadoop-dist/target/ozone/compose/ozone

docker-compose up -d
```

Now let us run some workload against Ozone. To do that we will run
_freon_, the Ozone load generator after logging into one of the docker
containers for OM, SCM or DataNode. Let's take DataNode for example:.
```
docker-compose exec datanode bash

ozone freon -mode offline -validateWrites -numOfVolumes 1 -numOfBuckets 10 -numOfKeys 100
```

You can checkout the OM UI to see the requests information.
```
http://localhost:9874/
```

If you need more datanodes you can scale up:
```
docker-compose up --scale datanode=3 -d
```

## Running Ozone using a real cluster

### Configuration

First initialize Hadoop cluster configuration files like hadoop-env.sh,
core-site.xml, hdfs-site.xml and any other configuration files that are
needed for your cluster.

#### Update hdfs-site.xml

The container manager part of Ozone runs inside DataNodes as a pluggable module.
To activate ozone you should define the service plugin implementation class.
**Important**: It should be added to the **hdfs-site.xml** as the plugin should
be activated as part of the normal HDFS Datanode bootstrap.
```
<property>
   <name>dfs.datanode.plugins</name>
   <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
</property>
```


#### Create ozone-site.xml

Ozone relies on its own configuration file called `ozone-site.xml`.
The following are the most important settings.

 1. _*ozone.enabled*_  This is the most important setting for ozone.
 Currently, Ozone is an opt-in subsystem of HDFS. By default, Ozone is
 disabled. Setting this flag to `true` enables ozone in the HDFS cluster.
 Here is an example,
    ```
    <property>
       <name>ozone.enabled</name>
       <value>True</value>
    </property>
    ```
 1.  **ozone.metadata.dirs** Administrators can specify where the
 metadata must reside. Usually you pick your fastest disk (SSD if
 you have them on your nodes). OM, SCM and datanode will write the metadata
 to these disks. This is a required setting, if this is missing Ozone will
 fail to come up. Here is an example,
    ```
   <property>
      <name>ozone.metadata.dirs</name>
      <value>/data/disk1/meta</value>
   </property>
    ```

1. **ozone.scm.names** Ozone is build on top of container framework. Storage
 container manager(SCM) is a distributed block service which is used by ozone
 and other storage services.
 This property allows datanodes to discover where SCM is, so that
 datanodes can send heartbeat to SCM. SCM is designed to be highly available
 and datanodes assume there are multiple instances of SCM which form a highly
 available ring. The HA feature of SCM is a work in progress. So we
 configure ozone.scm.names to be a single machine. Here is an example,
    ```
    <property>
      <name>ozone.scm.names</name>
      <value>scm.hadoop.apache.org</value>
    </property>
    ```

1. **ozone.scm.datanode.id** Each datanode that speaks to SCM generates an ID
just like HDFS.  This is a mandatory setting. Please note:
This path will be created by datanodes if it doesn't exist already. Here is an
 example,
    ```
   <property>
      <name>ozone.scm.datanode.id</name>
      <value>/data/disk1/scm/meta/node/datanode.id</value>
   </property>
    ```

1. **ozone.scm.block.client.address** Storage Container Manager(SCM) offers a
 set of services that can be used to build a distributed storage system. One
 of the services offered is the block services. OM and HDFS would use this
 service. This property describes where OM can discover SCM's block service
 endpoint. There is corresponding ports etc, but assuming that we are using
 default ports, the server address is the only required field. Here is an
 example,
    ```
    <property>
      <name>ozone.scm.block.client.address</name>
      <value>scm.hadoop.apache.org</value>
    </property>
    ```

1. **ozone.om.address** OM server address. This is used by OzoneClient and
Ozone File System.
    ```
    <property>
       <name>ozone.om.address</name>
       <value>om.hadoop.apache.org</value>
    </property>
    ```

#### Ozone Settings Summary

| Setting                        | Value                        | Comment |
|--------------------------------|------------------------------|------------------------------------------------------------------|
| ozone.enabled                  | True                         | This enables SCM and  containers in HDFS cluster.                |
| ozone.metadata.dirs            | file path                    | The metadata will be stored here.                                |
| ozone.scm.names                | SCM server name              | Hostname:port or or IP:port address of SCM.                      |
| ozone.scm.block.client.address | SCM server name and port     | Used by services like OM                                         |
| ozone.scm.client.address       | SCM server name and port     | Used by client side                                              |
| ozone.scm.datanode.address     | SCM server name and port     | Used by datanode to talk to SCM                                  |
| ozone.om.address               | OM server name               | Used by Ozone handler and Ozone file system.                     |


#### Sample ozone-site.xml

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
      <name>ozone.enabled</name>
      <value>True</value>
    </property>

    <property>
      <name>ozone.metadata.dirs</name>
      <value>/data/disk1/ozone/meta</value>
    </property>

    <property>
      <name>ozone.scm.names</name>
      <value>127.0.0.1</value>
    </property>

    <property>
       <name>ozone.scm.client.address</name>
       <value>127.0.0.1:9860</value>
    </property>

     <property>
       <name>ozone.scm.block.client.address</name>
       <value>127.0.0.1:9863</value>
     </property>

     <property>
       <name>ozone.scm.datanode.address</name>
       <value>127.0.0.1:9861</value>
     </property>

     <property>
       <name>ozone.om.address</name>
       <value>127.0.0.1:9874</value>
     </property>
</configuration>
```



### Starting Ozone

Ozone is designed to run concurrently with HDFS. The simplest way to [start
HDFS](../hadoop-common/ClusterSetup.html) is to run `start-dfs.sh` from the
`$HADOOP/sbin/start-dfs.sh`. Once HDFS
is running, please verify it is fully functional by running some commands like

   - *./hdfs dfs -mkdir /usr*
   - *./hdfs dfs -ls /*

 Once you are sure that HDFS is running, start Ozone. To start  ozone, you
 need to start SCM and OM.

The first time you bring up Ozone, SCM must be initialized.
```
ozone scm -init
```

Start SCM.
```
ozone --daemon start scm
```

Once SCM gets started, OM must be initialized.
```
ozone om -createObjectStore
```

Start OM.
```
ozone --daemon start om
```

If you would like to start HDFS and Ozone together, you can do that by running
 a single command.
```
$HADOOP/sbin/start-ozone.sh
```

This command will start HDFS and then start the ozone components.

Once you have ozone running you can use these ozone [shell](./OzoneCommandShell.html)
commands to start creating a  volume, bucket and keys.

## Diagnosing issues

Ozone tries not to pollute the existing HDFS streams of configuration and
logging. So ozone logs are by default configured to be written to a file
called `ozone.log`. This is controlled by the settings in `log4j.properties`
file in the hadoop configuration directory.

Here is the log4j properties that are added by ozone.


```
   #
   # Add a logger for ozone that is separate from the Datanode.
   #
   #log4j.debug=true
   log4j.logger.org.apache.hadoop.ozone=DEBUG,OZONE,FILE

   # Do not log into datanode logs. Remove this line to have single log.
   log4j.additivity.org.apache.hadoop.ozone=false

   # For development purposes, log both to console and log file.
   log4j.appender.OZONE=org.apache.log4j.ConsoleAppender
   log4j.appender.OZONE.Threshold=info
   log4j.appender.OZONE.layout=org.apache.log4j.PatternLayout
   log4j.appender.OZONE.layout.ConversionPattern=%d{ISO8601} [%t] %-5p \
    %X{component} %X{function} %X{resource} %X{user} %X{request} - %m%n

   # Real ozone logger that writes to ozone.log
   log4j.appender.FILE=org.apache.log4j.DailyRollingFileAppender
   log4j.appender.FILE.File=${hadoop.log.dir}/ozone.log
   log4j.appender.FILE.Threshold=debug
   log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
   log4j.appender.FILE.layout.ConversionPattern=%d{ISO8601} [%t] %-5p \
     (%F:%L) %X{function} %X{resource} %X{user} %X{request} - \
      %m%n
```

If you would like to have a single datanode log instead of ozone stuff
getting written to ozone.log, please remove this line or set this to true.
```
log4j.additivity.org.apache.hadoop.ozone=false
```

On the SCM/OM side, you will be able to see
1. `hadoop-hdfs-om-hostname.log`
1. `hadoop-hdfs-scm-hostname.log`

## Reporting Bugs
Please file any issues you see under [Apache HDDS Project Jira](https://issues.apache.org/jira/projects/HDDS/issues/).

## References
 - [Object store in HDFS: HDFS-7240](https://issues.apache.org/jira/browse/HDFS-7240)
 - [Ozone File System: HDFS-13074](https://issues.apache.org/jira/browse/HDFS-13074)
 - [Building HDFS on top of new storage layer (HDDS): HDFS-10419](https://issues.apache.org/jira/browse/HDFS-10419)
