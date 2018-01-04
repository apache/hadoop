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

ResourceManager Restart
======================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

ResourceManager is the central authority that manages resources and schedules applications running on YARN. Hence, it is potentially a single point of failure in an Apache YARN cluster. This document gives an overview of ResourceManager Restart, a feature that enhances ResourceManager to keep functioning across restarts and also makes ResourceManager down-time invisible to end-users.

There are two types of restart for ResourceManager:

* **Non-work-preserving RM restart**: This restart enhances RM to persist application/attempt state and other credentials information in a pluggable state-store. RM will reload this information from state-store on restart and re-kick the previously running applications. Users are not required to re-submit the applications.

* **Work-preserving RM restart**: This focuses on re-constructing the running state of RM by combining the container status from NodeManagers and container requests from ApplicationMasters on restart. The key difference from Non-work-preserving RM restart is that previously running applications will not be killed after RM restarts, and so applications will not lose its work because of RM outage.

Feature
-------

* **Non-work-preserving RM restart**

     In non-work-preserving RM restart, RM will save the application metadata (i.e. ApplicationSubmissionContext) in a pluggable state-store when client submits an application and also saves the final status of the application such as the completion state (failed, killed, or finished) and diagnostics when the application completes. Besides, RM also saves the credentials like security keys, tokens to work in a secure environment. When RM shuts down, as long as the required information (i.e.application metadata and the alongside credentials if running in a secure environment) is available in the state-store, then when RM restarts, it can pick up the application metadata from the state-store and re-submit the application. RM won't re-submit the applications if they were already completed (i.e. failed, killed, or finished) before RM went down.

     NodeManagers and clients during the down-time of RM will keep polling RM until RM comes up. When RM comes up, it will send a re-sync command to all the NodeManagers and ApplicationMasters it was talking to via heartbeats. The NMs will kill all its managed containers and re-register with RM. These re-registered NodeManagers are similar to the newly joining NMs. AMs (e.g. MapReduce AM) are expected to shutdown when they receive the re-sync command. After RM restarts and loads all the application metadata, credentials from state-store and populates them into memory, it will create a new attempt (i.e. ApplicationMaster) for each application that was not yet completed and re-kick that application as usual. As described before, the previously running applications' work is lost in this manner since they are essentially killed by RM via the re-sync command on restart.


* **Work-preserving RM restart**

     In work-preserving RM restart, RM ensures the persistency of application state and reload that state on recovery, this restart primarily focuses on re-constructing the entire running state of YARN cluster, the majority of which is the state of the central scheduler inside RM which keeps track of all containers' life-cycle, applications' headroom and resource requests, queues' resource usage and so on. In this way, RM need not kill the AM and re-run the application from scratch as it is done in non-work-preserving RM restart. Applications can simply re-sync back with RM and resume from where it were left off.

     RM recovers its running state by taking advantage of the container status sent from all NMs. NM will not kill the containers when it re-syncs with the restarted RM. It continues managing the containers and sends the container status across to RM when it re-registers. RM reconstructs the container instances and the associated applications' scheduling status by absorbing these containers' information. In the meantime, AM needs to re-send the outstanding resource requests to RM because RM may lose the unfulfilled requests when it shuts down. Application writers using AMRMClient library to communicate with RM do not need to worry about the part of AM re-sending resource requests to RM on re-sync, as it is automatically taken care by the library itself.


Configurations
--------------

This section describes the configurations involved to enable RM Restart feature.

### Enable RM Restart

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.recovery.enabled` | `true` |

### Configure the state-store for persisting the RM state

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.store.class` | The class name of the state-store to be used for saving application/attempt state and the credentials. The available state-store implementations are `org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore`, a ZooKeeper based state-store implementation and `org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore`, a Hadoop FileSystem based state-store implementation like HDFS and local FS. `org.apache.hadoop.yarn.server.resourcemanager.recovery.LeveldbRMStateStore`, a LevelDB based state-store implementation. The default value is set to `org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore`. |

### How to choose the state-store implementation

   * **ZooKeeper based state-store**: User is free to pick up any storage to set up RM restart, but must use ZooKeeper based state-store to support RM HA. The reason is that only ZooKeeper based state-store supports fencing mechanism to avoid a split-brain situation where multiple RMs assume they are active and can edit the state-store at the same time.

   * **FileSystem based state-store**: HDFS and local FS based state-store are supported. Fencing mechanism is not supported.

   * **LevelDB based state-store**: LevelDB based state-store is considered more light weight than HDFS and ZooKeeper based state-store. LevelDB supports better atomic operations, fewer I/O ops per state update,
    and far fewer total files on the filesystem. Fencing mechanism is not supported.

### Configurations for Hadoop FileSystem based state-store implementation

   Support both HDFS and local FS based state-store implementation. The type of file system to be used is determined by the scheme of URI. e.g. `hdfs://localhost:9000/rmstore` uses HDFS as the storage and `file:///tmp/yarn/rmstore` uses local FS as the storage. If no scheme(`hdfs://` or `file://`) is specified in the URI, the type of storage to be used is determined by `fs.defaultFS` defined in `core-site.xml`.

* Configure the URI where the RM state will be saved in the Hadoop FileSystem state-store.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.fs.state-store.uri` | URI pointing to the location of the FileSystem path where RM state will be stored (e.g. hdfs://localhost:9000/rmstore). Default value is `${hadoop.tmp.dir}/yarn/system/rmstore`. If FileSystem name is not provided, `fs.default.name` specified in **conf/core-site.xml* will be used. |

* Configure the retry policy state-store client uses to connect with the Hadoop FileSystem.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.fs.state-store.retry-policy-spec` | Hadoop FileSystem client retry policy specification. Hadoop FileSystem client retry is always enabled. Specified in pairs of sleep-time and number-of-retries i.e. (t0, n0), (t1, n1), ..., the first n0 retries sleep t0 milliseconds on average, the following n1 retries sleep t1 milliseconds on average, and so on. Default value is (2000, 500) |

### Configurations for ZooKeeper based state-store implementation

* Configure the ZooKeeper server address and the root path where the RM state is stored.

| Property | Description |
|:---- |:---- |
| `hadoop.zk.address` | Comma separated list of Host:Port pairs. Each corresponds to a ZooKeeper server (e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002") to be used by the RM for storing RM state. |
| `yarn.resourcemanager.zk-state-store.parent-path` | The full path of the root znode where RM state will be stored. Default value is /rmstore. |

* Configure the retry policy state-store client uses to connect with the ZooKeeper server.

| Property | Description |
|:---- |:---- |
| `hadoop.zk.num-retries` | Number of times RM tries to connect to ZooKeeper server if the connection is lost. Default value is 500. |
| `hadoop.zk.retry-interval-ms` | The interval in milliseconds between retries when connecting to a ZooKeeper server. Default value is 2 seconds. |
| `hadoop.zk.timeout-ms` | ZooKeeper session timeout in milliseconds. This configuration is used by the ZooKeeper server to determine when the session expires. Session expiration happens when the server does not hear from the client (i.e. no heartbeat) within the session timeout period specified by this configuration. Default value is 10 seconds |

* Configure the ACLs to be used for setting permissions on ZooKeeper znodes.

| Property | Description |
|:---- |:---- |
| `hadoop.zk.acl` | ACLs to be used for setting permissions on ZooKeeper znodes. Default value is `world:anyone:rwcda` |

### Configurations for LevelDB based state-store implementation

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.leveldb-state-store.path` | Local path where the RM state will be stored. Default value is `${hadoop.tmp.dir}/yarn/system/rmstore` |

### Configurations for work-preserving RM recovery

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms` | Set the amount of time RM waits before allocating new containers on RM work-preserving recovery. Such wait period gives RM a chance to settle down resyncing with NMs in the cluster on recovery, before assigning new containers to applications.|

Notes
-----

ContainerId string format is changed if RM restarts with work-preserving recovery enabled. It used to be such format:
`Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}`, e.g. `Container_1410901177871_0001_01_000005`.

It is now changed to:
`Container_`**e{epoch}**`_{clusterTimestamp}_{appId}_{attemptId}_{containerId}`, e.g. `Container_`**e17**`_1410901177871_0001_01_000005`.

Here, the additional epoch number is a monotonically increasing integer which starts from 0 and is increased by 1 each time RM restarts. If epoch number is 0, it is omitted and the containerId string format stays the same as before.

Sample Configurations
---------------------

Below is a minimum set of configurations for enabling RM work-preserving restart using ZooKeeper based state store.


     <property>
       <description>Enable RM to recover state after starting. If true, then
       yarn.resourcemanager.store.class must be specified</description>
       <name>yarn.resourcemanager.recovery.enabled</name>
       <value>true</value>
     </property>

     <property>
       <description>The class to use as the persistent store.</description>
       <name>yarn.resourcemanager.store.class</name>
       <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore</value>
     </property>

     <property>
       <description>Comma separated list of Host:Port pairs. Each corresponds to a ZooKeeper server
       (e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002") to be used by the RM for storing RM state.
       This must be supplied when using org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore
       as the value for yarn.resourcemanager.store.class</description>
       <name>hadoop.zk.address</name>
       <value>127.0.0.1:2181</value>
     </property>


