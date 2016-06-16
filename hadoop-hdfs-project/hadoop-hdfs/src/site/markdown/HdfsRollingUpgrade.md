<!--
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

HDFS Rolling Upgrade
====================

* [Introduction](#Introduction)
* [Upgrade](#Upgrade)
    * [Upgrade without Downtime](#Upgrade_without_Downtime)
        * [Upgrading Non-Federated Clusters](#Upgrading_Non-Federated_Clusters)
        * [Upgrading Federated Clusters](#Upgrading_Federated_Clusters)
    * [Upgrade with Downtime](#Upgrade_with_Downtime)
        * [Upgrading Non-HA Clusters](#Upgrading_Non-HA_Clusters)
* [Downgrade and Rollback](#Downgrade_and_Rollback)
* [Downgrade](#Downgrade)
* [Rollback](#Rollback)
* [Commands and Startup Options for Rolling Upgrade](#Commands_and_Startup_Options_for_Rolling_Upgrade)
    * [DFSAdmin Commands](#DFSAdmin_Commands)
        * [dfsadmin -rollingUpgrade](#dfsadmin_-rollingUpgrade)
        * [dfsadmin -getDatanodeInfo](#dfsadmin_-getDatanodeInfo)
        * [dfsadmin -shutdownDatanode](#dfsadmin_-shutdownDatanode)
    * [NameNode Startup Options](#NameNode_Startup_Options)
        * [namenode -rollingUpgrade](#namenode_-rollingUpgrade)


Introduction
------------

*HDFS rolling upgrade* allows upgrading individual HDFS daemons.
For examples, the datanodes can be upgraded independent of the namenodes.
A namenode can be upgraded independent of the other namenodes.
The namenodes can be upgraded independent of datanods and journal nodes.


Upgrade
-------

In Hadoop v2, HDFS supports highly-available (HA) namenode services and wire compatibility.
These two capabilities make it feasible to upgrade HDFS without incurring HDFS downtime.
In order to upgrade a HDFS cluster without downtime, the cluster must be setup with HA.

If there is any new feature which is enabled in new software release, may not work with old software release after upgrade.
In such cases upgrade should be done by following steps.

1. Disable new feature.
2. Upgrade the cluster.
3. Enable the new feature.

Note that rolling upgrade is supported only from Hadoop-2.4.0 onwards.


### Upgrade without Downtime

In a HA cluster, there are two or more *NameNodes (NNs)*, many *DataNodes (DNs)*,
a few *JournalNodes (JNs)* and a few *ZooKeeperNodes (ZKNs)*.
*JNs* is relatively stable and does not require upgrade when upgrading HDFS in most of the cases.
In the rolling upgrade procedure described here,
only *NNs* and *DNs* are considered but *JNs* and *ZKNs* are not.
Upgrading *JNs* and *ZKNs* may incur cluster downtime.

#### Upgrading Non-Federated Clusters

Suppose there are two namenodes *NN1* and *NN2*,
where *NN1* and *NN2* are respectively in active and standby states.
The following are the steps for upgrading a HA cluster:

1. Prepare Rolling Upgrade
    1. Run "[`hdfs dfsadmin -rollingUpgrade prepare`](#dfsadmin_-rollingUpgrade)"
       to create a fsimage for rollback.
    1. Run "[`hdfs dfsadmin -rollingUpgrade query`](#dfsadmin_-rollingUpgrade)"
       to check the status of the rollback image.
       Wait and re-run the command until
       the "`Proceed with rolling upgrade`" message is shown.
1. Upgrade Active and Standby *NNs*
    1. Shutdown and upgrade *NN2*.
    1. Start *NN2* as standby with the
       "[`-rollingUpgrade started`](#namenode_-rollingUpgrade)" option.
    1. Failover from *NN1* to *NN2*
       so that *NN2* becomes active and *NN1* becomes standby.
    1. Shutdown and upgrade *NN1*.
    1. Start *NN1* as standby with the
       "[`-rollingUpgrade started`](#namenode_-rollingUpgrade)" option.
1. Upgrade *DNs*
    1. Choose a small subset of datanodes (e.g. all datanodes under a particular rack).
        1. Run "[`hdfs dfsadmin -shutdownDatanode <DATANODE_HOST:IPC_PORT> upgrade`](#dfsadmin_-shutdownDatanode)"
           to shutdown one of the chosen datanodes.
        1. Run "[`hdfs dfsadmin -getDatanodeInfo <DATANODE_HOST:IPC_PORT>`](#dfsadmin_-getDatanodeInfo)"
           to check and wait for the datanode to shutdown.
        1. Upgrade and restart the datanode.
        1. Perform the above steps for all the chosen datanodes in the subset in parallel.
    1. Repeat the above steps until all datanodes in the cluster are upgraded.
1. Finalize Rolling Upgrade
    1. Run "[`hdfs dfsadmin -rollingUpgrade finalize`](#dfsadmin_-rollingUpgrade)"
       to finalize the rolling upgrade.


#### Upgrading Federated Clusters

In a federated cluster, there are multiple namespaces
and a pair of active and standby *NNs* for each namespace.
The procedure for upgrading a federated cluster is similar to upgrading a non-federated cluster
except that Step 1 and Step 4 are performed on each namespace
and Step 2 is performed on each pair of active and standby *NNs*, i.e.

1. Prepare Rolling Upgrade for Each Namespace
1. Upgrade Active and Standby *NN* pairs for Each Namespace
1. Upgrade *DNs*
1. Finalize Rolling Upgrade for Each Namespace


### Upgrade with Downtime

For non-HA clusters,
it is impossible to upgrade HDFS without downtime since it requires restarting the namenodes.
However, datanodes can still be upgraded in a rolling manner.


#### Upgrading Non-HA Clusters

In a non-HA cluster, there are a *NameNode (NN)*, a *SecondaryNameNode (SNN)*
and many *DataNodes (DNs)*.
The procedure for upgrading a non-HA cluster is similar to upgrading a HA cluster
except that Step 2 "Upgrade Active and Standby *NNs*" is changed to below:

* Upgrade *NN* and *SNN*
    1. Shutdown *SNN*
    1. Shutdown and upgrade *NN*.
    1. Start *NN* with the
       "[`-rollingUpgrade started`](#namenode_-rollingUpgrade)" option.
    1. Upgrade and restart *SNN*


Downgrade and Rollback
----------------------

When the upgraded release is undesirable
or, in some unlikely case, the upgrade fails (due to bugs in the newer release),
administrators may choose to downgrade HDFS back to the pre-upgrade release,
or rollback HDFS to the pre-upgrade release and the pre-upgrade state.

Note that downgrade can be done in a rolling fashion but rollback cannot.
Rollback requires cluster downtime.

Note also that downgrade and rollback are possible only after a rolling upgrade is started and
before the upgrade is terminated.
An upgrade can be terminated by either finalize, downgrade or rollback.
Therefore, it may not be possible to perform rollback after finalize or downgrade,
or to perform downgrade after finalize.


Downgrade
---------

*Downgrade* restores the software back to the pre-upgrade release
and preserves the user data.
Suppose time *T* is the rolling upgrade start time and the upgrade is terminated by downgrade.
Then, the files created before or after *T* remain available in HDFS.
The files deleted before or after *T* remain deleted in HDFS.

A newer release is downgradable to the pre-upgrade release
only if both the namenode layout version and the datanode layout version
are not changed between these two releases.

In a HA cluster,
when a rolling upgrade from an old software release to a new software release is in progress,
it is possible to downgrade, in a rolling fashion, the upgraded machines back to the old software release.
Same as before, suppose *NN1* and *NN2* are respectively in active and standby states.
Below are the steps for rolling downgrade without downtime:

1. Downgrade *DNs*
    1. Choose a small subset of datanodes (e.g. all datanodes under a particular rack).
        1. Run "[`hdfs dfsadmin -shutdownDatanode <DATANODE_HOST:IPC_PORT> upgrade`](#dfsadmin_-shutdownDatanode)"
           to shutdown one of the chosen datanodes.
        1. Run "[`hdfs dfsadmin -getDatanodeInfo <DATANODE_HOST:IPC_PORT>`](#dfsadmin_-getDatanodeInfo)"
           to check and wait for the datanode to shutdown.
        1. Downgrade and restart the datanode.
        1. Perform the above steps for all the chosen datanodes in the subset in parallel.
    1. Repeat the above steps until all upgraded datanodes in the cluster are downgraded.
1. Downgrade Active and Standby *NNs*
    1. Shutdown and downgrade *NN2*.
    1. Start *NN2* as standby normally.
    1. Failover from *NN1* to *NN2*
       so that *NN2* becomes active and *NN1* becomes standby.
    1. Shutdown and upgrade *NN1*.
    1. Start *NN1* as standby normally.
1. Finalize Rolling Downgrade
    1. Run "[`hdfs dfsadmin -rollingUpgrade finalize`](#dfsadmin_-rollingUpgrade)"
       to finalize the rolling downgrade.

Note that the datanodes must be downgraded before downgrading the namenodes
since protocols may be changed in a backward compatible manner but not forward compatible,
i.e. old datanodes can talk to the new namenodes but not vice versa.


Rollback
--------

*Rollback* restores the software back to the pre-upgrade release
but also reverts the user data back to the pre-upgrade state.
Suppose time *T* is the rolling upgrade start time and the upgrade is terminated by rollback.
The files created before *T* remain available in HDFS but the files created after *T* become unavailable.
The files deleted before *T* remain deleted in HDFS but the files deleted after *T* are restored.

Rollback from a newer release to the pre-upgrade release is always supported.
However, it cannot be done in a rolling fashion.  It requires cluster downtime.
Suppose *NN1* and *NN2* are respectively in active and standby states.
Below are the steps for rollback:

* Rollback HDFS
    1. Shutdown all *NNs* and *DNs*.
    1. Restore the pre-upgrade release in all machines.
    1. Start *NN1* as Active with the
       "[`-rollingUpgrade rollback`](#namenode_-rollingUpgrade)" option.
    1. Run `-bootstrapStandby' on NN2 and start it normally as standby.
    1. Start *DNs* with the "`-rollback`" option.


Commands and Startup Options for Rolling Upgrade
------------------------------------------------

### DFSAdmin Commands

#### `dfsadmin -rollingUpgrade`

    hdfs dfsadmin -rollingUpgrade <query|prepare|finalize>

Execute a rolling upgrade action.

* Options:

    | --- | --- |
    | `query` | Query the current rolling upgrade status. |
    | `prepare` | Prepare a new rolling upgrade. |
    | `finalize` | Finalize the current rolling upgrade. |


#### `dfsadmin -getDatanodeInfo`

    hdfs dfsadmin -getDatanodeInfo <DATANODE_HOST:IPC_PORT>

Get the information about the given datanode.
This command can be used for checking if a datanode is alive
like the Unix `ping` command.


#### `dfsadmin -shutdownDatanode`

    hdfs dfsadmin -shutdownDatanode <DATANODE_HOST:IPC_PORT> [upgrade]

Submit a shutdown request for the given datanode.
If the optional `upgrade` argument is specified,
clients accessing the datanode will be advised to wait for it to restart
and the fast start-up mode will be enabled.
When the restart does not happen in time, clients will timeout and ignore the datanode.
In such case, the fast start-up mode will also be disabled.

Note that the command does not wait for the datanode shutdown to complete.
The "[`dfsadmin -getDatanodeInfo`](#dfsadmin_-getDatanodeInfo)"
command can be used for checking if the datanode shutdown is completed.


### NameNode Startup Options

#### `namenode -rollingUpgrade`

    hdfs namenode -rollingUpgrade <rollback|started>

When a rolling upgrade is in progress,
the `-rollingUpgrade` namenode startup option is used to specify
various rolling upgrade options.

* Options:

    | --- | --- |
    | `rollback` | Restores the namenode back to the pre-upgrade release but also reverts the user data back to the pre-upgrade state. |
    | `started` | Specifies a rolling upgrade already started so that the namenode should allow image directories with different layout versions during startup. |

**WARN: downgrade options is obsolete.**
It is not necessary to start namenode with downgrade options explicitly.
