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

Service Level Authorization Guide
=================================

* [Service Level Authorization Guide](#Service_Level_Authorization_Guide)
    * [Purpose](#Purpose)
    * [Prerequisites](#Prerequisites)
    * [Overview](#Overview)
    * [Configuration](#Configuration)
        * [Enable Service Level Authorization](#Enable_Service_Level_Authorization)
        * [Hadoop Services and Configuration Properties](#Hadoop_Services_and_Configuration_Properties)
        * [Access Control Lists](#Access_Control_Lists)
        * [Refreshing Service Level Authorization Configuration](#Refreshing_Service_Level_Authorization_Configuration)
        * [Examples](#Examples)

Purpose
-------

This document describes how to configure and manage Service Level Authorization for Hadoop.

Prerequisites
-------------

Make sure Hadoop is installed, configured and setup correctly. For more information see:

* [Single Node Setup](./SingleCluster.html) for first-time users.
* [Cluster Setup](./ClusterSetup.html) for large, distributed clusters.

Overview
--------

Service Level Authorization is the initial authorization mechanism to ensure clients connecting to a particular Hadoop service have the necessary, pre-configured, permissions and are authorized to access the given service. For example, a MapReduce cluster can use this mechanism to allow a configured list of users/groups to submit jobs.

The `$HADOOP_CONF_DIR/hadoop-policy.xml` configuration file is used to define the access control lists for various Hadoop services.

Service Level Authorization is performed much before to other access control checks such as file-permission checks, access control on job queues etc.

Configuration
-------------

This section describes how to configure service-level authorization via the configuration file `$HADOOP_CONF_DIR/hadoop-policy.xml`.

### Enable Service Level Authorization

By default, service-level authorization is disabled for Hadoop. To enable it set the configuration property hadoop.security.authorization to true in `$HADOOP_CONF_DIR/core-site.xml`.

### Hadoop Services and Configuration Properties

This section lists the various Hadoop services and their configuration knobs:

| Property | Service |
|:---- |:---- |
| security.client.protocol.acl | ACL for ClientProtocol, which is used by user code via the DistributedFileSystem. |
| security.client.datanode.protocol.acl | ACL for ClientDatanodeProtocol, the client-to-datanode protocol for block recovery. |
| security.datanode.protocol.acl | ACL for DatanodeProtocol, which is used by datanodes to communicate with the namenode. |
| security.inter.datanode.protocol.acl | ACL for InterDatanodeProtocol, the inter-datanode protocol for updating generation timestamp. |
| security.namenode.protocol.acl | ACL for NamenodeProtocol, the protocol used by the secondary namenode to communicate with the namenode. |
| security.job.client.protocol.acl | ACL for JobSubmissionProtocol, used by job clients to communciate with the resourcemanager for job submission, querying job status etc. |
| security.job.task.protocol.acl | ACL for TaskUmbilicalProtocol, used by the map and reduce tasks to communicate with the parent nodemanager. |
| security.refresh.policy.protocol.acl | ACL for RefreshAuthorizationPolicyProtocol, used by the dfsadmin and rmadmin commands to refresh the security policy in-effect. |
| security.ha.service.protocol.acl | ACL for HAService protocol used by HAAdmin to manage the active and stand-by states of namenode. |

### Access Control Lists

`$HADOOP_CONF_DIR/hadoop-policy.xml` defines an access control list for each Hadoop service. Every access control list has a simple format:

The list of users and groups are both comma separated list of names. The two lists are separated by a space.

Example: `user1,user2 group1,group2`.

Add a blank at the beginning of the line if only a list of groups is to be provided, equivalently a comma-separated list of users followed by a space or nothing implies only a set of given users.

A special value of `*` implies that all users are allowed to access the service.

If access control list is not defined for a service, the value of `security.service.authorization.default.acl` is applied. If `security.service.authorization.default.acl` is not defined, `*` is applied.

* Blocked Access Control ListsIn some cases, it is required to specify blocked access control list for a service. This specifies the list of users and groups who are not authorized to access the service. The format of the blocked access control list is same as that of access control list. The blocked access control list can be specified via `$HADOOP_CONF_DIR/hadoop-policy.xml`. The property name is derived by suffixing with ".blocked".

    Example: The property name of blocked access control list for `security.client.protocol.acl>> will be <<<security.client.protocol.acl.blocked`

    For a service, it is possible to specify both an access control list and a blocked control list. A user is authorized to access the service if the user is in the access control and not in the blocked access control list.

    If blocked access control list is not defined for a service, the value of `security.service.authorization.default.acl.blocked` is applied. If `security.service.authorization.default.acl.blocked` is not defined, empty blocked access control list is applied.

### Refreshing Service Level Authorization Configuration

The service-level authorization configuration for the NameNode and ResourceManager can be changed without restarting either of the Hadoop master daemons. The cluster administrator can change `$HADOOP_CONF_DIR/hadoop-policy.xml` on the master nodes and instruct the NameNode and ResourceManager to reload their respective configurations via the `-refreshServiceAcl` switch to `dfsadmin` and `rmadmin` commands respectively.

Refresh the service-level authorization configuration for the NameNode:

       $ bin/hdfs dfsadmin -refreshServiceAcl

Refresh the service-level authorization configuration for the ResourceManager:

       $ bin/yarn rmadmin -refreshServiceAcl

Of course, one can use the `security.refresh.policy.protocol.acl` property in `$HADOOP_CONF_DIR/hadoop-policy.xml` to restrict access to the ability to refresh the service-level authorization configuration to certain users/groups.

* Access Control using list of ip addresses, host names and ip rangesAccess to a service can be controlled based on the ip address of the client accessing the service. It is possible to restrict access to a service from a set of machines by specifying a list of ip addresses, host names and ip ranges. The property name for each service is derived from the corresponding acl's property name. If the property name of acl is security.client.protocol.acl, property name for the hosts list will be security.client.protocol.hosts.

    If hosts list is not defined for a service, the value of `security.service.authorization.default.hosts` is applied. If `security.service.authorization.default.hosts` is not defined, `*` is applied.

    It is possible to specify a blocked list of hosts. Only those machines which are in the hosts list, but not in the blocked hosts list will be granted access to the service. The property name is derived by suffixing with ".blocked".

    Example: The property name of blocked hosts list for `security.client.protocol.hosts` will be `security.client.protocol.hosts.blocked`

    If blocked hosts list is not defined for a service, the value of `security.service.authorization.default.hosts.blocked` is applied. If `security.service.authorization.default.hosts.blocked` is not defined, empty blocked hosts list is applied.

### Examples

Allow only users `alice`, `bob` and users in the `mapreduce` group to submit jobs to the MapReduce cluster:

    <property>
         <name>security.job.client.protocol.acl</name>
         <value>alice,bob mapreduce</value>
    </property>

Allow only DataNodes running as the users who belong to the group datanodes to communicate with the NameNode:

    <property>
         <name>security.datanode.protocol.acl</name>
         <value>datanodes</value>
    </property>

Allow any user to talk to the HDFS cluster as a DFSClient:

    <property>
         <name>security.client.protocol.acl</name>
         <value>*</value>
    </property>
