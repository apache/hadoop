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

Consistent Reads from HDFS Observer NameNode
=============================================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
--------

This guide provides an overview of the HDFS Observer NameNode feature
and how to configure/install it in a typical HA-enabled cluster. For a
detailed technical design overview, please check the doc attached to
[HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943).

Background
-----------

In a HA-enabled HDFS cluster (for more information, check
[HDFSHighAvailabilityWithQJM](./HDFSHighAvailabilityWithQJM.html)), there
is a single Active NameNode and one or more Standby NameNode(s). The
Active NameNode is responsible for serving all client requests, while
Standby NameNode just keep the up-to-date information regarding the
namespace, by tailing edit logs from JournalNodes, as well as block
location information, by receiving block reports from all the DataNodes.
One drawback of this architecture is that the Active NameNode could be a
single bottle-neck and be overloaded with client requests, especially in
a busy cluster.

The Consistent Reads from HDFS Observer NameNode feature addresses the
above by introducing a new type of NameNode called **Observer
NameNode**. Similar to Standby NameNode, Observer NameNode keeps itself
up-to-date regarding the namespace and block location information.
In addition, it also has the ability to serve consistent reads, like
Active NameNode. Since read requests are the majority in a typical
environment, this can help to load balancing the NameNode traffic and
improve overall throughput.

Architecture
--------------

In the new architecture, a HA cluster could consists of namenodes in 3
different states: active, standby and observer. State transition can
happen between active and standby, standby and observer, but not
directly between active and observer.

To ensure read-after-write consistency within a single client, a state
ID, which is implemented using transaction ID within NameNode, is
introduced in RPC headers. When a client performs write through Active
NameNode, it updates its state ID using the latest transaction ID from
the NameNode. When performing a subsequent read, the client passes this
state ID to Observe NameNode, which will then check against its own
transaction ID, and will ensure its own transaction ID has caught up
with the request's state ID, before serving the read request.

Edit log tailing is critical for Observer NameNode as it directly affects
the latency between when a transaction is applied in Active NameNode and
when it is applied in the Observer NameNode. A new edit log tailing
mechanism, named "Edit Tailing Fast-Path", is introduced to
significantly reduce this latency. This is built on top of the existing
in-progress edit log tailing feature, with further improvements such as
RPC-based tailing instead of HTTP, a in-memory cache on the JournalNode,
etc. For more details, please see the design doc attached to HDFS-13150.

New client-side proxy providers are also introduced.
ObserverReadProxyProvider, which inherits the existing
ConfiguredFailoverProxyProvider, should be used to replace the latter to
enable reads from Observer NameNode. When submitting a client read
request, the proxy provider will first try each Observer NameNode
available in the cluster, and only fall back to Active NameNode if all
of the former failed. Similarly, ObserverReadProxyProviderWithIPFailover
is introduced to replace IPFailoverProxyProvider in a IP failover setup.

Deployment
-----------

### Configurations

To enable consistent reads from Observer NameNode, you'll need to add a
few configurations to your **hdfs-site.xml**:

*  **dfs.ha.tail-edits.in-progress** - to enable fast tailing on
   in-progress edit logs.

   This enables fast edit log tailing through in-progress edit logs and
   also other mechanisms such as RPC-based edit log fetching, in-memory
   cache in JournalNodes, and so on. It is disabled by default, but is
   **required to be turned on** for the Observer NameNode feature.

        <property>
          <name>dfs.ha.tail-edits.in-progress</name>
          <value>true</value>
        </property>

*  **dfs.ha.tail-edits.period** - how often Standby/Observer NameNodes
   should fetch edits from JournalNodes.

   This determines the staleness of Observer NameNode w.r.t the Active.
   If too large, RPC time will increase as client requests will wait
   longer in the RPC queue before Observer tails edit logs and catches
   up the latest state of Active. The default value is 1min. It is
   **highly recommend** to configure this to a much lower value.

        <property>
          <name>dfs.ha.tail-edits.period</name>
          <value>0ms</value>
        </property>

*  **dfs.journalnode.edit-cache-size.bytes** - the in-memory cache size,
   in bytes, on the JournalNodes.

   This is the size, in bytes, of the in-memory cache for storing edits
   on the JournalNode side. The cache is used for serving edits via
   RPC-based tailing. This is only effective when
   dfs.ha.tail-edits.in-progress is turned on.

        <property>
          <name>dfs.journalnode.edit-cache-size.bytes</name>
          <value>1048576</value>
        </property>

### New administrative command

A new HA admin command is introduced to transition a Standby NameNode
into observer state:

    haadmin -transitionToObserver

Note this can only be executed on Standby NameNode. Exception will be
thrown when invoking this on Active NameNode.

Similarly, existing **transitionToStandby** can also be run on an
Observer NameNode, which transition it to the standby state.

**NOTE**: the feature for Observer NameNode to participate in failover
is not implemented yet. Therefore, as described in the next section, you
should only use **transitionToObserver** to bring up an observer and put
it outside the ZooKeeper controlled failover group. You should not use
**transitionToStandby** since the host for the Observer NameNode cannot
have ZKFC running.

### Deployment details

To enable observer support, first you'll need a HA-enabled HDFS cluster
with more than 2 namenodes. Then, you need to transition Standby
NameNode(s) into the observer state. An minimum setup would be running 3
namenodes in the cluster, one active, one standby and one observer. For
large HDFS clusters we recommend running two or more Observers depending
on the intensity of read requests and HA requirements.

Note that currently Observer NameNode doesn't integrate fully when
automatic failover is enabled. If the
**dfs.ha.automatic-failover.enabled** is turned on, you'll also need to
disable ZKFC on the namenode for observer. In addition to that, you'll
also need to add **forcemanual** flag to the **transitionToObserver**
command:

    haadmin -transitionToObserver -forcemanual

In future, this restriction will be lifted.

### Client configuration

Clients who wish to use Observer NameNode for read accesses can
specify the ObserverReadProxyProvider class for proxy provider
implementation, in the client-side **hdfs-site.xml** configuration file:

    <property>
        <name>dfs.client.failover.proxy.provider.<nameservice></name>
        <value>org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider</value>
    </property>

Clients who do not wish to use Observer NameNode can still use the
existing ConfiguredFailoverProxyProvider and should not see any behavior
change.
