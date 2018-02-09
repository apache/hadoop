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

HDFS Router-based Federation
============================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

NameNodes have scalability limits because of the metadata overhead comprised of inodes (files and directories) and file blocks, the number of Datanode heartbeats, and the number of HDFS RPC client requests.
The common solution is to split the filesystem into smaller subclusters [HDFS Federation](./Federation.html) and provide a federated view [ViewFs](./ViewFs.html).
The problem is how to maintain the split of the subclusters (e.g., namespace partition), which forces users to connect to multiple subclusters and manage the allocation of folders/files to them.


Architecture
------------

A natural extension to this partitioned federation is to add a layer of software responsible for federating the namespaces.
This extra layer allows users to access any subcluster transparently, lets subclusters manage their own block pools independently, and supports rebalancing of data across subclusters.
To accomplish these goals, the federation layer directs block accesses to the proper subcluster, maintains the state of the namespaces, and provides mechanisms for data rebalancing.
This layer must be scalable, highly available, and fault tolerant.

This federation layer comprises multiple components.
The _Router_ component that has the same interface as a NameNode, and forwards the client requests to the correct subcluster, based on ground-truth information from a State Store.
The _State Store_ combines a remote _Mount Table_ (in the flavor of [ViewFs](./ViewFs.html), but shared between clients) and utilization (load/capacity) information about the subclusters.
This approach has the same architecture as [YARN federation](../hadoop-yarn/Federation.html).

![Router-based Federation Sequence Diagram | width=800](./images/routerfederation.png)


### Example flow
The simplest configuration deploys a Router on each NameNode machine.
The Router monitors the local NameNode and heartbeats the state to the State Store.
When a regular DFS client contacts any of the Routers to access a file in the federated filesystem, the Router checks the Mount Table in the State Store (i.e., the local cache) to find out which subcluster contains the file.
Then it checks the Membership table in the State Store (i.e., the local cache) for the NameNode responsible for the subcluster.
After it has identified the correct NameNode, the Router proxies the request.
The client accesses Datanodes directly.


### Router
There can be multiple Routers in the system with soft state.
Each Router has two roles:

* Federated interface: expose a single, global NameNode interface to the clients and forward the requests to the active NameNode in the correct subcluster
* NameNode heartbeat: maintain the information about a NameNode in the State Store

#### Federated interface
The Router receives a client request, checks the State Store for the correct subcluster, and forwards the request to the active NameNode of that subcluster.
The reply from the NameNode then flows in the opposite direction.
The Routers are stateless and can be behind a load balancer.
For performance, the Router also caches remote mount table entries and the state of the subclusters.
To make sure that changes have been propagated to all Routers, each Router heartbeats its state to the State Store.

The communications between the Routers and the State Store are cached (with timed expiration for freshness).
This improves the performance of the system.

#### NameNode heartbeat
For this role, the Router periodically checks the state of a NameNode (usually on the same server) and reports their high availability (HA) state and load/space status to the State Store.
Note that this is an optional role, as a Router can be independent of any subcluster.
For performance with NameNode HA, the Router uses the high availability state information in the State Store to forward the request to the NameNode that is most likely to be active.
Note that this service can be embedded into the NameNode itself to simplify the operation.

#### Availability and fault tolerance
The Router operates with failures at multiple levels.

* **Federated interface HA:**
The Routers are stateless and metadata operations are atomic at the NameNodes.
If a Router becomes unavailable, any Router can take over for it.
The clients configure their DFS HA client (e.g., ConfiguredFailoverProvider or RequestHedgingProxyProvider) with all the Routers in the federation as endpoints.

* **Unavailable State Store:**
If a Router cannot contact the State Store, it will enter into a Safe Mode state which disallows it from serving requests.
Clients will treat Routers in Safe Mode as it was an Standby NameNode and try another Router. There is a manual way to manage the Safe Mode for the Router.

The Safe Mode state can be managed by using the following command:

    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -safemode enter | leave | get

* **NameNode heartbeat HA:**
For high availability and flexibility, multiple Routers can monitor the same NameNode and heartbeat the information to the State Store.
This increases clients' resiliency to stale information, should a Router fail.
Conflicting NameNode information in the State Store is resolved by each Router via a quorum.

* **Unavailable NameNodes:**
If a Router cannot contact the active NameNode, then it will try the other NameNodes in the subcluster.
It will first try those reported as standby and then the unavailable ones.
If the Router cannot reach any NameNode, then it throws an exception.

* **Expired NameNodes:**
If a NameNode heartbeat has not been recorded in the State Store for a multiple of the heartbeat interval, the monitoring Router will record that the NameNode has expired and no Routers will attempt to access it.
If an updated heartbeat is subsequently recorded for the NameNode, the monitoring Router will restore the NameNode from the expired state.

#### Interfaces
To interact with the users and the administrators, the Router exposes multiple interfaces.

* **RPC:**
The Router RPC implements the most common interfaces clients use to interact with HDFS.
The current implementation has been tested using analytics workloads written in plain MapReduce, Spark, and Hive (on Tez, Spark, and MapReduce).
Advanced functions like snapshot, encryption and tiered storage are left for future versions.
All unimplemented functions will throw exceptions.

* **Admin:**
Administrators can query information from clusters and add/remove entries from the mount table over RPC.
This interface is also exposed through the command line to get and modify information from the federation.

* **Web UI:**
The Router exposes a Web UI visualizing the state of the federation, mimicking the current NameNode UI.
It displays information about the mount table, membership information about each subcluster, and the status of the Routers.

* **WebHDFS:**
The Router provides the HDFS REST interface (WebHDFS) in addition to the RPC one.

* **JMX:**
It exposes metrics through JMX mimicking the NameNode.
This is used by the Web UI to get the cluster status.

Some operations are not available in Router-based federation.
The Router throws exceptions for those.
Examples users may encounter include the following.

* Rename file/folder in two different nameservices.
* Copy file/folder in two different nameservices.
* Write into a file/folder being rebalanced.

### Quota management
Federation supports and controls global quota at mount table level.
For performance reasons, the Router caches the quota usage and updates it periodically. These quota usage values
will be used for quota-verification during each WRITE RPC call invoked in RouterRPCSever. See [HDFS Quotas Guide](./HdfsQuotaAdminGuide.html)
for the quota detail.

### State Store
The (logically centralized, but physically distributed) State Store maintains:

* The state of the subclusters in terms of their block access load, available disk space, HA state, etc.
* The mapping between folder/files and subclusters, i.e. the remote mount table.

The backend of the State Store is pluggable.
We leverage the fault tolerance of the backend implementations.
The main information stored in the State Store and its implementation:

* **Membership**:
The membership information encodes the state of the NameNodes in the federation.
This includes information about the subcluster, such as storage capacity and the number of nodes.
The Router periodically heartbeats this information about one or more NameNodes.
Given that multiple Routers can monitor a single NameNode, the heartbeat from every Router is stored.
The Routers apply a quorum of the data when querying this information from the State Store.
The Routers discard the entries older than a certain threshold (e.g., ten Router heartbeat periods).

* **Mount Table**:
This table hosts the mapping between folders and subclusters.
It is similar to the mount table in [ViewFs](.ViewFs.html) where it specifies the federated folder, the destination subcluster and the path in that folder.


### Security
Secure authentication and authorization are not supported yet, so the Router will not proxy to Hadoop clusters with security enabled.


Deployment
----------

By default, the Router is ready to take requests and monitor the NameNode in the local machine.
It needs to know the State Store endpoint by setting `dfs.federation.router.store.driver.class`.
The rest of the options are documented in [hdfs-default.xml](./hdfs-default.xml).

Once the Router is configured, it can be started:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs start dfsrouter

And to stop it:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --script $HADOOP_PREFIX/bin/hdfs stop dfsrouter

### Mount table management

The mount table entries are pretty much the same as in [ViewFs](./ViewFs.html).
A good practice for simplifying the management is to name the federated namespace with the same names as the destination namespaces.
For example, if we to mount `/data/app1` in the federated namespace, it is recommended to have that same name as in the destination namespace.

The federation admin tool supports managing the mount table.
For example, to create three mount points and list them:

    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -add /tmp ns1 /tmp
    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -add /data/app1 ns2 /data/app1
    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -add /data/app2 ns3 /data/app2
    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -ls

It also supports mount points that disallow writes:

    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -add /readonly ns1 / -readonly

If a mount point is not set, the Router will map it to the default namespace `dfs.federation.router.default.nameserviceId`.

Mount table have UNIX-like *permissions*, which restrict which users and groups have access to the mount point. Write permissions allow users to add
, update or remove mount point. Read permissions allow users to list mount point. Execute permissions are unused.

Mount table permission can be set by following command:

    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -add /tmp ns1 /tmp -owner root -group supergroup -mode 0755

The option mode is UNIX-style permissions for the mount table. Permissions are specified in octal, e.g. 0755. By default, this is set to 0755.

Router-based federation supports global quota at mount table level. Mount table entries may spread multiple subclusters and the global quota will be
accounted across these subclusters.

The federation admin tool supports setting quotas for specified mount table entries:

    [hdfs]$ $HADOOP_HOME/bin/hdfs dfsrouteradmin -setQuota /path -nsQuota 100 -ssQuota 1024

The above command means that we allow the path to have a maximum of 100 file/directories and use at most 1024 bytes storage space. The parameter for *ssQuota*
supports multiple size-unit suffix (e.g. 1k is 1KB, 5m is 5MB). If no suffix is specified then bytes is assumed.

Ls command will show below information for each mount table entry:

    Source                    Destinations              Owner                     Group                     Mode                      Quota/Usage
    /path                     ns0->/path                root                      supergroup                rwxr-xr-x                 [NsQuota: 50/0, SsQuota: 100 B/0 B]

Client configuration
--------------------

For clients to use the federated namespace, they need to create a new one that points to the routers.
For example, a cluster with 4 namespaces **ns0, ns1, ns2, ns3**, can add a new one to **hdfs-site.xml** called **ns-fed** which points to two of the routers:

```xml
<configuration>
  <property>
    <name>dfs.nameservices</name>
    <value>ns0,ns1,ns2,ns3,ns-fed</value>
  </property>
  <property>
    <name>dfs.ha.namenodes.ns-fed</name>
    <value>r1,r2</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.ns-fed.r1</name>
    <value>router1:rpc-port</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.ns-fed.r2</name>
    <value>router2:rpc-port</value>
  </property>
  <property>
    <name>dfs.client.failover.proxy.provider.ns-fed</name>
    <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
  </property>
  <property>
    <name>dfs.client.failover.random.order</name>
    <value>true</value>
  </property>
</configuration>
```

The `dfs.client.failover.random.order` set to `true` allows distributing the load evenly across the routers.

With this setting a user can interact with `ns-fed` as a regular namespace:

    $ $HADOOP_HOME/bin/hdfs dfs -ls hdfs://ns-fed/
    /tmp
    /data

This federated namespace can also be set as the default one at **core-site.xml** using `fs.defaultFS`.


Router configuration
--------------------

One can add the configurations for Router-based federation to **hdfs-site.xml**.
The main options are documented in [hdfs-default.xml](./hdfs-default.xml).
The configuration values are described in this section.

### RPC server

The RPC server to receive connections from the clients.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.default.nameserviceId | | Nameservice identifier of the default subcluster to monitor. |
| dfs.federation.router.rpc.enable | `true` | If `true`, the RPC service to handle client requests in the router is enabled. |
| dfs.federation.router.rpc-address | 0.0.0.0:8888 | RPC address that handles all clients requests. |
| dfs.federation.router.rpc-bind-host | 0.0.0.0 |  The actual address the RPC server will bind to. |
| dfs.federation.router.handler.count | 10 | The number of server threads for the router to handle RPC requests from clients. |
| dfs.federation.router.handler.queue.size | 100 | The size of the queue for the number of handlers to handle RPC client requests. |
| dfs.federation.router.reader.count | 1 | The number of readers for the router to handle RPC client requests. |
| dfs.federation.router.reader.queue.size | 100 | The size of the queue for the number of readers for the router to handle RPC client requests. |

#### Connection to the Namenodes

The Router forwards the client requests to the NameNodes.
It uses a pool of connections to reduce the latency of creating them.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.connection.pool-size | 1 | Size of the pool of connections from the router to namenodes. |
| dfs.federation.router.connection.clean.ms | 10000 | Time interval, in milliseconds, to check if the connection pool should remove unused connections. |
| dfs.federation.router.connection.pool.clean.ms | 60000 | Time interval, in milliseconds, to check if the connection manager should remove unused connection pools. |

### Admin server

The administration server to manage the Mount Table.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.admin.enable | `true` | If `true`, the RPC admin service to handle client requests in the router is enabled. |
| dfs.federation.router.admin-address | 0.0.0.0:8111 | RPC address that handles the admin requests. |
| dfs.federation.router.admin-bind-host | 0.0.0.0 | The actual address the RPC admin server will bind to. |
| dfs.federation.router.admin.handler.count | 1 | The number of server threads for the router to handle RPC requests from admin. |

### State Store

The connection to the State Store and the internal caching at the Router.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.store.enable | `true` | If `true`, the Router connects to the State Store. |
| dfs.federation.router.store.serializer | `StateStoreSerializerPBImpl` | Class to serialize State Store records. |
| dfs.federation.router.store.driver.class | `StateStoreZooKeeperImpl` | Class to implement the State Store. |
| dfs.federation.router.store.connection.test | 60000 | How often to check for the connection to the State Store in milliseconds. |
| dfs.federation.router.cache.ttl | 60000 | How often to refresh the State Store caches in milliseconds. |
| dfs.federation.router.store.membership.expiration | 300000 | Expiration time in milliseconds for a membership record. |

### Routing

Forwarding client requests to the right subcluster.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.file.resolver.client.class | MountTableResolver | Class to resolve files to subclusters. |
| dfs.federation.router.namenode.resolver.client.class | MembershipNamenodeResolver | Class to resolve the namenode for a subcluster. |

### Namenode monitoring

Monitor the namenodes in the subclusters for forwarding the client requests.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.heartbeat.enable | `true` | If `true`, the Router heartbeats into the State Store. |
| dfs.federation.router.heartbeat.interval | 5000 | How often the Router should heartbeat into the State Store in milliseconds. |
| dfs.federation.router.monitor.namenode | | The identifier of the namenodes to monitor and heartbeat. |
| dfs.federation.router.monitor.localnamenode.enable | `true` | If `true`, the Router should monitor the namenode in the local machine. |

### Quota

Global quota supported in federation.

| Property | Default | Description|
|:---- |:---- |:---- |
| dfs.federation.router.quota.enable | `false` | If `true`, the quota system enabled in the Router. |
| dfs.federation.router.quota-cache.update.interval | 60s | How often the Router updates quota cache. This setting supports multiple time unit suffixes. If no suffix is specified then milliseconds is assumed. |

Metrics
-------

The Router and State Store statistics are exposed in metrics/JMX. These info will be very useful for monitoring.
More metrics info can see [Router RPC Metrics](../../hadoop-project-dist/hadoop-common/Metrics.html#RouterRPCMetrics) and [State Store Metrics](../../hadoop-project-dist/hadoop-common/Metrics.html#StateStoreMetrics).