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
The common solution is to split the filesystem into smaller subclusters [HDFS Federation](.Federation.html) and provide a federated view [ViewFs](.ViewFs.html).
The problem is how to maintain the split of the subclusters (e.g., namespace partition), which forces users to connect to multiple subclusters and manage the allocation of folders/files to them.


Architecture
------------

A natural extension to this partitioned federation is to add a layer of software responsible for federating the namespaces.
This extra layer allows users to access any subcluster transparently, lets subclusters manage their own block pools independently, and supports rebalancing of data across subclusters.
To accomplish these goals, the federation layer directs block accesses to the proper subcluster, maintains the state of the namespaces, and provides mechanisms for data rebalancing.
This layer must be scalable, highly available, and fault tolerant.

This federation layer comprises multiple components.
The _Router_ component that has the same interface as a NameNode, and forwards the client requests to the correct subcluster, based on ground-truth information from a State Store.
The _State Store_ combines a remote _Mount Table_ (in the flavor of [ViewFs](.ViewFs.html), but shared between clients) and utilization (load/capacity) information about the subclusters.
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
Advanced functions like snapshotting, encryption and tiered storage are left for future versions.
All unimplemented functions will throw exceptions.

* **Admin:**
Adminstrators can query information from clusters and add/remove entries from the mount table over RPC.
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


Deployment
----------

By default, the Router is ready to take requests and monitor the NameNode in the local machine.
It needs to know the State Store endpoint by setting `dfs.federation.router.store.driver.class`.
The rest of the options are documented in [hdfs-default.xml](./hdfs-default.xml).

Once the Router is configured, it can be started:

    [hdfs]$ $HADOOP_HOME/bin/hdfs router

To manage the mount table:

    [hdfs]$ $HADOOP_HOME/bin/hdfs federation -add /tmp DC1 /tmp
    [hdfs]$ $HADOOP_HOME/bin/hdfs federation -add /data/wl1 DC2 /data/wl1
    [hdfs]$ $HADOOP_HOME/bin/hdfs federation -add /data/wl2 DC3 /data/wl2
    [hdfs]$ $HADOOP_HOME/bin/hdfs federation -ls
