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

Hadoop: YARN Federation
=======================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------
YARN is known to scale to thousands of nodes. The scalability of [YARN](./YARN.html) is determined by the Resource Manager, and is proportional to number of nodes, active applications, active containers, and frequency of heartbeat (of both nodes and applications). Lowering heartbeat can provide scalability increase, but is detrimental to utilization (see old Hadoop 1.x experience).
This document described a federation-based approach to scale a single YARN cluster to tens of thousands of nodes, by federating multiple YARN sub-clusters.  The proposed approach is to divide a large (10-100k nodes) cluster into smaller units called sub-clusters, each with its own YARN RM and compute nodes. The federation system will stitch these sub-clusters together and make them appear as one large YARN cluster to the applications.
The applications running in this federated environment will see a single massive YARN cluster and will be able to schedule tasks on any node of the federated cluster. Under the hood, the federation system will negotiate with sub-clusters resource managers and provide resources to the application. The goal is to allow an individual job to “span” sub-clusters seamlessly.

This design is structurally scalable, as we bound the number of nodes each RM is responsible for, and appropriate policies, will try to ensure that the majority of applications will reside within a single sub-cluster, thus the number of applications each RM will see is also bounded. This means we could almost linearly scale, by simply adding sub-clusters (as very little coordination is needed across them).
This architecture can provide very tight enforcement of scheduling invariants within each sub-cluster (simply inherits from YARN), while continuous re-balancing across sub-cluster will enforce (less strictly) that these properties are also respected at a global level (e.g., if a sub-cluster loses a large number of nodes, we could re-map queues to other sub-clusters to ensure users running on the impaired sub-cluster are not unfairly affected).

Federation is designed as a “layer” atop of existing YARN codebase, with limited changes in the core YARN mechanisms.

Assumptions:

* We assume reasonably good connectivity across sub-clusters (e.g., we are not looking to federate across DC yet, though future investigations of this are not excluded).
* We rely on HDFS federation (or equivalently scalable DFS solutions) to take care of scalability of the store side.


Architecture
------------
OSS YARN has been known to scale up to about few thousand nodes. The proposed architecture leverages the notion of federating a number of such smaller YARN clusters, referred to as sub-clusters, into a larger federated YARN cluster comprising of tens of thousands of nodes.
The applications running in this federated environment see a unified large YARN cluster and will be able to schedule tasks on any nodes in the cluster. Under the hood, the federation system will negotiate with sub-clusters RMs and provide resources to the application.  The logical architecture in Figure 1 shows the main components that comprise the federated cluster, which are described below.

![YARN Federation Architecture | width=800](./images/federation_architecture.png)

###YARN Sub-cluster
A sub-cluster is a YARN cluster with up to a few thousand nodes. The exact size of the sub-cluster will be determined considering ease of deployment/maintenance, alignment
with network or availability zones and general best practices.

The sub-cluster YARN RM will run with work-preserving high-availability turned-on, i.e., we should be able to tolerate YARN RM, NM failures with minimal disruption.
If the entire sub-cluster is compromised, external mechanisms will ensure that jobs are resubmitted in a separate sub-cluster (this could eventually be included in the federation design).

Sub-cluster is also the scalability unit in a federated environment. We can scale out the federated environment by adding one or more sub-clusters.

*Note*: by design each sub-cluster is a fully functional YARN RM, and its contribution to the federation can be set to be only a fraction of its overall capacity,
i.e. a sub-cluster can have a “partial” commitment to the federation, while retaining the ability to give out part of its capacity in a completely local way.

###Router
YARN applications are submitted to one of the Routers, which in turn applies a routing policy (obtained from the Policy Store), queries the State Store for the sub-cluster
URL and redirects the application submission request to the appropriate sub-cluster RM. We call the sub-cluster where the job is started the “home sub-cluster”, and we call
“secondary sub-clusters” all other sub-cluster a job is spanning on.
The Router exposes the ApplicationClientProtocol to the outside world, transparently hiding the presence of multiple RMs. To achieve this the Router also persists the mapping
between the application and its home sub-cluster into the State Store. This allows Routers to be soft-state while supporting user requests cheaply, as any Router can recover
this application to home sub-cluster mapping and direct requests to the right RM without broadcasting them. For performance caching and session stickiness might be advisable.
The state of the federation (including applications and nodes) is exposed through the Web UI.

###AMRMProxy
The AMRMProxy is a key component to allow the application to scale and run across sub-clusters. The AMRMProxy runs on all the NM machines and acts as a proxy to the
YARN RM for the AMs by implementing the ApplicationMasterProtocol. Applications will not be allowed to communicate with the sub-cluster RMs directly. They are forced
by the system to connect only to the AMRMProxy endpoint, which would provide transparent access to multiple YARN RMs (by dynamically routing/splitting/merging the communications).
At any one time, a job can span across one home sub-cluster and multiple secondary sub-clusters, but the policies operating in the AMRMProxy try to limit the footprint of each job
to minimize overhead on the scheduling infrastructure (more in section on scalability/load). The interceptor chain architecture of the ARMMProxy is showing in figure.

![Architecture of the AMRMProxy interceptor chain | width=800](./images/amrmproxy_architecture.png)

*Role of AMRMProxy*

1. Protect the sub-cluster YARN RMs from misbehaving AMs. The AMRMProxy can prevent DDOS attacks by throttling/killing AMs that are asking too many resources.
2. Mask the multiple YARN RMs in the cluster, and can transparently allow the AM to span across sub-clusters. All container allocations are done by the YARN RM framework that consists of the AMRMProxy fronting the home and other sub-cluster RMs.
3. Intercepts all the requests, thus it can enforce application quotas, which would not be enforceable by sub-cluster RM (as each only see a fraction of the AM requests).
4. The AMRMProxy can enforce load-balancing / overflow policies.

### Global Policy Generator
Global Policy Generator overlooks the entire federation and ensures that the system is configured and tuned properly all the time.
A key design point is that the cluster availability does not depend on an always-on GPG. The GPG operates continuously but out-of-band from all cluster operations,
and provide us with a unique vantage point, that allows to enforce global invariants, affect load balancing, trigger draining of sub-clusters that will undergo maintenance, etc.
More precisely the GPG will update user capacity allocation-to-subcluster mappings, and more rarely change the policies that run in Routers, AMRMProxy (and possible RMs).

In case the GPG is not-available, cluster operations will continue as of the last time the GPG published policies, and while a long-term unavailability might mean some
of the desirable properties of balance, optimal cluster utilization and global invariants might drift away, compute and access to data will not be compromised.

*NOTE*: In the current implementation the GPG is a manual tuning process, simply exposed via a CLI (YARN-3657).

This part of the federation system is part of future work in [YARN-5597](https://issues.apache.org/jira/browse/YARN-5597).

### Federation State-Store
The Federation State defines the additional state that needs to be maintained to loosely couple multiple individual sub-clusters into a single large federated cluster. This includes the following information:

#### Sub-cluster Membership
The member YARN RMs continuously heartbeat to the state store to keep alive and publish their current capability/load information.  This information is used by the
Global Policy Generator (GPG) to make proper policy decisions. Also, this information can be used by routers to select the best home sub-cluster.  This mechanism allows
us to dynamically grow/shrink the “cluster fleet” by adding or removing sub-clusters.  This also allows for easy maintenance of each sub-cluster. This is new functionality
that needs to be added to the YARN RM but the mechanisms are well understood as it’s similar to individual YARN RM HA.

#### Application’s Home Sub-cluster
The sub-cluster on which the Application Master (AM) runs is called the Application’s “home sub-cluster”. The AM is not limited to resources from the home sub-cluster
but can also request resources from other sub-clusters, referred to as secondary sub-clusters.
The federated environment will be configured and tuned periodically such that when an AM is placed on a sub-cluster, it should be able to find most of the resources
on the home sub-cluster. Only in certain cases it should need to ask for resources from other sub-clusters.

### Federation Policy Store
The federation Policy Store is a logically separate store (while it might be backed
by the same physical component), which contains information about how applications and
resource requests are routed to different sub-clusters. The current implementation provides
several policies, ranging from random/hashing/round-robin/priority to more sophisticated
ones which account for sub-cluster load, and request locality needs.


Running Applications across Sub-Clusters
----------------------------------------

When an application is submitted, the system will determine the most appropriate sub-cluster to run the application,
which we call as the application’s home sub-cluster. All the communications from the AM to the RM will be proxied via
the AMRMProxy running locally on the AM machine.
AMRMProxy exposes the same ApplicationMasterService protocol endpoint as the YARN RM. The AM can request containers
using the locality information exposed by the storage layer. In ideal case, the application will be placed on a sub-cluster
where all the resources and data required by the application will be available, but if it does need containers on nodes in
other sub-clusters, AMRMProxy will negotiate with the RMs of those sub-clusters transparently and provide the resources to
the application, thereby enabling the application to view the entire federated environment as one massive YARN cluster.
AMRMProxy, Global Policy Generator (GPG) and Router work together to make this happen seamlessly.

![Federation Sequence Diagram | width=800](./images/federation_sequence_diagram.png)


The figure shows a sequence diagram for the following job execution flow:

1. The Router receives an application submission request that is compliant with the YARN Application Client Protocol.
2. The router interrogates a routing table / policy to choose the “home RM” for the job (the policy configuration is received from the state-store on heartbeat).
3. The router queries the membership state to determine the endpoint of the home RM.
4. The router then redirects the application submission request to the home RM.
5. The router updates the application state with the home sub-cluster identifier.
6. Once the application is submitted to the home RM, the stock YARN flow is triggered, i.e. the application is added to the scheduler queue and its AM started in the home sub-cluster, on the first NodeManager that has available resources.
    a. During this process, the AM environment is modified by indicating that the address of the AMRMProxy as the YARN RM to talk to.
    b. The security tokens are also modified by the NM when launching the AM, so that the AM can only talk with the AMRMProxy. Any future communication from AM to the YARN RM is mediated by the AMRMProxy.
7. The AM will then request containers using the locality information exposed by HDFS.
8. Based on a policy the AMRMProxy can impersonate the AM on other sub-clusters, by submitting an Unmanaged AM, and by forwarding the AM heartbeats to relevant sub-clusters.
    a. Federation supports multiple application attempts with AMRMProxy HA. AM containers will have different attempt id in home sub-cluster, but the same Unmanaged AM in secondaries will be used across attempts.
    b. When AMRMProxy HA is enabled, UAM token will be stored in Yarn Registry. In the registerApplicationMaster call of each application attempt, AMRMProxy will go fetch existing UAM tokens from registry (if any) and re-attached to the existing UAMs.
9. The AMRMProxy will use both locality information and a pluggable policy configured in the state-store to decide whether to forward the resource requests received by the AM to the Home RM or to one (or more) Secondary RMs. In Figure 1, we show the case in which the AMRMProxy decides to forward the request to the secondary RM.
10. The secondary RM will provide the AMRMProxy with valid container tokens to start a new container on some node in its sub-cluster. This mechanism ensures that each sub-cluster uses its own security tokens and avoids the need for a cluster wide shared secret to create tokens.
11. The AMRMProxy forwards the allocation response back to the AM.
12. The AM starts the container on the target NodeManager (on sub-cluster 2) using the standard YARN protocols.




Configuration
-------------

  To configure the `YARN` to use the `Federation`, set the following property in the **conf/yarn-site.xml**:

### EVERYWHERE:

These are common configurations that should appear in the **conf/yarn-site.xml** at each machine in the federation.

| Property                          | Example                  | Description                                                                 |
|:----------------------------------|:-------------------------|:----------------------------------------------------------------------------|
| `yarn.federation.enabled`         | `true`                   | Whether federation is enabled or not                                        |
| `yarn.resourcemanager.cluster-id` | `<unique-subcluster-id>` | The unique subcluster identifier for this RM (same as the one used for HA). |

#### How to configure State-Store

Currently, we support ZooKeeper and SQL based implementations of the state-store.

**Note:** The State-Store implementation must always be overwritten with one of the below.

ZooKeeper: one must set the ZooKeeper settings for Hadoop:

| Property                            | Example                                                                             | Description                             |
|:------------------------------------|:------------------------------------------------------------------------------------|:----------------------------------------|
| `yarn.federation.state-store.class` | `org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore` | The type of state-store to use.         |
| `hadoop.zk.address`                 | `host:port`                                                                         | The address for the ZooKeeper ensemble. |

SQL: one must setup the following parameters:

| Property                                          | Example                                                                       | Description                                                                                              |
|:--------------------------------------------------|:------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------|
| `yarn.federation.state-store.class`               | `org.apache.hadoop.yarn.server.federation.store.impl.SQLFederationStateStore` | The type of state-store to use.                                                                          |
| `yarn.federation.state-store.sql.url`             | `jdbc:mysql://<host>:<port>/FederationStateStore`                             | For SQLFederationStateStore the name of the DB where the state is stored.                                |
| `yarn.federation.state-store.sql.jdbc-class`      | `com.mysql.jdbc.jdbc2.optional.MysqlDataSource`                               | For SQLFederationStateStore the jdbc class to use.                                                       |
| `yarn.federation.state-store.sql.username`        | `<dbuser>`                                                                    | For SQLFederationStateStore the username for the DB connection.                                          |
| `yarn.federation.state-store.sql.password`        | `<dbpass>`                                                                    | For SQLFederationStateStore the password for the DB connection.                                          |
| `yarn.federation.state-store.sql.max-connections` | `1`                                                                           | This is the maximum number of parallel connections each Router makes to the state-store.                 |
| `yarn.federation.state-store.sql.minimum-idle`    | `1`                                                                           | The property controls the minimum number of idle connections that HikariCP trie to maintain in the pool. |
| `yarn.federation.state-store.sql.pool-name`       | `YARN-Federation-DataBasePool`                                                | Specifies the name of the connection pool used by the FederationSQLStateStore.                           |
| `yarn.federation.state-store.sql.max-life-time`   | `30m`                                                                         | This property controls the maximum lifetime of a connection in the pool.                                 |
| `yarn.federation.state-store.sql.idle-time-out`   | `10m`                                                                         | This property controls the maximum amount of time that a connection is allowed to sit idle in the pool.  |
| `yarn.federation.state-store.sql.conn-time-out`   | `10s`                                                                         | Set the maximum amount of time that a client will wait for a connection from the pool.                   |

We provide scripts for **MySQL** and **Microsoft SQL Server**.

- MySQL

For MySQL, one must download the latest jar version 5.x from [MVN Repository](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and add it to the CLASSPATH.
Then the DB schema is created by executing the following SQL scripts in the database:

1. **sbin/FederationStateStore/MySQL/FederationStateStoreDatabase.sql**.
2. **sbin/FederationStateStore/MySQL/FederationStateStoreUser.sql**.
3. **sbin/FederationStateStore/MySQL/FederationStateStoreTables.sql**.
4. **sbin/FederationStateStore/MySQL/FederationStateStoreStoredProcs.sql**.

In the same directory we provide scripts to drop the Stored Procedures, the Tables, the User and the Database.

**Note:** the FederationStateStoreUser.sql defines a default user/password for the DB that you are **highly encouraged** to set this to a proper strong password.

**The versions supported by MySQL are MySQL 5.7 and above:**

1. MySQL 5.7
2. MySQL 8.0

- Microsoft SQL Server

For SQL-Server, the process is similar, but the jdbc driver is already included.
SQL-Server scripts are located in **sbin/FederationStateStore/SQLServer/**.

**The versions supported by SQL-Server are SQL Server 2008 R2 and above:**

1. SQL Server 2008 R2 Enterprise
2. SQL Server 2012 Enterprise
3. SQL Server 2016 Enterprise
4. SQL Server 2017 Enterprise
5. SQL Server 2019 Enterprise

#### How to configure Optional

| Property                                    | Example                                                                                   | Description                                                                                                                                                                                                                                                                                |
|:--------------------------------------------|:------------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.federation.failover.enabled`          | `true`                                                                                    | Whether should retry considering RM failover within each sub-cluster.                                                                                                                                                                                                                      |
| `yarn.federation.non-ha.enabled`            | `false`                                                                                   | If our subCluster's ResourceManager (RM) does not have High Availability (HA) enabled, we can configure this parameter as true. However, it is recommended to use RM HA in a production environment.                                                                                       |
| `yarn.client.failover-proxy-provider`       | `org.apache.hadoop.yarn.server.federation.failover.FederationRMFailoverProxyProvider`     | A FailoverProxyProvider implementation that uses the FederationStateStore to determine the ResourceManager to connect to. This supports both HA and regular mode which is controlled by configuration.                                                                                     |
| `yarn.federation.blacklist-subclusters`     | `<subcluster-id>`                                                                         | A list of black-listed sub-clusters, useful to disable a sub-cluster                                                                                                                                                                                                                       |
| `yarn.federation.policy-manager`            | `org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager` | The choice of policy manager determines how Applications and ResourceRequests are routed through the system.                                                                                                                                                                               |
| `yarn.federation.policy-manager-params`     | `<binary>`                                                                                | The payload that configures the policy. In our example a set of weights for router and amrmproxy policies. This is typically generated by serializing a policymanager that has been configured programmatically, or by populating the state-store with the .json serialized form of it.    |
| `yarn.federation.subcluster-resolver.class` | `org.apache.hadoop.yarn.server.federation.resolver.DefaultSubClusterResolverImpl`         | The class used to resolve which sub-cluster a node belongs to, and which subcluster(s) a rack belongs to.                                                                                                                                                                                  |
| `yarn.federation.machine-list`              | `<path of machine-list file>`                                                             | Path of machine-list file used by `SubClusterResolver`. Each line of the file is a node with sub-cluster and rack information. Below is the example: <br/> <br/> node1, subcluster1, rack1 <br/> node2, subcluster2, rack1 <br/> node3, subcluster3, rack2 <br/> node4, subcluster3, rack2 |

##### How to configure yarn.federation.policy-manager-params

  To configure the `yarn.federation.policy-manager-params` parameter, which represents the weight policy for the default queue,
  and where the relevant information will be parsed as `WeightedPolicyInfo`.

  We can use the following JSON format for configuration:

```xml
  <property>
     <name>yarn.federation.policy-manager-params</name>
     <value>{"routerPolicyWeights":{"entry":[{"key":{"id":"SC-2"},"value":"0.3"},{"key":{"id":"SC-1"},"value":"0.7"}]},"amrmPolicyWeights":{"entry":[{"key":{"id":"SC-2"},"value":"0.4"},{"key":{"id":"SC-1"},"value":"0.6"}]},"headroomAlpha":"1.0"}</value>
  </property>
```

  This JSON configuration allows you to define the weight policy for default queue, where:

  - The `routerPolicyWeights` section specifies the weightings for router policies. For instance, with a weight of `0.3` assigned to `SC-2` and `0.7` assigned to `SC-1`, this configuration will allocate `30%` of submitted application requests to `SC-2` and `70%` to `SC-1`.
  - The `amrmPolicyWeights` represents the allocation ratios for Application Master when request containers from different subclusters' RM. For instance, when an AM requests containers, it will request `40%` of the containers from `SC-2` and `60%` of the containers from `SC-1`.
  - The `headroomAlpha` used by policies that balance weight-based and load-based considerations in their decisions. For policies that use this parameter, values close to 1 indicate that most of the decision should be based on currently observed headroom from various sub-clusters, values close to zero, indicate that the decision should be mostly based on weights and practically ignore current load.

##### How to configure the policy-manager

**Router Policy**

  Router Policy defines the logic for determining the routing of an application submission and determines the HomeSubCluster for the application.

  - HashBasedRouterPolicy
    - This policy selects a sub-cluster based on the hash of the job's queue name. It is particularly useful when dealing with a large number of queues in a system, providing a default behavior. Furthermore, it ensures that all jobs belonging to the same queue are consistently mapped to the same sub-cluster, which can improve locality and performance.
  - LoadBasedRouterPolicy
    - This is a simplified load-balancing policy implementation. The policy utilizes binary weights (0/1 values) to enable or disable each sub-cluster. It selects the sub-cluster with the least load to forward the application traffic, ensuring optimal distribution.
  - LocalityRouterPolicy
    - This policy selects the sub-cluster based on the node specified by the client for running its application. Follows these conditions:
      - It succeeds if
        - There are three AMContainerResourceRequests in the order NODE, RACK, ANY
      - Falls back to WeightedRandomRouterPolicy
        - Null or empty AMContainerResourceRequests;
        - One AMContainerResourceRequests and it has ANY as ResourceName;
        - The node is in blacklisted SubClusters.
      - It fails if
        - The node does not exist and RelaxLocality is False;
        - We have an invalid number (not 0, 1 or 3) resource requests
  - RejectRouterPolicy
    - This policy simply rejects all incoming requests.
  - UniformRandomRouterPolicy
    - This simple policy picks at uniform random among any of the currently active sub-clusters. This policy is easy to use and good for testing.
  - WeightedRandomRouterPolicy
    - This policy implements a weighted random sample among currently active sub-clusters.

**AMRM Policy**

  AMRM Proxy defines the logic to split the resource request list received by AM among RMs.

  - BroadcastAMRMProxyPolicy
    - This policy simply broadcasts each ResourceRequest to all the available sub-clusters.
  - HomeAMRMProxyPolicy
    - This policy simply sends the ResourceRequest to the home sub-cluster.
  - LocalityMulticastAMRMProxyPolicy
    - Host localized ResourceRequests are always forwarded to the RM that owns the corresponding node, based on the feedback of a SubClusterResolver
      If the SubClusterResolver cannot resolve this node we default to forwarding the ResourceRequest to the home sub-cluster.
    - Rack localized ResourceRequests are forwarded to the RMs that owns the corresponding rack. Note that in some deployments each rack could be
      striped across multiple RMs. This policy respects that. If the SubClusterResolver cannot resolve this rack we default to forwarding
      the ResourceRequest to the home sub-cluster.
    - ANY requests corresponding to node/rack local requests are forwarded only to the set of RMs that owns the corresponding localized requests. The number of
      containers listed in each ANY is proportional to the number of localized container requests (associated to this ANY via the same allocateRequestId).
  - RejectAMRMProxyPolicy
    - This policy simply rejects all requests. Useful to prevent apps from accessing any sub-cluster.

**Policy Manager**

  The PolicyManager is providing a combination of RouterPolicy and AMRMPolicy.

  We can set policy-manager like this:
```xml
  <!--
   We provide 6 PolicyManagers, They have a common prefix: org.apache.hadoop.yarn.server.federation.policies.manager
   1. HashBroadcastPolicyManager
   2. HomePolicyManager
   3. PriorityBroadcastPolicyManager
   4. RejectAllPolicyManager
   5. UniformBroadcastPolicyManager
   6. WeightedLocalityPolicyManager
  -->
  <property>
     <name>yarn.federation.policy-manager</name>
     <value>org.apache.hadoop.yarn.server.federation.policies.manager.HashBroadcastPolicyManager</value>
  </property>
```

  - HashBroadcastPolicyManager
    - Policy that routes applications via hashing of their queuename, and broadcast resource requests. This picks a HashBasedRouterPolicy for the router and a BroadcastAMRMProxyPolicy for the amrmproxy as they are designed to work together.
  - HomePolicyManager
    - Policy manager which uses the UniformRandomRouterPolicy for the Router and HomeAMRMProxyPolicy as the AMRMProxy policy to find the RM.
  - PriorityBroadcastPolicyManager
    - Policy that allows operator to configure "weights" for routing. This picks a PriorityRouterPolicy for the router and a BroadcastAMRMProxyPolicy for the amrmproxy as they are designed to work together.
  - RejectAllPolicyManager
    - This policy rejects all requests for both router and amrmproxy routing. This picks a RejectRouterPolicy for the router and a RejectAMRMProxyPolicy for the amrmproxy as they are designed to work together.
  - UniformBroadcastPolicyManager
    - It combines the basic policies: UniformRandomRouterPolicy and BroadcastAMRMProxyPolicy, which are designed to work together and "spread" the load among sub-clusters uniformly. This simple policy might impose heavy load on the RMs and return more containers than a job requested as all requests are (replicated and) broadcasted.
  - WeightedLocalityPolicyManager
    - Policy that allows operator to configure "weights" for routing. This picks a LocalityRouterPolicy for the router and a LocalityMulticastAMRMProxyPolicy for the amrmproxy as they are designed to work together.

##### How to configure the queue policy

We will provide a set of commands to view and save queue policies.

The Queue Policy(SubClusterPolicyConfiguration) include the following:

| Property       | Description                                                           |
|:---------------|:----------------------------------------------------------------------|
| `queue`        | `Queue for Job submission`                                            |
| `policyType`   | `Policy Manager Class name, Default is UniformBroadcastPolicyManager` |
| `policyParams` | `It stores serialized objects of WeightedPolicyInfo.`                 |

WeightedPolicyInfo include the following:

- RouterWeight

  Weight for routing applications to different subclusters. We will route the application to different subclusters based on the configured weights.
  Assuming we have two subclusters, SC-1 and SC-2, with a weight of 0.7 for SC-1 and 0.3 for SC-2,
  the application will be allocated in such a way that 70% of the applications will be assigned to SC-1 and 30% to SC-2.

- AmRMWeight

  Weight for resource request from ApplicationMaster (AM) to different subclusters' Resource Manager (RM).
  Assuming we have two subclusters, SC-1 and SC-2, with a weight of 0.6 for SC-1 and 0.4 for SC-2,
  When AM requesting resources, 60% of the requests will be made to the Resource Manager (RM) of SC-1 and 40% to the RM of SC-2.

- HeadRoomAlpha

  used by policies that balance weight-based and load-based considerations in their decisions.
  For policies that use this parameter, values close to 1 indicate that most of the decision should be based on currently observed headroom from various sub-clusters, values close to zero, indicate that the decision should be mostly based on weights and practically ignore current load.

##### How to Config ZookeeperFederationStateStore Hierarchies

Similar to YARN-2962, We have implemented hierarchical storage for applications in the ZooKeeper federation store to manage the number of nodes under a specific Znode.

We can configure `yarn.resourcemanager.zk-appid-node.split-index`, default is 0, Index at which last section of application id (with each section separated by _ in application id) will be split so that application znode stored in zookeeper RM state store will be stored as two different znodes (parent-child). Split is done from the end.
For instance, with no split, appid znode will be of the form application_1352994193343_0001. If the value of this config is 1, the appid znode will be broken into two parts application_1352994193343_000 and 1 respectively with former being the parent node.
application_1352994193343_0002 will then be stored as 2 under the parent node application_1352994193343_000. This config can take values from 0 to 4. 0 means there will be no split. If configuration value is outside this range, it will be treated as config value of 0(i.e. no split). A value
larger than 0 (up to 4) should be configured if you are storing a large number of apps in ZK based RM state store and state store operations are failing due to LenError in Zookeeper.

### ON RMs:

These are extra configurations that should appear in the **conf/yarn-site.xml** at each ResourceManager.

| Property                     | Example          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:-----------------------------|:-----------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.resourcemanager.epoch` | `<unique-epoch>` | The seed value for the epoch. This is used to guarantee uniqueness of container-IDs generate by different RMs. It must therefore be unique among sub-clusters and `well-spaced` to allow for failures which increment epoch. Increments of 1000 allow for a large number of sub-clusters and practically ensure near-zero chance of collisions (a clash will only happen if a container is still alive for 1000 restarts of one RM, while the next RM never restarted, and an app requests more containers). |

Optional:

| Property                                              | Example | Description                                                                                 |
|:------------------------------------------------------|:--------|:--------------------------------------------------------------------------------------------|
| `yarn.federation.state-store.heartbeat-interval-secs` | `60`    | The rate at which RMs report their membership to the federation to the central state-store. |

**How to configure the cleanup of applications**

The Router supports storing scheduled applications in the StateStore. However, as the number of applications increases, it's essential to provide a mechanism for application cleanup.
We have implemented an automatic cleanup method in the ResourceManager (RM), which attempts to clean up the data in the StateStore after an application has completed its execution and has been removed from RM's memory.
Additionally, to account for certain exceptional cases where some applications may not be cleaned up properly, when the RM starts, we utilize a separate thread to attempt cleanup of completed applications.
We can refer to [YARN-11323](https://issues.apache.org/jira/browse/YARN-11323).

| Property                                                | Example | Description                                                                                                                                                                      |
|:--------------------------------------------------------|:--------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.federation.state-store.clean-up-retry-count`      | `1`     | The number of retries to clear the app in the FederationStateStore, the default value is 1, that is, after the app fails to clean up, it will retry the cleanup again.           |
| `yarn.federation.state-store.clean-up-retry-sleep-time` | `1s`    | Clear the sleep time of App retry in FederationStateStore. When the app fails to clean up, it will sleep for a period of time and then try to clean up. The default value is 1s. |

### ON ROUTER:

#### How to select Router Mode

Router supports YARN `Federation mode` and `Non-Federation` mode.

- Non-YARN Federation mode

the Router's role is to straightforwardly forward client requests to the cluster resourcemanager. In this mode, all we need to do is configure the cluster's ResourceManager addresses in the Router's **conf/yarn-site.xml**.

- YARN Federation mode

the Router distributes client requests to different sub-cluster resourcemanager based on user-configured queue policy. In this mode, we need to configure items such as interceptors, state store, and other settings.

These are extra configurations that should appear in the **conf/yarn-site.xml** at each Router.

| Property                                          | Example                                                                     | Description                                                                                                                                                                                                                                                                                                           |
|:--------------------------------------------------|:----------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.router.bind-host`                           | `0.0.0.0`                                                                   | Host IP to bind the router to.  The actual address the server will bind to. If this optional address is set, the RPC and webapp servers will bind to this address and the port specified in yarn.router.*.address respectively. This is most useful for making Router listen to all interfaces by setting to 0.0.0.0. |
| `yarn.router.clientrm.interceptor-class.pipeline` | `org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor` | A comma-separated list of interceptor classes to be run at the router when interfacing with the client. The last step of this pipeline must be the Federation Client Interceptor.                                                                                                                                     |
| `yarn.router.rmadmin.interceptor-class.pipeline`  | `org.apache.hadoop.yarn.server.router.rmadmin.FederationRMAdminInterceptor` | A comma-separated list of interceptor classes to be run at the router when interfacing with the client via Admin interface. The last step of this pipeline must be the Federation Admin Interceptor.                                                                                                                  |
| `yarn.router.webapp.interceptor-class.pipeline`   | `org.apache.hadoop.yarn.server.router.webapp.FederationInterceptorREST`     | A comma-separated list of interceptor classes to be run at the router when interfacing with the client via REST interface. The last step of this pipeline must be the Federation Interceptor REST.                                                                                                                    |

#### How to configure Router interceptor

- yarn.router.clientrm.interceptor-class.pipeline

The role of the interceptor is to forward client requests to RM.

| Property                                                                                | Mode             | Description                                                                                                                                                                                                                                              |
|:----------------------------------------------------------------------------------------|:-----------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `org.apache.hadoop.yarn.server.router.clientrm.DefaultClientRequestInterceptor`         | `Non-Federation` | That simply forwards the client requests to the cluster resource manager.                                                                                                                                                                                |
| `org.apache.hadoop.yarn.server.router.clientrm.PassThroughClientRequestInterceptor`     | `Federation`     | Interceptor that does not do anything other than forwarding it to the next Interceptor in the chain.                                                                                                                                                     |
| `org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor`             | `Federation`     | This Class provides an implementation for federation of YARN RM and scaling an application across multiple YARN SubClusters. All the federation specific implementation is encapsulated in this class. This is always the last interceptor in the chain. |
| `org.apache.hadoop.yarn.server.router.clientrm.ApplicationSubmissionContextInterceptor` | `Federation`     | It prevents DoS attack over the ApplicationClientProtocol. Currently, it checks the size of the ApplicationSubmissionContext. If it exceeds the limit it can cause Zookeeper failures.                                                                   |

**How to configure the thread pool of FederationClientInterceptor**

The FederationClientInterceptor retrieves data from multiple subClusters. To improve performance, we utilize a thread pool for concurrent access to multiple subClusters.
Below is the configuration for the thread pool, which can be adjusted based on the requirements of the production environment.

| Property                                                              | Mode    | Description                                                                                                                                                                                                                                  |
|:----------------------------------------------------------------------|:--------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.router.interceptor.user-thread-pool.minimum-pool-size`          | `5`     | This configurable is used to set the corePoolSize(minimumPoolSize) of the thread pool of the interceptor. Default is 5.                                                                                                                      |
| `yarn.router.interceptor.user-thread-pool.maximum-pool-size`          | `5`     | This configuration is used to set the default value of maximumPoolSize of the thread pool of the interceptor. Default is 5.                                                                                                                  |
| `yarn.router.interceptor.user-thread-pool.keep-alive-time`            | `0s`    | This configurable is used to set the keepAliveTime of the thread pool of the interceptor. Default is 0s.                                                                                                                                     |
| `yarn.router.interceptor.user-thread-pool.allow-core-thread-time-out` | `false` | This method configures the policy for core threads regarding termination when no tasks arrive within the keep-alive time. If set to true, We need to ensure that yarn.router.interceptor.user-thread-pool.keep-alive-time is greater than 0. |

- yarn.router.rmadmin.interceptor-class.pipeline

The role of the interceptor is to forward client's administrator requests to RM.

| Property                                                                        | Mode             | Description                                                                                                                                               |
|:--------------------------------------------------------------------------------|:-----------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `org.apache.hadoop.yarn.server.router.rmadmin.DefaultRMAdminRequestInterceptor` | `Non-Federation` | That simply forwards the client requests to the cluster resource manager.                                                                                 |
| `org.apache.hadoop.yarn.server.router.rmadmin.FederationRMAdminInterceptor`     | `Federation`     | Intercept the client's administrator request and forward it to the ResourceManager of Yarn SubClusters. This is always the last interceptor in the chain. |

**How to configure the thread pool of FederationRMAdminInterceptor**

The thread pool configuration used by the `FederationRMAdminInterceptor` is consistent with that of the `FederationClientInterceptor` and can be directly referenced for configuration.

- yarn.router.webapp.interceptor-class.pipeline

The role of the interceptor is to forward client Rest requests to RM.

| Property                                                                    | Mode             | Description                                                                                                                                      |
|:----------------------------------------------------------------------------|:-----------------|:-------------------------------------------------------------------------------------------------------------------------------------------------|
| `org.apache.hadoop.yarn.server.router.webapp.DefaultRequestInterceptorREST` | `Non-Federation` | That simply forwards the client requests to the cluster resource manager.                                                                        |
| `org.apache.hadoop.yarn.server.router.webapp.FederationInterceptorREST`     | `Federation`     | Intercept the client's Rest request and forward it to the ResourceManager of Yarn SubClusters. This is always the last interceptor in the chain. |

How to enable ApplicationSubmissionContextInterceptor

- If the `FederationStateStore` is configured with `Zookpeer` storage, the app information will be stored in `Zookpeer`. If the size of the app information exceeds `1MB`, `Zookpeer` may fail. `ApplicationSubmissionContextInterceptor` will check the size of `ApplicationSubmissionContext`, if the size exceeds the limit(default 1MB), an exception will be thrown.

- The size of the ApplicationSubmissionContext of the application `application_123456789_0001` is above the limit. Size = 1.02 MB.

- The required configuration is as follows:

```xml
<property>
  <name>yarn.router.clientrm.interceptor-class.pipeline</name>
  <value>org.apache.hadoop.yarn.server.router.clientrm.PassThroughClientRequestInterceptor,
         org.apache.hadoop.yarn.server.router.clientrm.ApplicationSubmissionContextInterceptor,
         org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor</value>
</property>
<property>
  <name>yarn.router.asc-interceptor-max-size</name>
  <value>1MB</value>
</property>
```

Optional:

| Property                                        | Example                                                           | Description                                                                                                                                                                                        |
|:------------------------------------------------|:------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.router.hostname`                          | `0.0.0.0`                                                         | Router host name.                                                                                                                                                                                  |
| `yarn.router.clientrm.address`                  | `0.0.0.0:8050`                                                    | Router client address.                                                                                                                                                                             |
| `yarn.router.webapp.address`                    | `0.0.0.0:8089`                                                    | Webapp address at the router.                                                                                                                                                                      |
| `yarn.router.admin.address`                     | `0.0.0.0:8052`                                                    | Admin address at the router.                                                                                                                                                                       |
| `yarn.router.webapp.https.address`              | `0.0.0.0:8091`                                                    | Secure webapp address at the router.                                                                                                                                                               |
| `yarn.router.submit.retry`                      | `3`                                                               | The number of retries in the router before we give up.                                                                                                                                             |
| `yarn.router.submit.interval.time`              | `10ms`                                                            | The interval between two retry, the default value is 10ms.                                                                                                                                         |
| `yarn.federation.cache-ttl.secs`                | `300s`                                                            | The Router caches informations, and this is the time to leave before the cache is invalidated.                                                                                                     |
| `yarn.federation.cache.class`                   | `org.apache.hadoop.yarn.server.federation.cache.FederationJCache` | The Router caches informations, We can configure the Cache implementation and the default implementation is FederationJCache.                                                                      |

#### How to configure Router security

Kerberos supported in federation.

| Property                                  | Example | Description                                                                                                                                                                                                                                                   |
|:------------------------------------------|:--------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.router.keytab.file`                 |         | The keytab file used by router to login as its service principal. The principal name is configured with 'yarn.router.kerberos.principal'.                                                                                                                     |
| `yarn.router.kerberos.principal`          |         | The Router service principal. This is typically set to router/_HOST@REALM.TLD. Each Router will substitute _HOST with its own fully qualified hostname at startup. The _HOST placeholder allows using the same configuration setting on all Routers in setup. |
| `yarn.router.kerberos.principal.hostname` |         | Optional. The hostname for the Router containing this configuration file.  Will be different for each machine. Defaults to current hostname.                                                                                                                  |

#### How to configure Router Cors support

To enable cross-origin support (CORS) for the Yarn Router, please set the following configuration parameters:

| Property                                  | Example                                                       | Description                                                                                              |
|-------------------------------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `hadoop.http.filter.initializers`         | `org.apache.hadoop.security.HttpCrossOriginFilterInitializer` | Optional. Set the filter to HttpCrossOriginFilterInitializer, Configure this parameter in core-site.xml. |
| `yarn.router.webapp.cross-origin.enabled` | `true`                                                        | Optional. Enable/disable CORS filter.Configure this parameter in yarn-site.xml.                          |

#### How to configure Router Cache

Cache is enabled by default. When we set the `yarn.federation.cache-ttl.secs` parameter and its value is greater than 0, Cache will be enabled.
We currently provide two Cache implementations: `JCache` and `GuavaCache`.

- JCache

We used `geronimo-jcache`,`geronimo-jcache` is an implementation of the Java Caching API (JSR-107) specification provided by the Apache Geronimo project.
It defines a standardized implementation of the JCache API, allowing developers to use the same API to access different caching implementations.
In YARN Federation we use a combination of `geronimo-jcache` and `Ehcache`.
If we want to use JCache, we can configure `yarn.federation.cache.class` to `org.apache.hadoop.yarn.server.federation.cache.FederationJCache`.

- GuavaCache

This is a Cache implemented based on the Guava framework.
If we want to use it, we can configure `yarn.federation.cache.class` to `org.apache.hadoop.yarn.server.federation.cache.FederationGuavaCache`.

#### How to configure Router AuditLog

We can enable the AuditLog configuration for the Router and collect the AuditLog in a separate log file. We need to modify the configuration related to RouterAuditLog in the **conf/log4j.properties** file.

The configuration is as follows:

```
router.audit.logger=INFO,ROUTERAUDIT
router.audit.log.maxfilesize=256MB
router.audit.log.maxbackupindex=20
log4j.logger.org.apache.hadoop.yarn.server.router.RouterAuditLogger=${router.audit.logger}
log4j.additivity.org.apache.hadoop.yarn.server.router.RouterAuditLogger=false
log4j.appender.ROUTERAUDIT=org.apache.log4j.RollingFileAppender
log4j.appender.ROUTERAUDIT.File=${hadoop.log.dir}/router-audit.log
log4j.appender.ROUTERAUDIT.layout=org.apache.log4j.PatternLayout
log4j.appender.ROUTERAUDIT.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.ROUTERAUDIT.MaxFileSize=${router.audit.log.maxfilesize}
log4j.appender.ROUTERAUDIT.MaxBackupIndex=${router.audit.log.maxbackupindex}
```

#### How to configure Router Opts

If we need to modify the `HEAPSIZE` or `OPTS` for the Router, we can make changes in the `yarn-env.sh` file.

- YARN_ROUTER_HEAPSIZE

```
# Specify the max heapsize for the Router. If no units are given, it will be assumed to be in MB.
# Default is the same as HADOOP_HEAPSIZE_MAX
export YARN_ROUTER_HEAPSIZE=
```

- YARN_ROUTER_OPTS
```
# Specify the JVM options to be used when starting the Router. These options will be appended to the options specified as HADOOP_OPTS
# and therefore may override any similar flags set in HADOOP_OPTS
export YARN_ROUTER_OPTS="-Drouter.audit.logger=INFO,ROUTERAUDIT"
```

#### How to configure the client to randomly Select a Router

By default, the client will try from the first router in the configured router list. If the connection is successful, the router will not be replaced. We can set `yarn.federation.failover.random.order` to true to allow clients to randomly select Routers.

#### How to configure the cleanup of expired subClusters

We allow the Router to initiate a separate thread for periodically monitoring the status of all subClusters. If a subCluster's heartbeat exceeds a certain time threshold, we categorize that subCluster as "LOST".

| Property                                           | Example | Description                                                                                                                                                               |
|:---------------------------------------------------|:--------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.router.deregister.subcluster.enable`         | `true`  | Whether to enable the capability for automatic subCluster offline. Default is true.                                                                                       |
| `yarn.router.subcluster.heartbeat.expiration.time` | `30m`   | The default subCluster heartbeat timeout time is 30 minutes.                                                                                                              |
| `yarn.router.scheduled.executor.threads`           | `1`     | The number of threads started to check for subCluster timeouts has a default value of 1, and using the default value is sufficient for meeting the checking requirements. |
| `yarn.router.subcluster.cleaner.interval.time`     | `60s`   | The check thread's checking interval. Default 60s.                                                                                                                        |

**Note** We don't need to configure the subCluster deregister checking threads for all Routers; using 1-2 Routers for checking is sufficient.

#### How to configure allow partial result

The Router is used to connect multiple YARN SubClusters and plays a role in merging the returned results from multiple subClusters for certain interfaces. However, if a subcluster undergoes RM upgrade or encounters RM failure, calling that particular RM will not return the correct results.
To address this issue, the Router provides configuration that allows returning partial results. When we configure the relevant parameters, the Router will skip the failed subClusters and only return results from the other subClusters.
This ensures that we can obtain at least some correct results.

| Property                                              | Example | Description                                   |
|:------------------------------------------------------|:--------|:----------------------------------------------|
| `yarn.router.interceptor.allow-partial-result.enable` | `false` | Whether to support returning partial results. |

**Note** It is important to note that even if we configure the parameters, if all sub-clusters return failures, the Router will still throw an exception. This is because there are no available results to return, making it impossible to provide a valid response.

#### How to use Router Command Line

##### Cmd1: deregisterSubCluster

This command is used to `deregister subCluster`, If the interval between the heartbeat time of the subCluster, and the current time exceeds the timeout period, set the state of the subCluster to `SC_LOST`.

Usage:

`yarn routeradmin -deregisterSubCluster [-sc|--subClusterId <subCluster Id>]`

Options:

| Property                              | Description                                                                                                                                                      |
|:--------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-sc, --subClusterId [subCluster Id]` | `'-sc' option allows you to specify the sub-cluster to operate on, while the '--subClusterId' option is the long format of -sc and serves the same purpose.`     |

Examples:

If we want to deregisterSubCluster `SC-1`

```
 yarn routeradmin -deregisterSubCluster -sc SC-1
 yarn routeradmin -deregisterSubCluster --subClusterId SC-1
```

##### Cmd2: policy

We provide a set of commands for Policy Include list policies, save policies, batch save policies.

Usage:

`yarn routeradmin -policy -s|--save (queue;router weight;amrm weight;headroomalpha)`

`yarn routeradmin -policy -bs|--batch-save (--format xml) (-f|--input-file fileName)`

`yarn routeradmin -policy -l|--list ([--pageSize][--currentPage][--queue][--queues])`

###### SubCmd1: -s|--save (<queue;router weight;amrm weight;headroomalpha>)

This command is used to save the policy information of the queue, including queue and weight information.

How to configure `queue;router weight;amrm weight;headroomalpha`

the sum of weights for all sub-clusters in routerWeight/amrmWeight should be 1.

| Property        | Description                                                                                                                                                                     |
|:----------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `queue`         | `Scheduled queue`                                                                                                                                                               |
| `router weight` | `Weight for routing applications to different subclusters.`                                                                                                                     |
| `amrm weight`   | `Weight for resource request from ApplicationMaster (AM) to different subclusters' Resource Manager (RM).`                                                                      |
| `headroomalpha` | `Used by policies that balance weight-based and load-based considerations in their decisions. It is recommended to use 1.0 because the load-base function is not yet complete.` |

Example:

We have two sub-clusters, `SC-1` and `SC-2`. We want to configure a weight policy for the `root.a` queue. The Router Weight is set to `SC-1` with a weight of `0.7` and `SC-2` with a weight of `0.3`.
The AMRM Weight is set `SC-1` to `0.6` and `SC-2` to `0.4`. We are using the default value of `0.1` for `headroomalpha`.

```
yarn routeradmin -policy --save root.a;SC-1:0.7,SC-2:0.3;SC-1:0.6,SC-2:0.4;1.0

yarn routeradmin -policy -s root.a;SC-1:0.7,SC-2:0.3;SC-1:0.6,SC-2:0.4;1.0
```

###### SubCmd2: -bs|--batch-save (--format xml) (-f|--input-file fileName)

This command can batch load weight information for queues based on the provided `federation-weights.xml` file.

| Property                  | Description                                                                                   |
|:--------------------------|:----------------------------------------------------------------------------------------------|
| `--format [xml]`          | `Configuration file format, we currently only support xml format`                             |
| `-f, --input-file [path]` | `The path to the configuration file. Please use the absolute path of the configuration file.` |

How to configure `federation-weights.xml`
```xml
  <federationWeights>
    <weight>
        <queue>
            <name>root.a</name>
            <amrmPolicyWeights>
                <subClusterIdInfo>
                    <id>SC-1</id>
                    <weight>0.7</weight>
                </subClusterIdInfo>
                <subClusterIdInfo>
                    <id>SC-2</id>
                    <weight>0.3</weight>
                </subClusterIdInfo>
            </amrmPolicyWeights>
            <routerPolicyWeights>
                <subClusterIdInfo>
                    <id>SC-1</id>
                    <weight>0.6</weight>
                </subClusterIdInfo>
                <subClusterIdInfo>
                    <id>SC-2</id>
                    <weight>0.4</weight>
                </subClusterIdInfo>
            </routerPolicyWeights>
            <headroomAlpha>1.0</headroomAlpha>
        </queue>
    </weight>
    <weight>
        <queue>
            <name>root.b</name>
            <amrmPolicyWeights>
                <subClusterIdInfo>
                    <id>SC-1</id>
                    <weight>0.8</weight>
                </subClusterIdInfo>
                <subClusterIdInfo>
                    <id>SC-2</id>
                    <weight>0.2</weight>
                </subClusterIdInfo>
            </amrmPolicyWeights>
            <routerPolicyWeights>
                <subClusterIdInfo>
                    <id>SC-1</id>
                    <weight>0.6</weight>
                </subClusterIdInfo>
                <subClusterIdInfo>
                    <id>SC-2</id>
                    <weight>0.4</weight>
                </subClusterIdInfo>
            </routerPolicyWeights>
            <headroomAlpha>1.0</headroomAlpha>
        </queue>
    </weight>
   </federationWeights>
```

Example:

We have two sub-clusters, `SC-1` and `SC-2`. We would like to configure weights for `root.a` and `root.b` queues. We can set the weights for `root.a` and `root.b` in the `federation-weights.xml` file.
and then use the batch-save command to save the configurations in bulk.

The file name can be any file name, but it is recommended to use `federation-weights.xml`

```
yarn routeradmin -policy -bs --format xml -f /path/federation-weights.xml

yarn routeradmin -policy --batch-save --format xml -f /path/federation-weights.xml
```

###### SubCmd3: -l|--list (--pageSize --currentPage --queue --queues)

This command is used to display the configured queue weight information.

| Property        | Description                                                  |
|:----------------|:-------------------------------------------------------------|
| `--pageSize`    | `The number of policies displayed per page.`                 |
| `--currentPage` | `This parameter represents the page number to be displayed.` |
| `--queue`       | `the queue we need to filter. example: root.a`               |
| `--queues`      | `list of queues to filter. example: root.a,root.b,root.c`    |

Example:

We can display the list of already configured queue weight information. We can use the `--queue` option to query the weight information for a specific queue or use the `--queues` option to query the weight information for multiple queues.

```
yarn routeradmin -policy -l --pageSize 20 --currentPage 1 --queue root.a

yarn routeradmin -policy -list --pageSize 20 --currentPage 1 --queues root.a,root.b
```

### ON GPG:

GlobalPolicyGenerator, abbreviated as “GPG”, is used for the automatic generation of global policies for subClusters. The functionality of GPG is still under development and not yet complete. It is not recommended for use in a production environment.

These are extra configurations that should appear in the **conf/yarn-site.xml** for GPG. We allow only one GPG.

Optional:

| Property                                                          | Example                                                                                | Description                                                                                                                                                                      |
|:------------------------------------------------------------------|:---------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.federation.gpg.scheduled.executor.threads`                  | `10`                                                                                   | The number of threads to use for the GPG scheduled executor service. default is 10.                                                                                              |
| `yarn.federation.gpg.subcluster.cleaner.interval-ms`              | `-1`                                                                                   | The interval at which the subcluster cleaner runs, -1 means disabled                                                                                                             |
| `yarn.federation.gpg.subcluster.heartbeat.expiration-ms`          | `30m`                                                                                  | The expiration time for a subcluster heartbeat, default is 30 minutes.                                                                                                           |
| `yarn.federation.gpg.application.cleaner.class`                   | `org.apache.hadoop.yarn.server.globalpolicygenerator.DefaultApplicationCleaner`        | The application cleaner class to use.                                                                                                                                            |
| `yarn.federation.gpg.application.cleaner.interval-ms`             | `-1`                                                                                   | The interval at which the application cleaner runs, -1 means disabled                                                                                                            |
| `yarn.federation.gpg.application.cleaner.contact.router.spec`     | `3,10,600000`                                                                          | Should have three values separated by comma: minimal success retries, maximum total retry, retry interval (ms).                                                                  |
| `yarn.federation.gpg.policy.generator.interval`                   | `1h`                                                                                   | The interval at which the policy generator runs, default is one hour.                                                                                                            |
| `yarn.federation.gpg.policy.generator.class`                      | `org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator.NoOpGlobalPolicy` | The configured policy generator class, runs NoOpGlobalPolicy by default.                                                                                                         |
| `yarn.federation.gpg.policy.generator.readonly`                   | `false`                                                                                | Whether or not the policy generator is running in read only (won't modify policies), default is false.`                                                                          |
| `yarn.federation.gpg.policy.generator.blacklist`                  |                                                                                        | Which sub-clusters the policy generator should blacklist.                                                                                                                        |
| `yarn.federation.gpg.policy.generator.load-based.pending.minimum` | `100`                                                                                  | The minimum number of pending applications in the subCluster.                                                                                                                    |
| `yarn.federation.gpg.policy.generator.load-based.pending.maximum` | `1000`                                                                                 | The maximum number of pending applications in the subCluster.                                                                                                                    |
| `yarn.federation.gpg.policy.generator.load-based.weight.minimum`  | `0`                                                                                    | If a subCluster has a very high load, we will assign this value to the subCluster. The default value is 0, which means that we no longer assign application to this subCluster.  |
| `yarn.federation.gpg.policy.generator.load-based.edit.maximum`    | `3`                                                                                    | This value represents the number of subClusters we want to calculate. default is 3.                                                                                              |
| `yarn.federation.gpg.policy.generator.load-based.scaling`         | `LINEAR`                                                                               | We provide 4 calculation methods: NONE, LINEAR, QUADRATIC, LOG.                                                                                                                  |
| `yarn.federation.gpg.webapp.address`                              | `0.0.0.0:8069`                                                                         | The address of the GPG web application.                                                                                                                                          |
| `yarn.federation.gpg.webapp.https.address`                        | `0.0.0.0:8070`                                                                         | The https address of the GPG web application.                                                                                                                                    |

- yarn.federation.gpg.application.cleaner.contact.router.spec

Specifications on how (many times) to contact Router for apps. We need to
do this because Router might return partial application list because some
sub-cluster RM is not responsive (e.g. failing over). Should have three values separated by comma: minimal success retries,
maximum total retry, retry interval (ms).

- yarn.federation.gpg.policy.generator.load-based.scaling

Note, this calculation method is when the number of Pending Applications in
the subCluster is less than yarn.federation.gpg.policy.generator.load-based.pending.maximum.

maxPendingVal = `yarn.federation.gpg.policy.generator.load-based.pending.maximum` -
`yarn.federation.gpg.policy.generator.load-based.pending.minimum`

curPendingVal = `Pending Applications in the subCluster` -
`yarn.federation.gpg.policy.generator.load-based.pending.minimum`

No calculation is required, and the weight is 1 at this time.

- LINEAR:
  For linear computation,
  we will use (maxPendingVal - curPendingVal) / (maxPendingVal).

- QUADRATIC:
  Calculated using quadratic, We will calculate quadratic for maxPendingVal, curPendingVal,
  then use this formula = (maxPendingVal - curPendingVal) / (maxPendingVal).

- LOG(LOGARITHM):
  Calculated using logarithm, We will calculate logarithm for maxPendingVal, curPendingVal,
  then use this formula = (maxPendingVal - curPendingVal) / (maxPendingVal).

LINEAR is used by default.

**Note**
It is not recommended to use GPG's capability to clean up expired applications in a production environment as this feature is still undergoing further development.

#### How to configure GPG security

| Property                                          | Example | Description                                                                                                                                                                                                                             |
|:--------------------------------------------------|:--------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.federation.gpg.keytab.file`                 |         | The keytab file used by GPG to login as its service principal. The principal name is configured with 'yarn.federation.gpg.kerberos.principal.hostname'.                                                                                 |
| `yarn.federation.gpg.kerberos.principal`          |         | The GPG service principal. This is typically set to GPG/_HOST@REALM.TLD. GPG will substitute _HOST with its own fully qualified hostname at startup. The _HOST placeholder allows using the same configuration setting on GPG in setup. |
| `yarn.federation.gpg.kerberos.principal.hostname` |         | Optional. The hostname for the GPG containing this configuration file. Will be different for each machine. Defaults to current hostname.                                                                                                |

#### How to configure GPG Cors support

To enable cross-origin support (CORS) for the Yarn Router, please set the following configuration parameters:

| Property                                          | Example                                                       | Description                                                                                              |
|---------------------------------------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `hadoop.http.filter.initializers`                 | `org.apache.hadoop.security.HttpCrossOriginFilterInitializer` | Optional. Set the filter to HttpCrossOriginFilterInitializer, Configure this parameter in core-site.xml. |
| `yarn.federation.gpg.webapp.cross-origin.enabled` | `true`                                                        | Optional. Enable/disable CORS filter.Configure this parameter in yarn-site.xml.                          |

#### How to configure GPG Opts

If we need to modify the `HEAPSIZE` or `OPTS` for the GPG, we can make changes in the `yarn-env.sh` file.

- YARN_GLOBALPOLICYGENERATOR_HEAPSIZE
```
# Specify the max heapsize for the Global Policy Generator.
# If no units are given, it will be assumed to be in MB. Default is the same as HADOOP_HEAPSIZE_MAX
# export YARN_GLOBALPOLICYGENERATOR_HEAPSIZE=
```

- YARN_GLOBALPOLICYGENERATOR_OPTS
```
# Specify the JVM options to be used when starting the GPG.
# These options will be appended to the options specified as HADOOP_OPTS and therefore may override any similar flags set in HADOOP_OPTS
#
# See ResourceManager for some examples
# export YARN_GLOBALPOLICYGENERATOR_OPTS=
```

### ON NMs:

These are extra configurations that should appear in the **conf/yarn-site.xml** at each NodeManager.


| Property                                                | Example                                                                     | Description                                                                                                                                          |
|:--------------------------------------------------------|:----------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.nodemanager.amrmproxy.enabled`                    | `true`                                                                      | Whether or not the AMRMProxy is enabled.                                                                                                             |
| `yarn.nodemanager.amrmproxy.interceptor-class.pipeline` | `org.apache.hadoop.yarn.server.nodemanager.amrmproxy.FederationInterceptor` | A comma-separated list of interceptors to be run at the amrmproxy. For federation the last step in the pipeline should be the FederationInterceptor. |

Optional:

| Property                                     | Example | Description                                                                                                                                                                                                             |
|:---------------------------------------------|:--------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `yarn.nodemanager.amrmproxy.ha.enable`       | `true`  | Whether or not the AMRMProxy HA is enabled for multiple application attempt support.                                                                                                                                    |
| `yarn.federation.statestore.max-connections` | `1`     | The maximum number of parallel connections from each AMRMProxy to the state-store. This value is typically lower than the router one, since we have many AMRMProxy that could burn-through many DB connections quickly. |
| `yarn.federation.cache-ttl.secs`             | `300`   | The time to leave for the AMRMProxy cache. Typically larger than at the router, as the number of AMRMProxy is large, and we want to limit the load to the centralized state-store.                                      |

Running a Sample Job
--------------------
In order to submit jobs to a Federation cluster one must create a separate set of configs for the client from which jobs will be submitted. In these, the **conf/yarn-site.xml** should have the following additional configurations:

| Property                                 | Example              | Description                                                           |
|:-----------------------------------------|:---------------------|:----------------------------------------------------------------------|
| `yarn.resourcemanager.address`           | `<router_host>:8050` | Redirects jobs launched at the client to the router's client RM port. |
| `yarn.resourcemanager.scheduler.address` | `localhost:8049`     | Redirects jobs to the federation AMRMProxy port.                      |

Any YARN jobs for the cluster can be submitted from the client configurations described above. In order to launch a job through federation, first start up all the clusters involved in the federation as described [here](../../hadoop-project-dist/hadoop-common/ClusterSetup.html). Next, start up the router on the router machine with the following command:

      $HADOOP_HOME/bin/yarn --daemon start router

Now with $HADOOP_CONF_DIR pointing to the client configurations folder that is described above, run your job the usual way. The configurations in the client configurations folder described above will direct the job to the router's client RM port where the router should be listening after being started. Here is an example run of a Pi job on a federation cluster from the client:

      $HADOOP_HOME/bin/yarn jar hadoop-mapreduce-examples-3.0.0.jar pi 16 1000

This job is submitted to the router which as described above, uses a generated policy from the [GPG](#Global_Policy_Generator) to pick a home RM for the job to which it is submitted.

The output from this particular example job should be something like:

      2017-07-13 16:29:25,055 INFO mapreduce.Job: Job job_1499988226739_0001 running in uber mode : false
      2017-07-13 16:29:25,056 INFO mapreduce.Job:  map 0% reduce 0%
      2017-07-13 16:29:33,131 INFO mapreduce.Job:  map 38% reduce 0%
      2017-07-13 16:29:39,176 INFO mapreduce.Job:  map 75% reduce 0%
      2017-07-13 16:29:45,217 INFO mapreduce.Job:  map 94% reduce 0%
      2017-07-13 16:29:46,228 INFO mapreduce.Job:  map 100% reduce 100%
      2017-07-13 16:29:46,235 INFO mapreduce.Job: Job job_1499988226739_0001 completed successfully
      .
      .
      .
      Job Finished in 30.586 seconds
      Estimated value of Pi is 3.14250000......

The state of the job can also be tracked on the Router Web UI at `routerhost:8089`.
Note that no change in the code or recompilation of the input jar was required to use federation. Also, the output of this job is the exact same as it would be when run without federation. Also, in order to get the full benefit of federation, use a large enough number of mappers such that more than one cluster is required. That number happens to be 16 in the case of the above example.

How to build a Test Federation Cluster
--------------------

The purpose of this document is to help users quickly set up a testing environment for YARN Federation. With this testing environment, users can utilize the core functionality of YARN Federation. This is the simplest test cluster setup (based on Linux) with only essential configurations (YARN non-HA mode). We require 3 machines, and each machine should have at least <4C, 8GB> of resources. We only cover YARN configuration in this document. For information on configuring HDFS and ZooKeeper, please refer to other documentation sources.

Test Environment Description:

- We need to build a HDFS test environment, this part can refer to HDFS documentation. [HDFS SingleCluster](../../hadoop-project-dist/hadoop-common/SingleCluster.html)
- We need two YARN clusters, each YARN cluster has one RM and one NM, The RM and NM on the same node.
- We need one ZK cluster(We only need one ZooKeeper node.), this part can refer to Zookeeper documentation. [ZookeeperStarted](https://zookeeper.apache.org/doc/current/zookeeperStarted.html)
- We need one Router and one Client.

Example of Machine-Role Mapping(Exclude HDFS):

| Machine   | Role          |
|:----------|:--------------|
| Machine A | RM1\NM1\ZK1   |
| Machine B | RM2\NM2       |
| Machine C | Router\Client |

### YARN-1(ClusterTest-Yarn1)

####  RM-1

- For the ResourceManager, we need to configure the following option:

```xml

<!-- YARN cluster-id -->
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>ClusterTest-Yarn1</value>
</property>

<!--
  We can choose to use FairScheduler or CapacityScheduler. Different schedulers have different configuration.
  FairScheduler: org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler
  CapacityScheduler: org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
-->
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
</property>

<!--
 This configuration option is used to specify the configuration file for FairScheduler.
 If we are using CapacityScheduler, we don't need to configure this option.
-->
<property>
  <name>yarn.scheduler.fair.allocation.file</name>
  <value>/path/fair-scheduler.xml</value>
</property>

<!-- Enable YARN Federation mode -->
<property>
  <name>yarn.federation.enabled</name>
  <value>true</value>
</property>

<!-- We use ZooKeeper to query/store Federation information. -->
<property>
  <name>yarn.federation.state-store.class</name>
  <value>org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore</value>
</property>

<!-- ZK Address. -->
<property>
  <name>hadoop.zk.address</name>
  <value>zkHost:zkPort</value>
</property>

```

- Start RM

```
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
```

#### NM-1

- For the NodeManager, we need to configure the following option:

```xml
<!-- YARN cluster-id -->
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>ClusterTest-Yarn1</value>
</property>

<!-- local dir -->
<property>
  <name>yarn.nodemanager.local-dirs</name>
  <value>path/local</value>
</property>

<!-- log dir -->
<property>
  <name>yarn.nodemanager.log-dirs</name>
  <value>path/logdir</value>
</property>

<!-- Enable YARN Federation mode -->
<property>
  <name>yarn.federation.enabled</name>
  <value>true</value>
</property>

<!-- Disable YARN Federation FailOver -->
<property>
  <name>yarn.federation.failover.enabled</name>
  <value>false</value>
</property>

<!-- Enable YARN Federation Non-HA Mode -->
<property>
  <name>yarn.federation.non-ha.enabled</name>
  <value>true</value>
</property>

<!-- We use ZooKeeper to query/store Federation information. -->
<property>
  <name>yarn.federation.state-store.class</name>
  <value>org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore</value>
</property>

<!-- ZK Address. -->
<property>
  <name>hadoop.zk.address</name>
  <value>zkHost:zkPort</value>
</property>

<!-- Enable AmRmProxy. -->
<property>
  <name>yarn.nodemanager.amrmproxy.enabled</name>
  <value>true</value>
</property>

<!-- interceptors to be run at the amrmproxy -->
<property>
  <name>yarn.nodemanager.amrmproxy.interceptor-class.pipeline</name>
  <value>org.apache.hadoop.yarn.server.nodemanager.amrmproxy.FederationInterceptor</value>
</property>
```

- Start NM

```
$HADOOP_HOME/bin/yarn --daemon start nodemanager
```

### YARN-2(ClusterTest-Yarn2)

#### RM-2

The RM of the `YARN-2` cluster is configured the same as the RM of `YARN-1` except for the `cluster-id`

```xml
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>ClusterTest-Yarn2</value>
</property>
```

#### NM-2

The NM of the `YARN-2` cluster is configured the same as the RM of `YARN-1` except for the `cluster-id`

```xml
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>ClusterTest-Yarn2</value>
</property>
```

After we have finished configuring the `YARN-2` cluster, we can proceed with starting the `YARN-2` cluster.

### Router

- For the Router, we need to configure the following option:

```xml
<!-- Enable YARN Federation mode -->
<property>
  <name>yarn.federation.enabled</name>
  <value>true</value>
</property>

<!-- We use ZooKeeper to query/store Federation information. -->
<property>
  <name>yarn.federation.state-store.class</name>
  <value>org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore</value>
</property>

<!-- ZK Address. -->
<property>
  <name>hadoop.zk.address</name>
  <value>zkHost:zkPort</value>
</property>

<!-- Configure the FederationClientInterceptor -->
<property>
  <name>yarn.router.clientrm.interceptor-class.pipeline</name>
  <value>org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor</value>
</property>

<!-- Configure the FederationInterceptorREST -->
<property>
  <name>yarn.router.webapp.interceptor-class.pipeline</name>
  <value>org.apache.hadoop.yarn.server.router.webapp.FederationInterceptorREST</value>
</property>

<!-- Configure the FederationRMAdminInterceptor -->
<property>
  <name>yarn.router.rmadmin.interceptor-class.pipeline</name>
  <value>org.apache.hadoop.yarn.server.router.rmadmin.FederationRMAdminInterceptor</value>
</property>
```

- Start Router

```
$HADOOP_HOME/bin/yarn --daemon start router
```

### Yarn-Client

- For the Yarn-Client, we need to configure the following option:

```xml

<!-- Enable YARN Federation mode -->
<property>
  <name>yarn.federation.enabled</name>
  <value>true</value>
</property>

<!-- Disable YARN Federation FailOver -->
<property>
  <name>yarn.federation.failover.enabled</name>
  <value>false</value>
</property>

<!-- Configure yarn.resourcemanager.address,
   We need to set it to the router address -->
<property>
  <name>yarn.resourcemanager.address</name>
  <value>router-1-Host:8050</value>
</property>

<!-- Configure yarn.resourcemanager.admin.address,
   We need to set it to the router address -->
<property>
  <name>yarn.resourcemanager.admin.address</name>
  <value>router-1-Host:8052</value>
</property>

<!-- Configure yarn.resourcemanager.scheduler.address,
   We need to set it to the AMRMProxy address -->
<property>
  <name>yarn.resourcemanager.scheduler.address</name>
  <value>localhost:8049</value>
</property>
```