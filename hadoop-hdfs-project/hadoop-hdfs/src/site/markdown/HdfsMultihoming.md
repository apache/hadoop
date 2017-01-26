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

HDFS Support for Multihomed Networks
====================================

This document is targetted to cluster administrators deploying `HDFS` in multihomed networks. Similar support for `YARN`/`MapReduce` is work in progress and will be documented when available.

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Multihoming Background
----------------------

In multihomed networks the cluster nodes are connected to more than one network interface. There could be multiple reasons for doing so.

1.  **Security**: Security requirements may dictate that intra-cluster
    traffic be confined to a different network than the network used to
    transfer data in and out of the cluster.

2.  **Performance**: Intra-cluster traffic may use one or more high bandwidth
    interconnects like Fiber Channel, Infiniband or 10GbE.

3.  **Failover/Redundancy**: The nodes may have multiple network adapters
    connected to a single network to handle network adapter failure.

Note that NIC Bonding (also known as NIC Teaming or Link
Aggregation) is a related but separate topic. The following settings
are usually not applicable to a NIC bonding configuration which handles
multiplexing and failover transparently while presenting a single 'logical
network' to applications.

Fixing Hadoop Issues In Multihomed Environments
-----------------------------------------------

### Ensuring HDFS Daemons Bind All Interfaces

By default `HDFS` endpoints are specified as either hostnames or IP addresses. In either case `HDFS` daemons will bind to a single IP address making the daemons unreachable from other networks.

The solution is to have separate setting for server endpoints to force binding the wildcard IP address `INADDR_ANY` i.e. `0.0.0.0`. Do NOT supply a port number with any of these settings.

**NOTE:** Prefer using hostnames over IP addresses in master/slave configuration files.

    <property>
      <name>dfs.namenode.rpc-bind-host</name>
      <value>0.0.0.0</value>
      <description>
        The actual address the RPC server will bind to. If this optional address is
        set, it overrides only the hostname portion of dfs.namenode.rpc-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node listen on all interfaces by
        setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.servicerpc-bind-host</name>
      <value>0.0.0.0</value>
      <description>
        The actual address the service RPC server will bind to. If this optional address is
        set, it overrides only the hostname portion of dfs.namenode.servicerpc-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node listen on all interfaces by
        setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.http-bind-host</name>
      <value>0.0.0.0</value>
      <description>
        The actual address the HTTP server will bind to. If this optional address
        is set, it overrides only the hostname portion of dfs.namenode.http-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node HTTP server listen on all
        interfaces by setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.https-bind-host</name>
      <value>0.0.0.0</value>
      <description>
        The actual address the HTTPS server will bind to. If this optional address
        is set, it overrides only the hostname portion of dfs.namenode.https-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node HTTPS server listen on all
        interfaces by setting it to 0.0.0.0.
      </description>
    </property>

### Clients use Hostnames when connecting to DataNodes

By default `HDFS` clients connect to DataNodes using the IP address provided by the NameNode. Depending on the network configuration this IP address may be unreachable by the clients. The fix is letting clients perform their own DNS resolution of the DataNode hostname. The following setting enables this behavior.

    <property>
      <name>dfs.client.use.datanode.hostname</name>
      <value>true</value>
      <description>Whether clients should use datanode hostnames when
        connecting to datanodes.
      </description>
    </property>

### DataNodes use HostNames when connecting to other DataNodes

Rarely, the NameNode-resolved IP address for a DataNode may be unreachable from other DataNodes. The fix is to force DataNodes to perform their own DNS resolution for inter-DataNode connections. The following setting enables this behavior.

    <property>
      <name>dfs.datanode.use.datanode.hostname</name>
      <value>true</value>
      <description>Whether datanodes should use datanode hostnames when
        connecting to other datanodes for data transfer.
      </description>
    </property>

Multihoming and Hadoop Security
-------------------------------

Configuring multihomed hosts with [Hadoop in Secure Mode](../hadoop-common/SecureMode.html) may require additional configuration.

### Hostname Lookup

Kerberos principals for Hadoop Services are specified using the pattern `ServiceName/_HOST@REALM.TLD` e.g. `nn/_HOST@REALM.TLD`. This allows the same configuration file to be used on all hosts. Services will substitute `_HOST` in the principal with their own hostname looked up at runtime.

When nodes are configured to have multiple hostnames in DNS or in `/etc/hosts` files, a service may lookup a different hostname than what is expected by the server. e.g. intra-cluster traffic between two services may be routed over a private interface but the client service looked up its public hostname. Kerberos authentication will fail since the hostname in the principal does not match the IP address over which the traffic arrived.

The following setting (available starting Apache Hadoop 2.8.0) can be used to control the hostname looked up the service.

    <property>
      <name>hadoop.security.dns.interface</name>
      <description>
        The name of the Network Interface from which the service should determine
        its host name for Kerberos login. e.g. eth2. In a multi-homed environment,
        the setting can be used to affect the _HOST subsitution in the service
        Kerberos principal. If this configuration value is not set, the service
        will use its default hostname as returned by
        InetAddress.getLocalHost().getCanonicalHostName().

        Most clusters will not require this setting.
      </description>
    </property>

Services can also be configured to use a specific DNS server for hostname lookups (rarely required).

    <property>
      <name>hadoop.security.dns.nameserver</name>
      <description>
        The host name or IP address of the name server (DNS) which a service Node
        should use to determine its own host name for Kerberos Login. Requires
        hadoop.security.dns.interface.

        Most clusters will not require this setting.
      </description>
    </property>
