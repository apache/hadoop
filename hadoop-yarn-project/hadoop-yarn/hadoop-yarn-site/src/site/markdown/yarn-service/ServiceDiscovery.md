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

# Service Discovery

This document describes the mechanism of service discovery on YARN and the
steps for enabling it.

## Overview
A [DNS server](RegistryDNS.html) is implemented to enable discovering services on YARN via
the standard mechanism: DNS lookup.

The framework ApplicationMaster posts the container information such as hostname and IP address into
the YARN service registry. The DNS server exposes the information in YARN service registry by translating them into DNS
records such as A record and SRV record. Clients can then discover the IPs of containers via standard DNS lookup.

For non-docker containers (containers with null `Artifact` or with `Artifact` type set to `TARBALL`), since all containers on the same host share the same ip address,
the DNS supports forward DNS lookup, but not support reverse DNS lookup.
With docker, it supports both forward and reverse lookup, since each container
can be configured to have its own unique IP. In addition, the DNS also supports configuring static zone files for both foward and reverse lookup.

## Docker Container IP Management in Cluster
To support the use-case of per container per IP, containers must be launched with `bridge` network. However, with `bridge` network, containers
running on one node are not routable from other nodes by default. This is not an issue if you are only doing single node testing, however, for
a multi-node environment, containers must be made routable from other nodes.

There are several approaches to solve this depending on the platforms like GCE or AWS. Please refer to specific platform documentations for how to enable this.
For on-prem cluster, one way to solve this issue is, on each node, configure the docker daemon to use a custom bridge say `br0` which is routable from all nodes.
Also, assign an exclusive, contiguous range of IP addresses expressed in CIDR form e.g `172.21.195.240/26 (64 IPs)` to each docker
daemon using the `fixed-cidr` option like  below in the docker `daemon.json`:
```
"bridge": "br0"
"fixed-cidr": "172.21.195.240/26"
```
Check how to [customize docker bridge network](https://docs.docker.com/engine/userguide/networking/default_network/custom-docker0/) for details.


## Naming Convention with Registry DNS
With the DNS support, user can simply access their services in a well-defined naming format as below:

```
${COMPONENT_INSTANCE_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}
```
For example, in a cluster whose domain name is `yarncluster` (as defined by the `hadoop.registry.dns.domain-name` in `yarn-site.xml`), a service named `hbase` deployed by user `devuser`
with two components `hbasemaster` and `regionserver` can be accessed as below:

This URL points to the usual hbase master UI
```
http://hbasemaster-0.hbase.devuser.yarncluster:16010/master-status
```


Note that YARN service framework assigns `COMPONENT_INSTANCE_NAME` for each container in a sequence of monotonically increasing integers. For example, `hbasemaster-0` gets
assigned `0` since it is the first and only instance for the `hbasemaster` component. In case of `regionserver` component, it can have multiple containers
 and so be named as such: `regionserver-0`, `regionserver-1`, `regionserver-2` ... etc

`Disclaimer`: The DNS implementation is still experimental. It should not be used as a fully-functional DNS.


## Configure Registry DNS

Below is the set of configurations in `yarn-site.xml` required for enabling Registry DNS. A full list of properties can be found in the Configuration
section of [Registry DNS](RegistryDNS.html).


```
  <property>
    <description>The domain name for Hadoop cluster associated records.</description>
    <name>hadoop.registry.dns.domain-name</name>
    <value>ycluster</value>
  </property>

  <property>
    <description>The port number for the DNS listener. The default port is 5353.
    If the standard privileged port 53 is used, make sure start the DNS with jsvc support.</description>
    <name>hadoop.registry.dns.bind-port</name>
    <value>5353</value>
  </property>

  <property>
    <description>The DNS functionality is enabled for the cluster. Default is false.</description>
    <name>hadoop.registry.dns.enabled</name>
    <value>true</value>
  </property>

  <property>
    <description>Address associated with the network interface to which the DNS listener should bind.</description>
    <name>hadoop.registry.dns.bind-address</name>
    <value>localhost</value>
  </property>

  <property>
    <description> A comma separated list of hostname:port pairs defining the zookeeper quorum for the YARN registry</description>
    <name>hadoop.registry.zk.quorum</name>
    <value>localhost:2181</value>
  </property>
```
To configure Registry DNS to serve reverse lookup for `172.17.0.0/24`
```
  <property>
    <description>The network mask associated with the zone IP range. If specified, it is utilized to ascertain the
    IP range possible and come up with an appropriate reverse zone name.</description>
    <name>hadoop.registry.dns.zone-mask</name>
    <value>255.255.255.0</value>
  </property>

  <property>
    <description>An indicator of the IP range associated with the cluster containers. The setting is utilized for the
     generation of the reverse zone name.</description>
    <name>hadoop.registry.dns.zone-subnet</name>
    <value>172.17.0.0</value>
  </property>
```
## Start Registry DNS Server
By default, the DNS server runs on non-privileged port `5353`. Start the server
with:
```
yarn --daemon start registrydns
```

If the DNS server is configured to use the standard privileged port `53`, the
environment variables `YARN_REGISTRYDNS_SECURE_USER` and
`YARN_REGISTRYDNS_SECURE_EXTRA_OPTS` must be uncommented in the `yarn-env.sh`
file. The DNS server should then be launched as `root` and jsvc will be used to
reduce the privileges of the daemon after the port has been bound.

## Make your cluster use Registry DNS
You can edit the `/etc/resolv.conf` to make your system use the registry DNS such as below, where `192.168.154.3` is the ip address of your DNS host. It should appear before any nameservers that would return NXDOMAIN for lookups in the domain used by the cluster.
```
nameserver 192.168.154.3
```
Alternatively, if you have a corporate DNS in your organization, you can configure zone forwarding so that the Registry DNS resolves hostnames for the domain used by the cluster.