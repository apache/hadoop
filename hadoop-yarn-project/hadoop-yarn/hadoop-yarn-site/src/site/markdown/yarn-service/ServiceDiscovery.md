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

This document describes the mechanism of service discovery on Hadoop and the
steps for enabling it.

## Overview
A [DNS server](../registry/registry-dns.html) is implemented to enable discovering services on Hadoop via
the standard mechanism: DNS lookup.

The framework ApplicationMaster posts the container information such as hostname and IP address into
the Hadoop service registry. The DNS server exposes the information in Hadoop service registry by translating them into DNS
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
For example, in a cluster whose domain name is `yarncluster` (as defined by the `hadoop.registry.dns.domain-name` in `core-site.xml`), a service named `hbase` deployed by user `devuser`
with two components `hbasemaster` and `regionserver` can be accessed as below:

This URL points to the usual hbase master UI
```
http://hbasemaster-0.hbase.devuser.yarncluster:16010/master-status
```


Note that YARN service framework assigns `COMPONENT_INSTANCE_NAME` for each container in a sequence of monotonically increasing integers. For example, `hbasemaster-0` gets
assigned `0` since it is the first and only instance for the `hbasemaster` component. In case of `regionserver` component, it can have multiple containers
 and so be named as such: `regionserver-0`, `regionserver-1`, `regionserver-2` ... etc

Each YARN service component also has Multi-A Records for container fault tolerance or load balancing via RegistryDNS.  The naming format is defined as:
```
${COMPONENT_NAME}.${SERVICE_NAME}.${USER}.${DOMAIN}
```

For example, a component named www for application app launched by Chuck with 3 containers will have DNS records that look like:
```
www.app.chuck.example.com IN A 123.123.123.1
www.app.chuck.example.com IN A 123.123.123.1
www.app.chuck.example.com IN A 123.123.123.1
```

`Disclaimer`: The DNS implementation is still experimental. It should not be used as a fully-functional DNS.
