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

# YARN DNS Server

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

## Introduction

The YARN DNS Server provides a standard DNS interface to the information posted into the YARN Registry by deployed applications. The DNS service serves the following functions:

1. **Exposing existing service-discovery information via DNS** - Information provided in
the current YARN service registry’s records will be converted into DNS entries, thus
allowing users to discover information about YARN applications using standard DNS
client mechanisms (for e.g. a DNS SRV Record specifying the hostname and port
number for services).
2. **Enabling Container to IP mappings** - Enables discovery of the IPs of containers via
standard DNS lookups. Given the availability of the records via DNS, container
name-based communication will be facilitated (e.g. ‘curl
http://myContainer.myDomain.com/endpoint’).

## Service Properties

The existing YARN Service Registry is leveraged as the source of information for the DNS Service.

The following core functions are supported by the DNS-Server:

### Functional properties

1. Supports creation of DNS records for end-points of the deployed YARN applications
2. Record names remain unchanged during restart of containers and/or applications
3. Supports reverse lookups (name based on IP). Note, this works only for Docker containers.
4. Supports security using the standards defined by The Domain Name System Security
Extensions (DNSSEC)
5. Highly available
6. Scalable - The service provides the responsiveness (e.g. low-latency) required to
respond to DNS queries (timeouts yield attempts to invoke other configured name
servers).

### Deployment properties

1. Supports integration with existing DNS assets (e.g. a corporate DNS server) by acting as
a DNS server for a Hadoop cluster zone/domain. The server is not intended to act as a
primary DNS server and does not forward requests to other servers.
2. The DNS Server exposes a port that can receive both TCP and UDP requests per
DNS standards. The default port for DNS protocols is in a restricted, administrative port
range (5353), so the port is configurable for deployments in which the service may
not be managed via an administrative account.

## DNS Record Name Structure

The DNS names of generated records are composed from the following elements (labels). Note that these elements must be compatible with DNS conventions (see “Preferred Name Syntax” in RFC 1035):

* **domain** - the name of the cluster DNS domain. This name is provided as a
configuration property. In addition, it is this name that is configured at a parent DNS
server as the zone name for the defined yDNS zone (the zone for which the parent DNS
server will forward requests to yDNS). E.g. yarncluster.com
* **username** - the name of the application deployer. This name is the simple short-name (for
e.g. the primary component of the Kerberos principal) associated with the user launching
the application. As the username is one of the elements of DNS names, it is expected
that this also confirms DNS name conventions (RFC 1035 linked above), so special translation is performed for names with special characters like hyphens and spaces.
* **application name** - the name of the deployed YARN application. This name is inferred
from the YARN registry path to the application's node. Application name, rather thn application id, was chosen as a way of making it easy for users to refer to human-readable DNS
names. This obviously mandates certain uniqueness properties on application names.
* **container id** - the YARN assigned ID to a container (e.g.
container_e3741_1454001598828_01_000004)
* **component name** - the name assigned to the deployed component (for e.g. a master
component). A component is a distributed element of an application or service that is
launched in a YARN container (e.g. an HBase master). One can imagine multiple
components within an application. A component name is not yet a first class concept in
YARN, but is a very useful one that we are introducing here for the sake of yDNS
entries. Many frameworks like MapReduce, Slider already have component names
(though, as mentioned, they are not yet supported in YARN in a first class fashion).
* **api** - the api designation for the exposed endpoint

### Notes about DNS Names

* In most instances, the DNS names can be easily distinguished by the number of
elements/labels that compose the name. The cluster’s domain name is always the last
element. After that element is parsed out, reading from right to left, the first element
maps to the application user and so on. Wherever it is not easily distinguishable, naming conventions are used to disambiguate the name using a prefix such as
“container” or suffix such as “api”. For example, an endpoint published as a
management endpoint will be referenced with the name *management-api.griduser.yarncluster.com*.
* Unique application name (per user) is not currently supported/guaranteed by YARN, but
it is supported by frameworks such as Apache Slider. The yDNS service currently
leverages the last element of the ZK path entry for the application as an
application name. These application names have to be unique for a given user.

## DNS Server Functionality

The primary functions of the DNS service are illustrated in the following diagram:

![DNS Functional Overview](../images/dns_overview.png "DNS Functional Overview")

### DNS record creation
The following figure illustrates at slightly greater detail the DNS record creation and registration sequence (NOTE: service record updates would follow a similar sequence of steps,
distinguished only by the different event type):

![DNS Functional Overview](../images/dns_record_creation.jpeg "DNS Functional Overview")

### DNS record removal
Similarly, record removal follows a similar sequence

![DNS Functional Overview](../images/dns_record_removal.jpeg "DNS Functional Overview")

(NOTE: The DNS Zone requires a record as an argument for the deletion method, thus
requiring similar parsing logic to identify the specific records that should be removed).

### DNS Service initialization
* The DNS service initializes both UDP and TCP listeners on a configured port. As
noted above, the default port of 5353 is in a restricted range that is only accessible to an
account with administrative privileges.
* Subsequently, the DNS service listens for inbound DNS requests. Those requests are
standard DNS requests from users or other DNS servers (for example, DNS servers that have the
YARN DNS service configured as a forwarder).

## Start the DNS Server
By default, the DNS runs on non-privileged port `5353`.
If it is configured to use the standard privileged port `53`, the DNS server needs to be run as root:
```
sudo su - -c "yarn org.apache.hadoop.registry.server.dns.RegistryDNSServer > /${HADOOP_LOG_FOLDER}/registryDNS.log 2>&1 &" root
```

## Configuration
The YARN DNS server reads its configuration properties from the yarn-site.xml file.  The following are the DNS associated configuration properties:

| Name | Description |
| ------------ | ------------- |
| hadoop.registry.dns.enabled | The DNS functionality is enabled for the cluster. Default is false. |
| hadoop.registry.dns.domain-name  | The domain name for Hadoop cluster associated records.  |
| hadoop.registry.dns.bind-address | Address associated with the network interface to which the DNS listener should bind.  |
| hadoop.registry.dns.bind-port | The port number for the DNS listener. The default port is 5353. However, since that port falls in a administrator-only range, typical deployments may need to specify an alternate port.  |
| hadoop.registry.dns.dnssec.enabled | Indicates whether the DNSSEC support is enabled. Default is false.  |
| hadoop.registry.dns.public-key  | The base64 representation of the server’s public key. Leveraged for creating the DNSKEY Record provided for DNSSEC client requests.  |
| hadoop.registry.dns.private-key-file  | The path to the standard DNSSEC private key file. Must only be readable by the DNS launching identity. See [dnssec-keygen](https://ftp.isc.org/isc/bind/cur/9.9/doc/arm/man.dnssec-keygen.html) documentation.  |
| hadoop.registry.dns-ttl | The default TTL value to associate with DNS records. The default value is set to 1 (a value of 0 has undefined behavior). A typical value should be approximate to the time it takes YARN to restart a failed container.  |
| hadoop.registry.dns.zone-subnet  | An indicator of the IP range associated with the cluster containers. The setting is utilized for the generation of the reverse zone name.  |
| hadoop.registry.dns.zone-mask | The network mask associated with the zone IP range.  If specified, it is utilized to ascertain the IP range possible and come up with an appropriate reverse zone name. |
| hadoop.registry.dns.zones-dir | A directory containing zone configuration files to read during zone initialization.  This directory can contain zone master files named *zone-name.zone*.  See [here](http://www.zytrax.com/books/dns/ch6/mydomain.html) for zone master file documentation.|
