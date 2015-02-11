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

# Registry Security

This document describes how security is implemented in the service registry

In a non-Kerberos-enabled Hadoop cluster, the Registry does not offer any
security at all: the registry is world writeable.

This document is therefore relevant only to secure clusters.

## Security Model

The security model of the registry is designed to meet the following goals
a secure registry:
1. Deliver functional security on a secure ZK installation.
1. Allow the RM to create per-user regions of the registration space
1. Allow applications belonging to a user to write registry entries
into their part of the space. These may be short-lived or long-lived
YARN applications,  or they may be be static applications.
1. Prevent other users from writing into another user's part of the registry.
1. Allow system services to register to a `/services` section of the registry.
1. Provide read access to clients of a registry.
1. Permit future support of DNS
1. Permit the future support of registering data private to a user.
This allows a service to publish binding credentials (keys &c) for clients to use.
1. Not require a ZK keytab on every user's home directory in a YARN cluster.
This implies that kerberos credentials cannot be used by YARN applications.


ZK security uses an ACL model, documented in
[Zookeeper and SASL](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)
In which different authentication schemes may be used to restrict access
to different znodes. This permits the registry to use a mixed
Kerberos + Private password model.

* The YARN-based registry (the `RMRegistryOperationsService`), uses kerberos
as the authentication mechanism for YARN itself.
* The registry configures the base of the registry to be writeable only by
itself and other hadoop system accounts holding the relevant kerberos credentials.
* The user specific parts of the tree are also configured to allow the same
system accounts to write and manipulate that part of the tree.
* User accounts are created with a `(username,password)` keypair granted
write access to their part of the tree.
* The secret part of the keypair is stored in the users' home directory
on HDFS, using the Hadoop Credentials API.
* Initially, the entire registry tree will be world readable.


What are the limitations of such a scheme?

1. It is critical that the user-specific registry keypair is kept a secret.
This relies on filesystem security to keep the file readable only
 by the (authenticated) user.
1. As the [ZK Documentation says](http://zookeeper.apache.org/doc/r3.4.6/zookeeperProgrammers.html#sc_ZooKeeperAccessControl),
*" Authentication is done by sending the username:password in clear text"
1. While it is possible to change the password for an account,
this involves a recursive walk down the registry tree, and will stop all
running services from being able to authenticate for write access until they
reload the key.
1. A world-readable registry tree is exposing information about the cluster.
There is some mitigation here in that access may be restricted by IP Address.
1. There's also the need to propagate information from the registry down to
the clients for setting up ACLs.



## ACL Configuration propagation

The registry manager cannot rely on clients consistently setting
ZK permissions. At the very least, they cannot relay on client applications
unintentionally wrong values for the accounts of the system services

*Solution*: Initially, a registry permission is used here.

### Automatic domain extension

In a kerberos domain, it is possible for a kerberized client to determine the
realm of a cluster at run time from the local
user's kerberos credentials as used to talk to YARN or HDFS.

This can be used to auto-generate account names with the correct realm for the
system accounts hence aid having valid constants.

This allows the registry to support a default configuration value for
`hadoop.registry.system.accounts` of:

      "sasl:yarn@, sasl:mapred@, sasl:hdfs@, sasl:hadoop@";

#### In-registry publishing of core binding data

Another strategy could be to have a `ServiceRecord` at the root
of the registry that actually defines the registry —including listing
those default binding values in the `data` field..

### Auditing

Something (perhaps the RM) could scan a user's portion of the registry and
detect some ACL problems: IP/world access too lax, admin account settings wrong.
It cannot view or fix the ACL permissions unless it has the `ADMIN` permission,
though that situation can at least be detected. Given the RM must have `DELETE`
permissions further up the stack, it would be in a position to delete the errant
part of the tree —though this could be a destructive overreaction.

## Further Reading

* [Zookeeper and SASL](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)
* [Up and Running with Secure Zookeeper](https://github.com/ekoontz/zookeeper/wiki)
