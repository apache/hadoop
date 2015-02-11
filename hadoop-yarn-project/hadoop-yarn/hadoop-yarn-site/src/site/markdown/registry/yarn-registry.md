<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

## The YARN Service Registry

# Introduction and concepts

This document describes a YARN service registry built to address two problems:

1. How can clients talk to YARN-deployed services and the components which form
such services?
1. Allow Hadoop core services to be registered and discovered thereby
reducing configuration parameters and to allow core services to be more
easily moved.

Service registration and discovery is a long-standing problem in distributed
computing, dating back to Xerox's Grapevine Service. This proposal is for a
registry for locating distributed applications deployed by YARN, and
determining the binding information needed to communicate with these
applications.

### Definitions
**Service**: a potentially-distributed application deployed in —or reachable
from— a Hadoop YARN cluster. Examples: Apache HBase, Apache hcatalog, Apache
Storm. Services may be short-lived or long-lived.

**Service Class:** the name of a type of service, used as a path in a registry
and matching the DNS-compatible path naming scheme. Examples:
`org-apache-hbase`, `org-apache-hcatalog`

**Component**: a distributed element of a service. Examples: HBase master
nodes, HBase region servers and HBase REST servers.

**Service Instance:** A single instance of an application. Example, an HBase
cluster `demo1`. A service instance is running if the instances the components
which for the service are running. This does not imply "live" in the
distributed computing sense, merely that the process are running.

**Component Instance**: a single instance of a component within a service
instance. Examples: an HBase master node on host `rack1server6` or a region
server on host `rack3server40`.

**Endpoint**: one means of binding with a service instance or a component
instance. Examples: HBase's Apache Zookeeper binding, a Java JMX port on a
region server, a Web UI on an HBase Master, and the REST API of an HBase REST
component instance. Endpoints may be *internal* —for use within the service
instance, or *external*: for use by clients of a service instance.

**Service Record**: a record in the registry describing a service instance or a
component instance, including listing its endpoints.

**YARN Resource Manager, "RM":** the YARN component which allows client
applications to submit work (including requests to deploy service instances) to
a YARN cluster. The RM retains state on all running applications.

**YARN Application**: An application deployed via YARN. Every application
instance has a unique application ID.

**YARN Application Master, "AM":** the application-specific component which is
scheduled and deployed by the RM. It has the duty of maintaining the internal
state of the application, including requesting and managing all other component
instances of this application instance. The YARN RM will detect the failure of
the AM, and respond by rescheduling it.

**YARN Container:** An allocation of resources, including CPU and RAM, for a
component instance. The AM has the responsibility of requesting the containers
its components need, and building the commands to instantiate its component
instances onto allocated containers. Every allocated container has a unique
container ID.

## The binding problem
Hadoop YARN allows applications to run on the Hadoop cluster. Some of these are
batch jobs or queries that can managed via Yarn’s existing API using its
application ID. In addition YARN can deploy ong-lived services instances such a
pool of Apache Tomcat web servers or an Apache HBase cluster. YARN will deploy
them across the cluster depending on the individual each component requirements
and server availability. These service instances need to be discovered by
clients; traditionally their IP added is registered in DNS or in some
configuration file —but that is not feasible in YARN-deployed applications when
neither the hostname nor network ports can be known in advance.

As a result there is no easy way for clients to interact with dynamically
deployed applications.

YARN supports a rudimentary registry which allows YARN Application Masters to
register a web URL and an IPC address. but is not sufficient for our purposes
since it It does not allow any other *endpoints* to be registered —such as REST
URLs, or zookeeper path or the endpoints of the tasks that the Application
Master executes. Further, information that can be registered is mapped to the
YARN application instance —a unique instance ID that changes whenever a YARN
application is started. This makes it impossible to resolve binding information
via a static reference to a named service, or to even probe for the existence
of a service instance which is not currently live.

# Use Cases

## Service Name Examples


Core Hadoop services.

These may be deployed statically, dynamically via an account with the
permissions to write to the `/services` path, or even registrations of remote
services accessible from within the Hadoop cluster

        /services/hdfs
        /services/yarn
        /services/oozie

Yarn-deployed services belonging to individual users.

        /users/joe/org-apache-hbase/demo1
        /users/joe/org-apache-hbase/demo1/components/regionserver1

## Registration Use Cases

1. A Hadoop core service that is not running under YARN example: HDFS) can be
registered in for discovery. This could be done by the service or by management
tools..

2. A long-lived application deployed by YARN registers itself for discovery by
clients. The registration data is intended to outlive the application master,
and perhaps the lifespan of a single deployment of the service instance.

3. Component instances of a service register themselves, publishing internal
binding information, such as JMX ports.

4. A YARN-deployed application can bind to dependent service instances both
static and dynamic. Example: a Tomcat web pool binding to the dynamic HBase
service instance "/users/joe/services/hbase/demo1".

5. Component Instances use the registry to bind to an internal endpoint of
their application master, to which they heartbeat regularly.

## Unsupported Registration use cases:

1. A short-lived Yarn application is registered automatically in the registry,
including all its containers. and unregistered when the job terminates.
Short-lived applications with many containers will place excessive load on a
registry. All YARN applications will be given the option of registering, but it
will not be automatic —and application authors must be advised against
registering short-lived containers.

## Lookup Use Cases

1. A client application looks up a dynamically deployed service instance whose
user, service class and instance name is known, e.g.
`/users/joe/services/hbase/demo1`, and retrieves the information needed to
connect to the service

2. A client application looks up a statically deployed Hadoop service Example:
`/services/hdfs`.

3. An Application Master enumerates all registered component instances,
discovers their listed JMX ports, and, initializes own web UI, offers links to these
endpoints.

4. A user connects to a private HBase service instance at
`/users/joe/services/hbase/demo1`.

5. A user connects to the cluster’s HBase service at `/services/hbase`.

6. A user looks up the binding information to a remote Hadoop cluster's
filesystem at `/net/cluster4/services/hdfs`. The registration information
includes the `webhdfs://` URL for the remote filesystem.

7. A user lists their HBase service instances:

        ls /users/joe/services/hbase

8. User finds all Hbase services in the cluster:

        find -endpointField.api=org.apache.hbase

9. Possibly in future: looking up a service via DNS.

This registry proposal is intended to support these use cases by providing a
means for applications to register their service endpoints, and for clients to
locate them.

# Key Requirements of a Service Registry

Allow dynamic registration of service instances

  * YARN deployed services instances must be able register their bindings and be
  discovered by clients.

  * Core Hadoop service instances must be able to register their service
  endpoints.

  * The binding must be upgradable if the service moves or in case if HA fails
  over.

  * A service instance must be able to publish a variety of endpoints for a
  service: Web UI, RPC, REST, Zookeeper, others. Furthermore one must also be
  able register certificates and other public security information may be
  published as part of a binding.

Registry service properties:

  * The registry must be highly available.

  * Scale: many services and many clients in a large cluster. This will limit
  how much data a service can publish.

  * Ubiquity: we need this in every YARN cluster, whether physical, virtual or
  in-cloud.

  * Must support hierarchical namespace and names. The name convention must
  match that of DNS so that we have the option of accessing the namespace via
  DNS protocol at a later phase of the project.

  * Registry API Language/Protocols

  * Cross-language: independent of any language; client language != service

  * REST API for reading registry data

Access Control:

  * Read access for all

  * Write is restricted so that squatting and impersonation can be avoided.

Remote accessibility: supports remote access even on clusters which are
  only reachable via Apache Knox, or hosted in cloud environments.

## Non-Requirements

* The registry is not intended for liveness detection, leader-election or
  perform other "shared consensual state" actions for an application itself,
  other than potentially sharing binding information between component
  instances.

* The registry is not intended to be a store for arbitrary application state,
  or for publishing configuration data other than binding information to
  endpoints offered by a service and its components. Such use would overload
  the registry and rapidly reach limits of what Zookeeper permits.

# Architecture

We propose a base registry service that binds string-names to records
describing service and component instances. We plan to use ZK as the base name
service since it supports many of the properties, We pick a part of the ZK
namespace to be the root of the service registry ( default: `yarnRegistry`).

On top this base implementation we build our registry service API and the
naming conventions that Yarn will use for its services. The registry will be
accessed by the registry API, not directly via ZK - ZK is just an
implementation choice (although unlikely to change in the future).

1. Services are registered by binding a **_path_** to a value called a
**_Service Record_**. Paths are hierarchical and use `/` as the root as well as
the separator.

2. Service records are registered as persistent znodes. This ensures that the
record remains present during planned and unplanned outages of the service, on
the assumption that client code is resilient to transient outages.

3. Each service instance’s service record lists the endpoints for its various
protocols exported by that service instance.

4. For each protocol endpoint it must contain

     1. The *protocol* name including: Web, REST, IPC, zookeeper. (type:string)

     2. Its *address*: the specific details used to locate this endpoint

     3. Its *addressType*. This is the format of the binding string. (URL, ZK path,
        hostname:port pair). For the predefined protocols, we will define what
        format the binding string MUST be. Example: `protocol==REST` means binding
        type is `URL`, `protocol==IPC` binding uses the addresstype `host/port`.

     4. The *api*. This is the API offered by the endpoint, and is application
        specific. examples: `org.apache.hadoop.namenode`,
        `org.apache.hadoop.webhdfs`

5. Endpoints may be *external* —for use by programs other than the service
itself, and *internal* —for connecting components within the service instance.
They will be listed in different sections of the Service Record to distinguish
them.

6. Core services will be registered using the following convention:
`/services/{servicename}` e.g. `/services/hdfs`.

7. Yarn services SHOULD be registered using the following convention:

        /users/{username}/{serviceclass}/{instancename}

6. Component instances SHOULD be registered under

        /users/{username}/{serviceclass}/{instancename}/components/{componentname}


8. Each of the user's services which follows this convention must have unique
service class names,

9. Each component instance must have a name that is unique for that service
instance. For a YARN-deployed application, this can be trivially
derived from the container ID.

The requirements for unique names ensures that the path to a service instance
or component instance is guaranteed to be unique, and that all instances of a
specific service class can be enumerated by listing all children of the service
class path.


# Registry Model

Service entries MUST be persistent —it is the responsibility of YARN and other
tools to determine when a service entry is to be deleted.

## Path Elements

All path elements MUST match that of a lower-case entry in a hostname path as
defined in RFC1123; the regular expression is:

    ([a-z0-9]|([a-z0-9][a-z0-9\-]*[a-z0-9]))

This policy will ensure that were the registry hierarchy ever to exported by a
DNS service, all service classes and names would be valid.

A complication arises with user names, as platforms may allow user names with
spaces, high unicode and other characters in them. Such paths must be converted
to valid DNS hostname entries using the punycode convention used for
internationalized DNS.

## Service Record

A Service Record has some basic information and possibly empty lists of
internal and external endpoints.

### Service Record:

A Service Record contains some basic informations and two lists of endpoints:
one list for users of a service, one list for internal use within the
application.

<table>
  <tr>
    <td>Name</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>type: String</td>
    <td>Always: "JSONServiceRecord"</td>
  </tr>
  <tr>
    <td>description: String</td>
    <td>Human-readable description.</td>
  </tr>
  <tr>
    <td>external: List[Endpoint]</td>
    <td>A list of service endpoints for external callers.</td>
  </tr>
  <tr>
    <td>internal: List[Endpoint]</td>
    <td>A list of service endpoints for internal use within the service instance.</td>
  </tr>
</table>

The type field MUST be `"JSONServiceRecord"`. Mandating this string allows
future record types *and* permits rapid rejection of byte arrays that
lack this string before attempting to parse the data with a JSON parser.

### YARN Persistence policies

The YARN Resource Manager integration integrates cleanup of service records
as an application, attempt or container is completed.

This allows service to register entries which have a lifespan bound to one of
these aspects of YARN applications' lifecycles. This is a feature which is only
supported when the RM has had its registry integration enabled via the
configuration option `hadoop.registry.rm.enabled`.

If this option is enabled, and the YARN resource manager is running,
it will clean up service records as defined
below.

If the option is disabled, the RM does not provide any registry support at all.


The attributes, `yarn:id` and `yarn:persistence` specify which records
*and any child entries* may be deleted as the associated YARN components complete.

The `yarn:id` field defines the application, attempt or container ID to match;
the `yarn:persistence` attribute defines the trigger for record cleanup, and
implicitly the type of the contents of the `yarn:id` field.

These attributes use the prefix "`yarn:`" to indicate that their reliance on
the YARN layer of the Hadoop cluster to implement the policy. If the registry
were to run standalone —which is entirely possible— all records would be
implicitly persistent.

<table>
  <tr>
    <td>Name</td>
    <td>Description</td>
    <td>contents of `yarn:id` field</td>
  </tr>
  <tr>
    <td>permanent</td>
    <td>The record persists until removed manually.</td>
    <td>(unused)</td>
  </tr>
  <tr>
    <td>application</td>
    <td>Remove when the YARN application defined in the id field terminates.</td>
    <td>application ID</td>
  </tr>
  <tr>
    <td>application-attempt</td>
    <td>Remove when the current YARN application attempt finishes.</td>
    <td>application attempt ID</td>
  </tr>
  <tr>
    <td>container</td>
    <td>Remove when the YARN container in the ID field finishes</td>
    <td>container ID</td>
  </tr>
</table>


The policies which clean up when an application, application attempt or
container terminates require the `yarn:id` field to match that of the
application, attempt or container. If the wrong ID is set, the cleanup does not
take place —and if set to a different application or container, will be cleaned
up according the lifecycle of that application.

### Endpoint:

<table>
  <tr>
    <td>Name</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>api: URI as String</td>
    <td>API implemented at the end of the binding</td>
  <tr>
    <td>protocol: String</td>
    <td>Protocol. Examples:
`http`, `https`, `hadoop-rpc`, `zookeeper`, `web`, `REST`, `SOAP`, ...</td>
  </tr>
  <tr>
    <td>addressType: String</td>
    <td>format of the binding</td>
  </tr>
  </tr>
    <tr>
    <td>addresses: List[Map[String, String]]</td>
    <td>a list of address maps</td>
  </tr>

</table>


All string fields have a limit on size, to dissuade services from hiding
complex JSON structures in the text description.

#### Field `addressType`: Address Type

The `addressType` field defines the string format of entries.

Having separate types is that tools (such as a web viewer) can process binding
strings without having to recognize the protocol.

<table>
  <tr>
    <td>Format</td>
    <td>binding format</td>
  </tr>
  <tr>
    <td>uri</td>
    <td>uri:URI of endpoint</td>
  </tr>
  <tr>
    <td>hostname</td>
    <td>hostname: service host</td>
  </tr>
  <tr>
    <td>inetaddress</td>
    <td>hostname: service host, port: service port</td>
  </tr>
  <tr>
    <td>path</td>
    <td>path: generic unix filesystem path</td>
  </tr>
  <tr>
    <td>zookeeper</td>
    <td>hostname: service host, port: service port, path: ZK path</td>
  </tr>
</table>


In the zookeeper binding, every entry represents a single node in quorum,
the `hostname` and `port` fields defining the hostname of the ZK instance
and the port on which it is listening. The `path` field lists zookeeper path
for applications to use. For example, for HBase this would refer to the znode
containing information about the HBase cluster.

The path MUST be identical across all address elements in the `addresses` list.
This ensures that any single address contains enough information to connect
to the quorum and connect to the relevant znode.

New Address types may be defined; if not standard please prefix with the
character sequence `"x-"`.

### Field `api`: API identifier

The API field MUST contain a URI that identifies the specific API of an endpoint.
These MUST be unique to an API to avoid confusion.

The following strategies are suggested to provide unique URIs for an API

1. The SOAP/WS-* convention of using the URL to where the WSDL defining the service
2. A URL to the svn/git hosted document defining a REST API
3. the `classpath` schema followed by a path to a class or package in an application.
4. The `uuid` schema with a generated UUID.

It is hoped that standard API URIs will be defined for common APIs. Two such non-normative APIs are used in this document

* `http://` : A web site for humans
* `classpath:javax.management.jmx`: and endpoint supporting the JMX management protocol (RMI-based)


### Examples of Service Entries

Here is an example of a service entry for a YARN-deployed tomcat application.

After creation and registration of the application, the registry looks as follows:

    /users
      /devteam
       /org-apache-tomcat
         /test1
           /components
             /container-1408631738011-0001-01-000002
             /container-1408631738011-0001-01-000001

The `/users/devteam/org-apache-tomcat/tomcat-test` service record describes the
overall application. It exports the URL to a load balancer.

    {
      "description" : "tomcat-based web application",
      "external" : [ {
        "api" : "http://internal.example.org/restapis/scheduler/20141026v1",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [
         { "uri" : "http://loadbalancer/" },
         { "uri" : "http://loadbalancer2/" }
          ]
      } ],
      "internal" : [ ]
    }

The service instance is built from two component instances, each described with
their container ID converted into a DNS-compatible hostname. The entries are
marked as ephemeral. If the entries were set within the container, then when
that container is released or if the component fails, the entries will be
automatically removed. Accordingly, it's persistence policy is declared to be
"3", container. The `yarn:id` field identifies the container whose completion
will trigger the deletion of this entry

    /users/devteam/org-apache-tomcat/test1/components/container-1408631738011-0001-01-000001

    {
      "yarn:id" : "container_1408631738011_0001_01_000001",
      "yarn:persistence" : "container",
      "description" : "",
      "external" : [ {
        "api" : "http://internal.example.org/restapis/scheduler/20141026v1",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [{ "uri" : "rack4server3:43572" }  ]
      } ],
      "internal" : [ {
        "api" : "classpath:javax.management.jmx",
        "addressType" : "host/port",
        "protocol" : "rmi",
        "addresses" : [ {
          "host" : "rack4server3",
          "port" : "48551"
        } ]
      } ]
    }

The component instances list their endpoints: the public REST API as an
external endpoint, the JMX addresses as internal.

    /users/devteam/org-apache-tomcat/test1/components/container-1408631738011-0001-01-000002

    {
      "registrationTime" : 1408638082445,
      "yarn:id" : "container_1408631738011_0001_01_000002",
      "yarn:persistence" : "container",
      "description" : null,
      "external" : [ {
        "api" : "http://internal.example.org/restapis/scheduler/20141026v1",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ [ "http://rack1server28:35881" ] ]
      } ],
      "internal" : [ {
        "api" : "classpath:javax.management.jmx",
        "addressType" : "host/port",
        "protocol" : "rmi",
        "addresses" : [ {
          "host" : "rack1server28",
          "port" : "48551"
        } ]
      } ]
    }

This information could be used by the (hypothetical) load balancer to enumerate
the components and build a list of component instances to dispatch requests to.
Similarly, a management application could enumerate all available component
instances and their JMX ports, then connect to each to collect performance
metrics.

# Registry API

Here is the registry API as seen from a Java application. The API is a thin
layer above the ZK operations, essentially building up paths, reading, writing
and updating entries, and enumerating children. The REST API is implemented
inside a server and use this same API to implement its REST API.

The exceptions that are listed are only a subset of possible exception —the
interface merely lists those that have special meaning.

All write operations must assume that they are communicating with a registry
service with the consistency view of a Zookeeper client; read-only clients must
assume that their view may be somewhat out of date.

All clients must recognize that the registry is a shared resource and that
it may change during a sequence of actions.

### Registry Operations

    public interface RegistryOperations extends Service {

      /**
       * Create a path.
       *
       * It is not an error if the path exists already, be it empty or not.
       *
       * The createParents flag also requests creating the parents.
       * As entries in the registry can hold data while still having
       * child entries, it is not an error if any of the parent path
       * elements have service records.
       *
       * @param path path to create
       * @param createParents also create the parents.
       * @throws PathNotFoundException parent path is not in the registry.
       * @throws InvalidPathnameException path name is invalid.
       * @throws IOException Any other IO Exception.
       * @return true if the path was created, false if it existed.
       */
      boolean mknode(String path, boolean createParents)
          throws PathNotFoundException,
          InvalidPathnameException,
          IOException;

      /**
       * Set a service record to an entry
       * @param path path to service record
       * @param record service record service record to create/update
       * @param createFlags creation flags
       * @throws PathNotFoundException the parent path does not exist
       * @throws FileAlreadyExistsException path exists but create flags
       * do not include "overwrite"
       * @throws InvalidPathnameException path name is invalid.
       * @throws IOException Any other IO Exception.
       */
      void bind(String path, ServiceRecord record, int createFlags)
          throws PathNotFoundException,
          FileAlreadyExistsException,
          InvalidPathnameException,
          IOException;

      /**
       * Resolve the record at a path
       * @param path path to service record
       * @return the record
       * @throws PathNotFoundException path is not in the registry.
       * @throws InvalidPathnameException the path is invalid.
       * @throws IOException Any other IO Exception
       */

      ServiceRecord resolve(String path) throws PathNotFoundException,
          InvalidPathnameException,
          IOException;

      /**
       * Get the status of a path
       * @param path path to query
       * @return the status of the path
       * @throws PathNotFoundException path is not in the registry.
       * @throws InvalidPathnameException the path is invalid.
       * @throws IOException Any other IO Exception
       */
      RegistryPathStatus stat(String path)
          throws PathNotFoundException,
          InvalidPathnameException,
          IOException;

      /**
       * Probe for a path existing.
       * This is equivalent to {@link #stat(String)} with
       * any failure downgraded to a
       * @param path path to query
       * @return true if the path was found
       * @throws IOException
       */
      boolean exists(String path) throws IOException;

     /**
       * List all entries under a registry path
       * @param path path to query
       * @return a possibly empty list of the full path names of
       * child entries
       * @throws PathNotFoundException
       * @throws InvalidPathnameException
       * @throws IOException
       */
       List<String> list(String path) throws
          PathNotFoundException,
          InvalidPathnameException,
          IOException;

      /**
       * Delete a path.
       *
       * If the operation returns without an error then the entry has been
       * deleted.
       * @param path path delete recursively
       * @param recursive recursive flag
       * @throws PathNotFoundException path is not in the registry.
       * @throws InvalidPathnameException the path is invalid.
       * @throws PathIsNotEmptyDirectoryException path has child entries, but
       * recursive is false.
       * @throws IOException Any other IO Exception
       *
       */
      void delete(String path, boolean recursive)
          throws PathNotFoundException,
          PathIsNotEmptyDirectoryException,
          InvalidPathnameException,
          IOException;

      /**
       * Add a new write access entry to be added to node permissions in all
       * future write operations of a session connected to a secure registry.
       *
       * This does not grant the session any more rights: if it lacked any write
       * access, it will still be unable to manipulate the registry.
       *
       * In an insecure cluster, this operation has no effect.
       * @param id ID to use
       * @param pass password
       * @return true if the accessor was added: that is, the registry connection
       * uses permissions to manage access
       * @throws IOException on any failure to build the digest
       */
      boolean addWriteAccessor(String id, String pass) throws IOException;

      /**
       * Clear all write accessors.
       *
       * At this point all standard permissions/ACLs are retained,
       * including any set on behalf of the user
       * Only  accessors added via {@link #addWriteAccessor(String, String)}
       * are removed.
       */
      public void clearWriteAccessors();
    }


### `RegistryPathStatus`

The `RegistryPathStatus` class summarizes the contents of a node in the registry.

    public class RegistryPathStatus {

      /**
       * Short path in the registry to this entry
       */
      public String path;

      /**
       * Timestamp
       */
      public long time;

      /**
       * Entry size in bytes, as returned by the storage infrastructure.
       * In zookeeper, even "empty" nodes have a non-zero size.
       */
      public long size;

      /**
       * Number of child nodes
       */
      public int children;
    }


## Security

The registry will allow a service instance can only be registered under the
path where it has permissions. Yarn will create directories with appropriate
permissions for users where Yarn deployed services can be registered by a user.
of the user account of the service instance. The admin will also create
directories (such as `/services`) with appropriate permissions (where core Hadoop
services can register themselves.

There will no attempt to restrict read access to registry information. The
services will protect inappropriate access by clients by requiring
authentication and authorization. There is a *scope* field in a service record
, but this is just a marker to say "internal API only", rather than a direct
security restriction. (this is why "internal" and "external" are proposed, not
"public" and "private").

Rationale: the endpoints being registered would be discoverable through port
scanning anyway. Having everything world-readable allows the REST API to have a
simpler access model —and is consistent with DNS.

On a secure cluster, ZK token renewal may become an issue for long-lived
services —if their token expires their session may expire. Renewal of such
tokens is not part of the API implementation —we may need to add a means to
update the tokens of an instance of the registry operations class.

### Security Policy Summary

In an a non-Kerberos Zookeeper Cluster, no security policy is implemented.

The registry is designed to be secured *on a kerberos-managed cluster*.

* The registry root grants full rights to "system accounts":
`mapred`, `hdfs`, `yarn` : `"rwcda"`; all other accounts, and anonymous access
is read-only.

* The permissions are similarly restricted for `/users`, and `/services/`

* installations may extend or change these system accounts.

* When an application belonging to a user is scheduled, YARN
SHALL create an entry for that user `/users/${username}`.

* This node will have full access to the system; the user the access rights:
`"crd"`. That is, they may create or delete child nodes, but not write to
their home node, —or alter its permissions.

* Applications wishing to write to the registry must use a SASL connection
to authenticate via Zookeeper,

* Applications creating nodes in the user path MUST include the site-specified
system accounts in the ACL list, with full access.

* Applications creating nodes in the user path MUST include an ACL Which

* Applications creating nodes in the user path MUST declare their own
user identity as a `sasl:user@REALM` entry.

* Applications creating nodes the user path MAY add extra `digest:` ACL tokens
so as to give their services the ability to manipulate portions of the
registry *without needing kerberos credentials*.

The digest-driven authentication avoid the problem of credential renewal in
long-lived applications. An YARN application may be passed the token to
connect with the ZK service when launched. It can then create or update an
entry, including a secret digest ACL in the permissions of nodes it creates.
As a result, even after the credentials expire, it retains *some* access.

Note that for this to be successful, the client will need to fall back
session to *not* use SASL, instead using authentication id:pass credentials.


## Out of cluster and cross-cluster access

1. A client should be able to access the registry of another cluster in order
to access services of that cluster. Detail of this need to further fleshed out.

2. Firewall services such as Apache Knox can examine the internal set of
published services, and publish a subset of their endpoints. They MAY implement
a future REST API.

# Limits

**Entry Size**

Zookeeper has a default limit of 1MB/node. If all endpoints of a service or
component are stored in JSON attached to that node, then there is a total limit
of 1MB of all endpoint registration data.

To prevent this becoming a problem, the client API should implement strict
limits on the maximum length of fields, with low limits on the addressType,
protocol, and api fields, something longer on the description and addresses
elements —along with a limit on the number of elements in the addresses field.

**Name size**

To support DNS in future, there must be a limit of 63 bytes on all path
elements. For non-ASCII User names, this restriction implies that a shorter
path may be a limit.

**Rate of Update**

A rapid rate of entry change is considered antisocial in a ZK cluster.
Implementations may throttle update operations.

**Rate of Polling**

Clients which poll the registry may be throttled.

# Complete service record example

Below is a (non-normative) example of a service record retrieved
from a YARN application.


    {
      "type" : "JSONServiceRecord",
      "description" : "Slider Application Master",
      "yarn:persistence" : "application",
      "yarn:id" : "application_1414052463672_0028",
      "external" : [ {
        "api" : "classpath:org.apache.slider.appmaster",
        "addressType" : "host/port",
        "protocol" : "hadoop/IPC",
        "addresses" : [ {
          "port" : "48551",
          "host" : "nn.example.com"
        } ]
      }, {
        "api" : "http://",
        "addressType" : "uri",
        "protocol" : "web",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.management",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743/ws/v1/slider/mgmt"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.publisher",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743/ws/v1/slider/publisher"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.registry",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743/ws/v1/slider/registry"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.publisher.configurations",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743/ws/v1/slider/publisher/slider"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.publisher.exports",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "http://nn.example.com:40743/ws/v1/slider/publisher/exports"
        } ]
      } ],
      "internal" : [ {
        "api" : "classpath:org.apache.slider.agents.secure",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "https://nn.example.com:52705/ws/v1/slider/agents"
        } ]
      }, {
        "api" : "classpath:org.apache.slider.agents.oneway",
        "addressType" : "uri",
        "protocol" : "REST",
        "addresses" : [ {
          "uri" : "https://nn.example.com:33425/ws/v1/slider/agents"
        } ]
      } ]
    }

It publishes a number of endpoints, both internal and external.

External:

1. The IPC hostname and port for client-AM communications
1. URL to the AM's web UI
1. A series of REST URLs under the web UI for specific application services.
The details are irrelevant —note that they use an application-specific API
value to ensure uniqueness.

Internal:

1. Two URLs to REST APIs offered by the AM for containers deployed by
 the application itself.

Python agents running in the containers retrieve the internal endpoint
URLs to communicate with their AM. The record is resolved on container startup
and cached until communications problems occur. At that point the registry is
queried for the current record, then an attempt is made to reconnect to the AM.

Here "connectivity" problems means both "low level socket/IO errors" and
"failures in HTTPS authentication". The agents use two-way HTTPS authentication
—if the AM fails and another application starts listening on the same ports
it will trigger an authentication failure and hence service record reread.
