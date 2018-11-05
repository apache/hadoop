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

# Using the Hadoop Service Registry

The Hadoop service registry can be used in a number of ways :-

1. To register dynamic YARN-deployed applications with entries that match the
   lifespan of the YARN application.
   Service Records can be set to be deleted on
   the completion of the YARN application, the application attempt,
   or an individual container.
1. To look up static or dynamic applications and the mechanisms to communicate
   with them.
   Those mechanisms can include: HTTP(S) URLs, Zookeeper paths,
   hostnames and ports and even paths in a Hadoop filesystem to
   configuration data.
1. On a secure cluster, to verify that a service binding has been published
   by a specific user, or a system account.
   This can be done simply by looking at the path under which an entry has
   been placed.
1. To register static applications.
   These will remain in the registry until deleted.
    They can be updated as required.

A user of the registry may be both a publisher of entries —Service Records—
and a consumer of other services located via their service records.
Different parts of a distributed application may also use it for different
purposes. As an example, the Application Master of a YARN application
can publish bindings for use by its worker containers. The code running in the containers
which can then look up the bindings to communicate with that manager even
if it was restarted on different nodes in the cluster. Client applications can
look up external service endpoints to interact with the AM via a public API.

The registry cannot be used:-

* To subscribe to service records or registry paths and listen for changes.
* To directly share arbitrary data from a server for their clients.
  Such data must be published by some other means, a means which the registry
  entry can publish.
* To share secrets between processes. The registry is world readable.


## Registry Application Design Patterns


### Short-lived YARN Application Masters registering their public service endpoints.

1. A YARN application is deployed. In a secure cluster, it is given the kerberos
   token to write to the registry.
2. When launched, it creates a service record at a known path
3. This record MAY have application attempt persistence policy of and an ID of
   the application attempt

		yarn:persistence = "application_attempt"
		yarn:id = ${application_attemptId}

	 This means that the record will be deleted when the application attempt
	 completes, even if a new attempt is created. Every Application attempt will have to re-register the endpoint —which may be needed to locate the service anyway.
4. Alternatively, the record MAY have the persistence policy of "application":

		yarn:persistence = "application_attempt"
		yarn:id = application_attemptId
	This means that the record will persist even between application attempts, albeit with out of date endpoint information.
5. Client applications look up the service by way of the path.

The choice of path is an application specific one.
For services with a YARN application name guaranteed to be unique,
we recommend a convention of:

	/users/${username}/applications/${service-class}/${instance-name}

Alternatively, the application Id can be used in the path:

	/users/${username}/applications/${service-class}/${applicationId}

The latter makes mapping a YARN application listing entry to a service record trivial.

Client applications may locate the service

* By enumerating all instances of a service class and selecting one by specific critera.
* From a supplied service class and instance name
* If listed by application ID, from the service class and application ID.

After locating a service record, the client can enumerate the `external`
bindings and locate the entry with the desired API.


### YARN Containers registering their public service endpoints

Here all containers in a YARN application are publishing service endpoints
for public consumption.

1. The deployed containers are passed the base path under which they should
   register themselves.
2. Long-lived containers must be passed an `id:password` pair which gives
   them the right to update these entries without the kerberos credentials of the user. This allows the containers to update their entries even after the user tokens granting the AM write access to a registry path expire.
3. The containers instantiate a registry operations instance with the
   `id:password` pair.
4. They then a register service record on a path consisting of:

		${base-path} + "/" + RegistryPathUtils.encodeYarnID(containerId)

	This record should have the container persistence policy an ID of the container

		yarn:persistence = "container"
		yarn:id = containerId

	When the container is terminated, the entry will be automatically deleted.

5. The exported service endpoints of this container-deployed service should
   be listed in the `external` endpoint list of the service record.
6. Clients can enumerate all containers exported by a YARN application by
   listing the entries under `${base-path}`.


### Registering Static cluster services.

Services which are generally fixed in a cluster, but which need to publish
binding and configuration information may be published in the registry.
Example: an Apache Oozie service.
Services external to the cluster to which deployed applications may also
be published. Example: An Amazon Dynamo instance.


These services can be registered under paths which belong to the users
running the service, such as `/users/oozie` or `/users/hbase`.
Client applications would use this path.
While this can authenticate the validity of the service record,
it does rely on the client applications knowing the username a service
is deployed on, or being configured with the full path.

The alternative is for the services to be deployed under a static services path,
under `/services`. For example, `/services/oozie` could contain
the registration of the Oozie service.
As the permissions for this path are restricted to pre-configured
system accounts, the presence of a service registration on this path on a secure
cluster, confirms that it was registered by the cluster administration tools.

1. The service is deployed by some management tool, or directly by
   the cluster operator.
2. The deployed application can register itself under its own user name
   if given the binding information for the registry.
3. If the application is to be registered under `/services` and it has been
   deployed by one of the system user accounts —it may register itself directly.
4. If the application does not have the permissions to do so, the cluster
   administration tools must register the service instead.
5. Client applications may locate a service by resolving its well
   known/configured path.
6. If a service is stopped, the administration tools may delete the entry,
   or retain the entry but delete all it service endpoints.
   This is a proposed convention to indicate
   "the service is known but not currently reachable".
7. When a service is restarted, its binding information may be updated,
   or its entire registry entry recreated.


### YARN containers locating their Application Master

Here YARN containers register with their AM to receive work, usually by some
heartbeat mechanism where they report in regularly.
If the AM is configured for containers to outlive the application attempt,
when an AM fails the containers keep running.
These containers will need to bind to any restarted AM.
They may also wish to conclude that if an AM does not restart,
that they should eventually time out and terminate themselves.
Such a policy helps the application react to network partitions.

1. The YARN AM publishes its service endpoints such as the FQDN and
   socket port needed for IPC communications, or an HTTP/HTTPS URL needed
   for a REST channel.
   These are published in the `internal` endpoint list, with the
   `api` field set to a URL of the specific API the containers use.
1. The YARN containers are launched with the path to the service record
   (somehow) passed to them.
   Environment variables or command line parameters are two viable mechanisms.
   Shared secrets should also be passed that way: command line parameters are
   visible in the unix `ps` command.
   More secure is saving shared secrets to the cluster filesystem,
   passing down the path to the containers. The URI to such as path MAY be one
   of the registered internal endpoints of the application.
1. The YARN containers look up the service registry to identify the
   communications binding.
1. If the registered service entry cannot be found, the container MAY do one of:
   exit. spin with some (jittered) retry period, polling for the entry, until
   the entry reappears. This implies that the AM has been found.
1. If the service entry is found, the client should attempt to communicate
   with the AM on its channel.
   Shared authentication details may be used to validate the client with the
   server and vice versa.
1. The client report in to the AM until the connections start failing to
   connect or authenticate, or when a long lived connection is broken
   and cannot be restarted.
1. A this point the client may revert to step (3).
   Again, some backoff policy with some jitter helps stop a
   newly-restarted AM being overloaded.
   Containers may also with to have some timeout after which they conclude
   that the AM is not coming back and exit.
1. We recommend that alongside the functional commands that an AM may
   issue to a client, a "terminate" command can be issued to a container.
   This allows the system to handle the specific situation of the
   YARN Node Manager terminating while spawned containers keep running.

### YARN Applications and containers publishing their management and metrics bindings

Management ports and bindings are simply others endpoint to publish.
These should be published as *internal* endpoints, as they are not
intended for public consumption.

### Client application enumerating services by endpoint APIs

A client application wishes to locate all services implementing a specific API,
such as `"classpath://org.apache.hbase"`

1. The client starts from a path in the registry
1. The client calls `registryOperations.list(path)` to list all nodes directly
   under that path, getting a relative list of child nodes.
1. the client enumerates the child record statuses by calling `stat()`
   on each child.
1. For all status entries, if the size of the entry is greater than the
   value of `ServiceRecordHeader.getLength()`, it MAY contain a service record.
1. The contents can be retrieved using the `resolve()` operation.
   If successful, it does contain a service record —so the client can enumerate
   the `external` endpoints and locate the one with the desired API.
1. The `children` field of each `RegistryPathStatus` status entry should
   be examined. If it is >= 0, the enumeration should be performed recursively on the path of that entry.
1. The operation ultimately completes with a list of all entries.
1. One of the enumerated endpoints may be selected and used as the binding information
   for a service

This algorithm describes a depth first search of the registry tree.
Variations are of course possible, including breadth-first search,
or immediately halting the search as soon as a single entry point.
There is also the option of parallel searches of different subtrees
—this may reduce search time, albeit at the price of a higher client
load on the registry infrastructure.

A utility class `RegistryUtils` provides static utility methods for
common registry operations,in particular,
`RegistryUtils.listServiceRecords(registryOperations, path)`
performs the listing and collection of all immediate child record entries of
a specified path.

Client applications are left with the problem of "what to do when the endpoint
is not valid", specifically, when a service is not running —what should be done?

Some transports assume that the outage is transient, and that spinning retries
against the original binding is the correct strategy. This is the default
policy of the Hadoop IPC client.

Other transports fail fast, immediately reporting the failure via an
exception or other mechanism. This is directly visible to the client —but
does allow the client to rescan the registry and rebind to the application.

Finally, some application have been designed for dynamic failover from the
outset: their published binding information is actually a zookeeper path.
Apache HBase and Apache Accumulo are examples of this. The registry is used
for the initial lookup of the binding, after which the clients are inherently
resilient to failure.
