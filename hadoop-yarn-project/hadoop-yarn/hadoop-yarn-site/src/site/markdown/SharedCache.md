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

YARN Shared Cache
===================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

The YARN Shared Cache provides the facility to upload and manage shared
application resources to HDFS in a safe and scalable manner. YARN applications
can leverage resources uploaded by other applications or previous
runs of the same application without having to re­upload and localize identical files
multiple times. This will save network resources and reduce YARN application
startup time.

Current Status and Future Plans
------------

Currently the YARN Shared Cache is released and ready to use. The major
components are implemented and have been deployed in a large-scale
production setting. There are still some pieces missing (i.e. strong
authentication). These missing features will be implemented as part of a
follow-up phase 2 effort. Please see
[YARN-7282](https://issues.apache.org/jira/browse/YARN-7282) for more information.

Architecture
------------

The shared cache feature consists of 4 major components:

1. The shared cache client.
2. The HDFS directory that acts as a cache.
3. The shared cache manager (aka. SCM).
4. The localization service and uploader.

### The Shared Cache Client

YARN application developers and users, should interact with the shared cache using
the shared cache client. This client is responsible for interacting with the
shared cache manager, computing the checksum of application resources, and
claiming application resources in the shared cache. Once an application has claimed
a resource, it is free to use that resource for the life-cycle of the application.
Please see the SharedCacheClient.java javadoc for further documentation.

### The Shared Cache HDFS Directory

The shared cache HDFS directory stores all of the shared cache resources. It is protected
by HDFS permissions and is globally readable, but writing is restricted to a trusted user.
This HDFS directory is only modified by the shared cache manager and the resource uploader
on the node manager. Resources are spread across a set of subdirectories using the resources's
checksum:
```
/sharedcache/a/8/9/a896857d078/foo.jar
/sharedcache/5/0/f/50f11b09f87/bar.jar
/sharedcache/a/6/7/a678cb1aa8f/job.jar
```

### Shared Cache Manager (SCM)

The shared cache manager is responsible for serving requests from the client and
managing the contents of the shared cache. It looks after both the meta data as
well as the persisted resources in HDFS. It is made up of two major components,
a back end store and a cleaner service. The SCM runs as a separate daemon
process that can be placed on any node in the cluster. This allows for
administrators to start/stop/upgrade the SCM without affecting other YARN
components (i.e. the resource manager or node managers).

The back end store is responsible for maintaining and persisting metadata about
the shared cache. This includes the resources in the cache, when a resource was
last used and a list of applications that are currently using the resource. The
implementation for the backing store is pluggable and it currently uses an
in-memory store that recreates its state after a restart.

The cleaner service maintains the persisted resources in HDFS by ensuring that
resources that are no longer used are removed from the cache. It scans the
resources in the cache periodically and evicts resources if they are both stale
and there are no live applications currently using the application.

### The Shared Cache uploader and localization

The shared cache uploader is a service that runs on the node manager and adds
resources to the shared cache. It is responsible for verifying a resources
checksum, uploading the resource to HDFS and notifying the shared cache
manager that a resource has been added to the cache. It is important to note
that the uploader service is asynchronous from the container launch and does not
block the startup of a yarn application. In addition adding things to the cache
is done in a best effort way and does not impact running applications. Once the
uploader has placed a resource in the shared cache, YARN uses the normal node
manager localization mechanism to make resources available to the application.

Developing YARN applications with the Shared Cache
------------

To support the YARN shared cache, an application must use the shared cache
client during application submission. The shared cache client returns a
URL corresponding to a resource if it is in the shared cache. To use the cached
resource, a YARN application simply uses the cached URL to create a
LocalResource object and sets setShouldBeUploadedToSharedCache to true during
application submission.

For example, here is how you would create a LocalResource using a cached URL:
```
String localPathChecksum = sharedCacheClient.getFileChecksum(localPath);
URL cachedResource = sharedCacheClient.use(appId, localPathChecksum);
LocalResource resource = LocalResource.newInstance(cachedResource,
      LocalResourceType.FILE, LocalResourceVisibility.PUBLIC
      size, timestamp, null, true);
```

Administrating the Shared Cache
------------

### Setting up the shared cache
An administrator can initially set up the shared cache by following these steps:

1. Create an HDFS directory for the shared cache (default: /sharedcache).
2. Set the shared cache directory permissions to 0755.
3. Ensure that the shared cache directory is owned by the user that runs the
shared cache manager daemon and the node manager.
4. In the yarn-site.xml file, set *yarn.sharedcache.enabled* to true and
*yarn.sharedcache.root-dir* to the directory specified in step 1. For more configuration
parameters, see the configuration parameters section.
5. Start the shared cache manager:
```
/hadoop/bin/yarn --daemon start sharedcachemanager
```

### Configuration parameters
The configuration parameters can be found in yarn-default.xml and should be set
in the yarn-site.xml file. Here are a list of configuration parameters and their defaults:

Name | Description | Default value
--- | --- | ---
yarn.sharedcache.enabled | Whether the shared cache is enabled | false
yarn.sharedcache.root-dir | The root directory for the shared cache | /sharedcache
yarn.sharedcache.nested-level | The level of nested directories before getting to the checksum directories. It must be non-negative. | 3
yarn.sharedcache.store.class | The implementation to be used for the SCM store | org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore
yarn.sharedcache.app-checker.class | The implementation to be used for the SCM app-checker | org.apache.hadoop.yarn.server.sharedcachemanager.RemoteAppChecker
yarn.sharedcache.store.in-memory.staleness-period-mins | A resource in the in-memory store is considered stale if the time since the last reference exceeds the staleness period. This value is specified in minutes. | 10080
yarn.sharedcache.store.in-memory.initial-delay-mins | Initial delay before the in-memory store runs its first check to remove dead initial applications. Specified in minutes. | 10
yarn.sharedcache.store.in-memory.check-period-mins | The frequency at which the in-memory store checks to remove dead initial applications. Specified in minutes. | 720
yarn.sharedcache.admin.address | The address of the admin interface in the SCM (shared cache manager) | 0.0.0.0:8047
yarn.sharedcache.admin.thread-count | The number of threads used to handle SCM admin interface (1 by default) | 1
yarn.sharedcache.webapp.address | The address of the web application in the SCM (shared cache manager) | 0.0.0.0:8788
yarn.sharedcache.cleaner.period-mins | The frequency at which a cleaner task runs. Specified in minutes. | 1440
yarn.sharedcache.cleaner.initial-delay-mins | Initial delay before the first cleaner task is scheduled. Specified in minutes. | 10
yarn.sharedcache.cleaner.resource-sleep-ms | The time to sleep between processing each shared cache resource. Specified in milliseconds. | 0
yarn.sharedcache.uploader.server.address | The address of the node manager interface in the SCM (shared cache manager) | 0.0.0.0:8046
yarn.sharedcache.uploader.server.thread-count | The number of threads used to handle shared cache manager requests from the node manager (50 by default) | 50
yarn.sharedcache.client-server.address | The address of the client interface in the SCM (shared cache manager) | 0.0.0.0:8045
yarn.sharedcache.client-server.thread-count | The number of threads used to handle shared cache manager requests from clients (50 by default) | 50
yarn.sharedcache.checksum.algo.impl | The algorithm used to compute checksums of files (SHA-256 by default) | org.apache.hadoop.yarn.sharedcache.ChecksumSHA256Impl
yarn.sharedcache.nm.uploader.replication.factor | The replication factor for the node manager uploader for the shared cache (10 by default) | 10
yarn.sharedcache.nm.uploader.thread-count | The number of threads used to upload files from a node manager instance (20 by default) | 20