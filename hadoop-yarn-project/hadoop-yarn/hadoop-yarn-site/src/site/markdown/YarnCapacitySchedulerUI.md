<!--
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

Hadoop: YARN Capacity Scheduler UI
===================================

Overview
--------

The YARN Capacity Scheduler UI is a modern web interface for managing the
YARN Capacity Scheduler configuration. It provides visual queue management,
node label administration, placement rule editing, and global scheduler
settings — all from the browser.

Key features include:

* **Queue Management** — View and edit the queue hierarchy with an interactive
  tree visualization.
* **Staged Changes** — Stage configuration changes, review them as a batch,
  and apply them to the cluster in one operation.
* **Validation** — Real-time validation of configuration changes with
  error and warning severity levels.
* **Node Labels** — Create, delete, and assign node labels to cluster nodes.
* **Placement Rules** — Configure application placement policies.
* **Global Settings** — Manage cluster-wide scheduler parameters.
* **Read-Only Mode** — Optionally restrict the UI to view-only access.

Prerequisites
-------------

The Capacity Scheduler UI must be built by passing `-Pyarn-ui` to Maven.
Refer to `BUILDING.txt` for more details.

Configurations
--------------

*In `yarn-site.xml`*

| Configuration Property | Description |
|:---- |:---- |
| `yarn.webapp.scheduler-ui.enable` | *(Required)* Enables the Capacity Scheduler UI on the ResourceManager. Defaults to `false`. |
| `yarn.webapp.scheduler-ui.war-file-path` | *(Optional)* WAR file path for the Capacity Scheduler UI web application. By default this is empty and YARN will look up the required WAR file from the classpath. |
| `yarn.webapp.scheduler-ui.read-only.enable` | *(Optional)* When set to `true`, the UI operates in read-only mode: users can view configuration and stage changes for review, but cannot apply mutations to the cluster. Defaults to `false`. |

If you run YARN daemons locally for testing, you need the following
configurations added to `yarn-site.xml` to enable cross-origin (CORS) support.

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.resourcemanager.webapp.cross-origin.enabled` | true | Enable CORS support for Resource Manager |

Also ensure that CORS related configurations are enabled in `core-site.xml`.
Refer to [HTTP Authentication](../../hadoop-project-dist/hadoop-common/HttpAuthentication.html)
for details.

Use it
------

Open your browser and go to `rm-address:8088/scheduler-ui`.

Notes
-----

* The Capacity Scheduler UI is served by the ResourceManager as a separate
  web application context at the `/scheduler-ui` path.
* The UI communicates with the ResourceManager via its REST API endpoints
  under `/ws/v1/cluster/`.
* In read-only mode, mutation endpoints (updating scheduler configuration,
  adding/removing node labels, replacing node-to-label mappings) are blocked
  on the client side.
* This UI framework is verified under security environment as well.
