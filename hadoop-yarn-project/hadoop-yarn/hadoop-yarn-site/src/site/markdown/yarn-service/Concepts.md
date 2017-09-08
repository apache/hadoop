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

# Concepts
This document describes some key concepts and features that makes YARN as a first-class platform in order to natively support long running services on YARN.

### Service Framework (ApplicationMaster) on YARN
A container orchestration framework is implemented to help deploying services on YARN. In a nutshell, the framework is an ApplicationMaster that
requests containers from ResourceManager based on service definition provided by the user and launch the containers across the cluster adhering to placement policies.
It also does all the heavy lifting work such as resolving the service definition and configurations, managing component life cycles such as automatically restarting
failed containers, monitoring components' healthiness and readiness, ensuring dependency start order across components, flexing up/down components, 
upgrading components etc. The end goal of the framework is to make sure the service is up and running as the state that user desired.


### A Restful API-Server for deploying/managing services on YARN
A restful API server is developed to allow users to deploy/manage their services on YARN via a simple JSON spec. This avoids users
from dealing with the low-level APIs, writing complex code to bring their services onto YARN. The REST layer acts as a unified REST based entry for
creation and lifecycle management of YARN services. Services here can range from simple single-component apps to the most complex, 
multi-component applications needing special orchestration needs. Please refer to this [API doc](YarnServiceAPI.md) for detailed API documentations.

The API-server is stateless, which means users can simply spin up multiple instances, and have a load balancer fronting them to 
support HA, distribute the load etc.

### Service Discovery
A DNS server is implemented to enable discovering services on YARN via the standard mechanism: DNS lookup.
The DNS server essentially exposes the information in YARN service registry by translating them into DNS records such as A record and SRV record.
Clients can discover the IPs of containers via standard DNS lookup.
The previous read mechanisms of YARN Service Registry were limited to a registry specific (java) API and a REST interface and are difficult
to wireup existing clients and services. The DNS based service discovery eliminates this gap. Please refer to this [DNS doc](ServiceDiscovery.md) 
for more details.

### Scheduling

A host of scheduling features are being developed to support long running services.

* Affinity and anti-affinity scheduling across containers ([YARN-6592](https://issues.apache.org/jira/browse/YARN-6592)).
* Container resizing ([YARN-1197](https://issues.apache.org/jira/browse/YARN-1197))
* Special handling of container preemption/reservation for services 

### Container auto-restarts

[YARN-3998](https://issues.apache.org/jira/browse/YARN-3998) implements a retry-policy to let NM re-launch a service container when it fails.
The service REST API provides users a way to enable NodeManager to automatically restart the container if it fails.
The advantage is that it avoids the entire cycle of releasing the failed containers, re-asking new containers, re-do resource localizations and so on, which
greatly minimizes container downtime.


### Container in-place upgrade

[YARN-4726](https://issues.apache.org/jira/browse/YARN-4726) aims to support upgrading containers in-place, that is, without losing the container allocations.
It opens up a few APIs in NodeManager to allow ApplicationMasters to upgrade their containers via a simple API call.
Under the hood, NodeManager does below steps:
* Downloading the new resources such as jars, docker container images, new configurations.
* Stop the old container. 
* Start the new container with the newly downloaded resources. 

At the time of writing this document, core changes are done but the feature is not usable end-to-end.

### Resource Profiles

In [YARN-3926](https://issues.apache.org/jira/browse/YARN-3926), YARN introduces Resource Profiles which extends the YARN resource model for easier 
resource-type management and profiles. 
It primarily solves two problems:
* Make it easy to support new resource types such as network bandwith([YARN-2140](https://issues.apache.org/jira/browse/YARN-2140)), disks([YARN-2139](https://issues.apache.org/jira/browse/YARN-2139)).
 Under the hood, it unifies the scheduler codebase to essentially parameterize the resource types.
* User can specify the container resource requirement by a profile name, rather than fiddling with varying resource-requirements for each resource type.
