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

In addition, it leverages a lot of features in YARN core to accomplish scheduling constraints, such as
affinity and anti-affinity scheduling, log aggregation for services, automatically restart a container if it fails, and do in-place upgrade of a container.

### A Restful API-Server for deploying/managing services on YARN
A restful API server is developed to allow users to deploy/manage their services on YARN via a simple JSON spec. This avoids users
from dealing with the low-level APIs, writing complex code to bring their services onto YARN. The REST layer acts as a unified REST based entry for
creation and lifecycle management of YARN services. Services here can range from simple single-component apps to the most complex, 
multi-component applications needing special orchestration needs. Please refer to this [API doc](YarnServiceAPI.md) for detailed API documentations.

The API-server is stateless, which means users can simply spin up multiple instances, and have a load balancer fronting them to 
support HA, distribute the load etc.

### Service Discovery
A DNS server is implemented to enable discovering services on YARN via the standard mechanism: DNS lookup.

The framework posts container information such as hostname and ip into the [YARN service registry](../registry/index.md). And the DNS server essentially exposes the
information in YARN service registry by translating them into DNS records such as A record and SRV record.
Clients can then discover the IPs of containers via standard DNS lookup.

The previous read mechanisms of YARN Service Registry were limited to a registry specific (java) API and a REST interface and are difficult
to wireup existing clients and services. The DNS based service discovery eliminates this gap. Please refer to this [Service Discovery doc](ServiceDiscovery.md)
for more details.