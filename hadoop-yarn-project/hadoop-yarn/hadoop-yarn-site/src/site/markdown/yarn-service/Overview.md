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

# YARN Service
## Overview
Yarn Service framework provides first class support and APIs to host long running services natively in YARN. 
In a nutshell, it serves as a container orchestration platform for managing containerized services on YARN. It supports both docker container
and traditional process based containers in YARN.

The responsibility of this framework includes performing configuration resolutions and mounts, 
lifecycle management such as stop/start/delete the service, flexing service components up/down, rolling upgrades services on YARN, monitoring services' healthiness and readiness and more.

The yarn-service framework primarily includes below components:

* A core framework (ApplicationMaster) running on YARN to serve as a container orchestrator, being responsible for all service lifecycle managements.
* A restful API-server to for users to interact with YARN to deploy/manage their services via a simple JSON spec.
* A DNS server backed by YARN service registry to enable discovering services on YARN by the standard DNS lookup.

## Why should I try YARN Service framework?

YARN Service framework makes it easy to bring existing services onto YARN.
It hides all the complex low-level details of application management and relieves
users from forced into writing new code. Developers of new services do not have
to worry about YARN internals and only need to focus on containerization of their
service(s).

Further, another huge win of this feature is that now you can enable both
traditional batch processing jobs and long running services in a single platform!
The benefits of combining these workloads are two-fold:

* Greatly simplify the cluster operations as you have only a single cluster to deal with.
* Making both batch jobs and services share a cluster can greatly improve resource utilization.

## How do I get started?

*`This feature is in alpha state`* and so APIs, command lines are subject to change. We will continue to update the documents over time.

[QuickStart](QuickStart.md) shows a quick tutorial that walks you through simple steps to deploy a service on YARN.

## How do I get my hands dirty?

* [Concepts](Concepts.md): Describes the internals of the framework and some features in YARN core to support running services on YARN.
* [Service REST API](YarnServiceAPI.md): The API doc for deploying/managing services on YARN.
* [Service Discovery](ServiceDiscovery.md): Describes the service discovery mechanism on YARN.
* [Registry DNS](RegistryDNS.md): Deep dives into the Registry DNS internals.
* [Examples](Examples.md): List some example service definitions (`Yarnfile`).
* [Configurations](Configurations.md): Describes how to configure the custom services on YARN.


 
