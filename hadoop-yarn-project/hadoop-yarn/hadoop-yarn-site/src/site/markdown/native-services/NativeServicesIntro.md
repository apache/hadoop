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

# Introduction: YARN Native Services

## Overview
YARN Native Services provides first class framework support and APIs to host long running services natively in YARN. In addition to launching services, the new APIs support performing lifecycle management operations, such as flex service components up/down, manage lifetime, upgrade the service to a newer version, and stop/restart/delete the service.

The native services capabilities are built on the existing low-level resource management API provided by YARN that can support any type of application. Other application frameworks like Hadoop MapReduce already expose higher level APIs that users can leverage to run applications on top of YARN. With the advent of containerization technologies like Docker, providing first class support and APIs for long running services at the framework level made sense.

Relying on a framework has the advantage of exposing a simpler usage model to the user by enabling service configuration and launch through specification (without writing new code), as well as hiding complex low-level details including state management and fault-tolerance etc. Users/operators of existing services typically like to avoid modifying an existing service to be aware of YARN. With first class support capable of running a single Docker image as well as complex assemblies comprised of multiple Docker images, there is no need for service owners to be aware of YARN. Developers of new services do not have to worry about YARN internals and only need to focus on containerization of their service(s).

## First class support for services
In order to natively provide first class support for long running services, several new features and improvements have been made at the framework level.

### Incorporate Apache Slider into Apache YARN
Apache Slider, which existed as a separate incubator project has been merged into YARN to kick start the first class support. Apache Slider is a universal Application Master (AM) which had several key features built in - fault tolerance of service containers and AM, work-preserving AM restarts, service logs management, service management like flex up/down, stop/start, and rolling upgrade to newer service versions, etc. Of course lot more work has been done on top of what Apache Slider brought in, details of which follow.

### Native Services API
A significant effort has gone into simplifying the user facing story for building services. In the past, bringing a new service to YARN was not a pleasant experience. The APIs of existing frameworks are either too low-level (native YARN), require writing new code (for frameworks with programmatic APIs) or require writing a complex spec (for declarative frameworks).

The new REST APIs are very simple to use. The REST layer acts as a single point of entry for creation and lifecycle management of YARN services. Services here can range from simple single-component apps to the most complex, multi-component applications needing special orchestration needs.

Plan is to make this a unified REST based entry point for other important features like resource-profile management ([YARN-3926](https://issues.apache.org/jira/browse/YARN-4793)), package-definitions' lifecycle-management and service-discovery ([YARN-913](https://issues.apache.org/jira/browse/YARN-913)/[YARN-4757](https://issues.apache.org/jira/browse/YARN-4757)).

### Native Services Discovery
The new discovery solution exposes the registry information through a more generic and widely used mechanism: DNS. Service Discovery via DNS uses the well-known DNS interfaces to browse the network for services. Having the registry information exposed via DNS simplifies the life of services.

The previous read mechanisms of YARN Service Registry were limited to a registry specific (java) API and a REST interface. In practice, this made it very difficult for wiring up existing clients and services. For e.g., dynamic configuration of dependent endpoints of a service was not easy to implement using the registry-read mechanisms, **without** code-changes to existing services. These are solved by the DNS based service discovery.

### Scheduling
[YARN-6592](https://issues.apache.org/jira/browse/YARN-6592) covers a host of scheduling features that are useful for short-running applications and services alike. Below, are a few very important YARN core features that help schedule services better. Without these, running services on YARN is a hassle.

* Affinity (TBD)
* Anti-affinity (TBD)
* Gang scheduling (TBD)
* Malleable container sizes ([YARN-1197](https://issues.apache.org/jira/browse/YARN-1197))

### Resource Profiles
YARN always had support for memory as a resource, inheriting it from Hadoop-(1.x)’s MapReduce platform. Later support for CPU as a resource ([YARN-2](https://issues.apache.org/jira/browse/YARN-2)/[YARN-3](https://issues.apache.org/jira/browse/YARN-3)) was added. Multiple efforts added support for various other resource-types in YARN such as disk ([YARN-2139](https://issues.apache.org/jira/browse/YARN-2139)), and network ([YARN-2140](https://issues.apache.org/jira/browse/YARN-2140)), specifically benefiting long running services.

In many systems outside of YARN, users are already accustomed to specifying their desired ‘box’ of requirements where each box comes with a predefined amount of each resources.  Admins would define various available box-sizes (small, medium, large etc) and users would pick the ones they desire and everybody is happy. In  [YARN-3926](https://issues.apache.org/jira/browse/YARN-3926), YARN introduces Resource Profiles which extends the YARN resource model for easier resource-type management and profiles. This helps in two ways - the system can schedule applications better and it can perform intelligent over-subscription of resources where applicable.

Resource profiles are all the more important for services since -
* Similar to short running apps, you don’t have to fiddle with varying resource-requirements for each container type
* Services usually end up planning for peak usages, leaving a lot of possibility of barren utilization

### Special handling of preemption and container reservations
TBD.

Preemption and reservation of long running containers have different implications from regular ones. Preemption of resources in YARN today works by killing of containers. For long-lived services this is unacceptable. Also, scheduler should avoid allocating long running containers on borrowed resources. [YARN-4724](https://issues.apache.org/jira/browse/YARN-4724) will address some of these special recognition of service containers.

### Container auto-restarts
If a service container dies, expiring container's allocation and releasing the allocation is undesirable in many cases. Long running containers may exit for various reasons, crash and need to restart but forcing them to go through the complete scheduling cycle, resource localization, etc. is both unnecessary and expensive.

Services can enable app-specific policies to prevent NodeManagers to automatically restart containers. [YARN-3998](https://issues.apache.org/jira/browse/YARN-3998) implements a  retry-policy to let NM re-launch a service container when it fails.

### Container allocation re-use for application upgrades
TBD.

Auto-restart of containers will support upgrade of service containers without reclaiming the resources first. During an upgrade, with multitude of other applications running in the system, giving up and getting back resources allocated to the service is hard to manage. Node-Labels help this cause but are not straight-forward to use to address the app-specific use-cases. The umbrella [YARN-4726](https://issues.apache.org/jira/browse/YARN-4726) along with [YARN-5620](https://issues.apache.org/jira/browse/YARN-5620) and [YARN-4470](https://issues.apache.org/jira/browse/YARN-4470) will take care of this.

### Dynamic Configurations
Most production-level services require dynamic configurations to manage and simplify their lifecycle. Container’s resource size, local/work dirs and log-dirs are the most basic information services need. Service's endpoint details (host/port), their inter-component dependencies, health-check endpoints, etc. are all critical to the success of today's real-life services.

### Resource re-localization for reconfiguration/upgrades
TBD

### Service Registry
TBD

### Service persistent storage and volume support
TBD

### Packaging
TBD

### Container image registry (private, public and hybrid)
TBD

### Container image management and APIs
TBD

### Container image storage
TBD

### Monitoring
TBD

### Metrics
TBD

### Service Logs
TBD


