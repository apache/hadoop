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

MapReduce NextGen aka YARN aka MRv2
===================================

The new architecture introduced in hadoop-0.23, divides the two major functions of the JobTracker: resource management and job life-cycle management into separate components.

The new ResourceManager manages the global assignment of compute resources to applications and the per-application ApplicationMaster manages the application’s scheduling and coordination.

An application is either a single job in the sense of classic MapReduce jobs or a DAG of such jobs.

The ResourceManager and per-machine NodeManager daemon, which manages the user processes on that machine, form the computation fabric.

The per-application ApplicationMaster is, in effect, a framework specific library and is tasked with negotiating resources from the ResourceManager and working with the NodeManager(s) to execute and monitor the tasks.

More details are available in the [Architecture](./YARN.html) document.

Documentation Index
===================

YARN
----

* [YARN Architecture](./YARN.html)

* [Capacity Scheduler](./CapacityScheduler.html)

* [Fair Scheduler](./FairScheduler.html)

* [ResourceManager Restart](./ResourceManagerRestart.htaml)

* [ResourceManager HA](./ResourceManagerHA.html)

* [Web Application Proxy](./WebApplicationProxy.html)

* [YARN Timeline Server](./TimelineServer.html)

* [Writing YARN Applications](./WritingYarnApplications.html)

* [YARN Commands](./YarnCommands.html)

* [Scheduler Load Simulator](#hadoop-slsSchedulerLoadSimulator.html)

* [NodeManager Restart](./NodeManagerRestart.html)

* [DockerContainerExecutor](./DockerContainerExecutor.html)

* [Using CGroups](./NodeManagerCGroups.html)

* [Secure Containers](./SecureContainer.html)

* [Registry](./registry/index.html)

YARN REST APIs
--------------

* [Introduction](./WebServicesIntro.html)

* [Resource Manager](./ResourceManagerRest.html)

* [Node Manager](./NodeManagerRest.html)


