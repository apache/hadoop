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

Using CGroups with YARN
=======================

* [CGroups Configuration](#CGroups_configuration)
* [CGroups and Security](#CGroups_and_security)

CGroups is a mechanism for aggregating/partitioning sets of tasks, and all their future children, into hierarchical groups with specialized behaviour. CGroups is a Linux kernel feature and was merged into kernel version 2.6.24. From a YARN perspective, this allows containers to be limited in their resource usage. A good example of this is CPU usage. Without CGroups, it becomes hard to limit container CPU usage. Currently, CGroups is only used for limiting CPU usage.

CGroups Configuration
---------------------

This section describes the configuration variables for using CGroups.

The following settings are related to setting up CGroups. These need to be set in *yarn-site.xml*.

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.container-executor.class` | This should be set to "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor". CGroups is a Linux kernel feature and is exposed via the LinuxContainerExecutor. |
| `yarn.nodemanager.linux-container-executor.resources-handler.class` | This should be set to "org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler". Using the LinuxContainerExecutor doesn't force you to use CGroups. If you wish to use CGroups, the resource-handler-class must be set to CGroupsLCEResourceHandler. |
| `yarn.nodemanager.linux-container-executor.cgroups.hierarchy` | The cgroups hierarchy under which to place YARN proccesses(cannot contain commas). If yarn.nodemanager.linux-container-executor.cgroups.mount is false (that is, if cgroups have been pre-configured), then this cgroups hierarchy must already exist |
| `yarn.nodemanager.linux-container-executor.cgroups.mount` | Whether the LCE should attempt to mount cgroups if not found - can be true or false. |
| `yarn.nodemanager.linux-container-executor.cgroups.mount-path` | Where the LCE should attempt to mount cgroups if not found. Common locations include /sys/fs/cgroup and /cgroup; the default location can vary depending on the Linux distribution in use. This path must exist before the NodeManager is launched. Only used when the LCE resources handler is set to the CgroupsLCEResourcesHandler, and yarn.nodemanager.linux-container-executor.cgroups.mount is true. A point to note here is that the container-executor binary will try to mount the path specified + "/" + the subsystem. In our case, since we are trying to limit CPU the binary tries to mount the path specified + "/cpu" and that's the path it expects to exist. |
| `yarn.nodemanager.linux-container-executor.group` | The Unix group of the NodeManager. It should match the setting in "container-executor.cfg". This configuration is required for validating the secure access of the container-executor binary. |

The following settings are related to limiting resource usage of YARN containers:

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.resource.percentage-physical-cpu-limit` | This setting lets you limit the cpu usage of all YARN containers. It sets a hard upper limit on the cumulative CPU usage of the containers. For example, if set to 60, the combined CPU usage of all YARN containers will not exceed 60%. |
| `yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage` | CGroups allows cpu usage limits to be hard or soft. When this setting is true, containers cannot use more CPU usage than allocated even if spare CPU is available. This ensures that containers can only use CPU that they were allocated. When set to false, containers can use spare CPU if available. It should be noted that irrespective of whether set to true or false, at no time can the combined CPU usage of all containers exceed the value specified in "yarn.nodemanager.resource.percentage-physical-cpu-limit". |

CGroups and security
--------------------

CGroups itself has no requirements related to security. However, the LinuxContainerExecutor does have some requirements. If running in non-secure mode, by default, the LCE runs all jobs as user "nobody". This user can be changed by setting "yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user" to the desired user. However, it can also be configured to run jobs as the user submitting the job. In that case "yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users" should be set to false.

| yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user | yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users | User running jobs |
|:---- |:---- |:---- |
| (default) | (default) | nobody |
| yarn | (default) | yarn |
| yarn | false | (User submitting the job) |


