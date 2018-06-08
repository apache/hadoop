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

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

CGroups is a mechanism for aggregating/partitioning sets of tasks, and all their future children, into hierarchical groups with specialized behaviour. CGroups is a Linux kernel feature and was merged into kernel version 2.6.24. From a YARN perspective, this allows containers to be limited in their resource usage. A good example of this is CPU usage. Without CGroups, it becomes hard to limit container CPU usage.

CGroups Configuration
---------------------

This section describes the configuration variables for using CGroups.

The following settings are related to setting up CGroups. These need to be set in *yarn-site.xml*.

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.container-executor.class` | This should be set to "org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor". CGroups is a Linux kernel feature and is exposed via the LinuxContainerExecutor. |
| `yarn.nodemanager.linux-container-executor.resources-handler.class` | This should be set to "org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler". Using the LinuxContainerExecutor doesn't force you to use CGroups. If you wish to use CGroups, the resource-handler-class must be set to CGroupsLCEResourceHandler. |
| `yarn.nodemanager.linux-container-executor.cgroups.hierarchy` | The cgroups hierarchy under which to place YARN proccesses(cannot contain commas). If yarn.nodemanager.linux-container-executor.cgroups.mount is false (that is, if cgroups have been pre-configured) and the YARN user has write access to the parent directory, then the directory will be created. If the directory already exists, the administrator has to give YARN write permissions to it recursively. |
| `yarn.nodemanager.linux-container-executor.cgroups.mount` | Whether the LCE should attempt to mount cgroups if not found - can be true or false. |
| `yarn.nodemanager.linux-container-executor.cgroups.mount-path` | Optional. Where CGroups are located. LCE will try to mount them here, if `yarn.nodemanager.linux-container-executor.cgroups.mount` is true. LCE will try to use CGroups from this location, if `yarn.nodemanager.linux-container-executor.cgroups.mount` is false. If specified, this path and its subdirectories (CGroup hierarchies) must exist and they should be readable and writable by YARN before the NodeManager is launched. See CGroups mount options below for details. |
| `yarn.nodemanager.linux-container-executor.group` | The Unix group of the NodeManager. It should match the setting in "container-executor.cfg". This configuration is required for validating the secure access of the container-executor binary. |

The following settings are related to limiting resource usage of YARN containers:

|Configuration Name | Description |
|:---- |:---- |
| `yarn.nodemanager.resource.percentage-physical-cpu-limit` | This setting lets you limit the cpu usage of all YARN containers. It sets a hard upper limit on the cumulative CPU usage of the containers. For example, if set to 60, the combined CPU usage of all YARN containers will not exceed 60%. |
| `yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage` | CGroups allows cpu usage limits to be hard or soft. When this setting is true, containers cannot use more CPU usage than allocated even if spare CPU is available. This ensures that containers can only use CPU that they were allocated. When set to false, containers can use spare CPU if available. It should be noted that irrespective of whether set to true or false, at no time can the combined CPU usage of all containers exceed the value specified in "yarn.nodemanager.resource.percentage-physical-cpu-limit". |

CGroups mount options
---------------------

YARN uses CGroups through a directory structure mounted into the file system by the kernel. There are three options to attach to CGroups.

| Option | Description |
|:---- |:---- |
| Discover CGroups mounted already | This should be used on newer systems like RHEL7 or Ubuntu16 or if the administrator mounts CGroups before YARN starts. Set `yarn.nodemanager.linux-container-executor.cgroups.mount` to false and leave other settings set to their defaults. YARN will locate the mount points in `/proc/mounts`. Common locations include `/sys/fs/cgroup` and `/cgroup`. The default location can vary depending on the Linux distribution in use.|
| CGroups mounted by YARN | IMPORTANT: This option is deprecated due to security reasons with the `container-executor.cfg` option `feature.mount-cgroup.enabled=0` by default. Please mount cgroups before launching YARN.|
| CGroups mounted already or linked but not in `/proc/mounts` | If cgroups is accessible through lxcfs or simulated by another filesystem, then point `yarn.nodemanager.linux-container-executor.cgroups.mount-path` to your CGroups root directory. Set `yarn.nodemanager.linux-container-executor.cgroups.mount` to false. YARN tries to use this path first, before any CGroup mount point discovery. The path should have a subdirectory for each CGroup hierarchy named by the comma separated CGroup subsystems supported like `<path>/cpu,cpuacct`. Valid subsystem names are `cpu, cpuacct, cpuset, memory, net_cls, blkio, freezer, devices`.|

CGroups and security
--------------------

CGroups itself has no requirements related to security. However, the LinuxContainerExecutor does have some requirements. If running in non-secure mode, by default, the LCE runs all jobs as user "nobody". This user can be changed by setting "yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user" to the desired user. However, it can also be configured to run jobs as the user submitting the job. In that case "yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users" should be set to false.

| yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user | yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users | User running jobs |
|:---- |:---- |:---- |
| (default) | (default) | nobody |
| yarn | (default) | yarn |
| yarn | false | (User submitting the job) |


