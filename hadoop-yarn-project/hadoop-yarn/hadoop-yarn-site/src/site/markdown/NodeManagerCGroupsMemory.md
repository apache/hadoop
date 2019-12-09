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

Using Memory Control in YARN
=======================

YARN has multiple features to enforce container memory limits. There are three types of controls in YARN that can be used.
1. The polling feature monitors periodically measures container memory usage and kills the containers that exceed their limits. This is a legacy feature with some issues notably a delay that may lead to node shutdown.
2. Strict memory control kills each container that has exceeded its limits. It is using the OOM killer capability of the cgroups Linux kernel feature.
3. Elastic memory control is also based on cgroups. It allows bursting and starts killing containers only, if the overall system memory usage reaches a limit.

If you use 2. or 3. feature 1. is disabled.

Strict Memory Feature
---------------------

cgroups can be used to preempt containers in case of out-of-memory events. This feature leverages cgroups to clean up containers with the kernel when this happens. If your container exited with exit code `137`, then ou can verify the cause in `/var/log/messages`.

Elastic Memory Feature
----------------------

The cgroups kernel feature has the ability to notify the node manager, if the parent cgroup of all containers specified by `yarn.nodemanager.linux-container-executor.cgroups.hierarchy` goes over a memory limit. The YARN feature that uses this ability is called elastic memory control. The benefits are that containers can burst using more memory than they are reserved to. This is allowed as long as we do not exceed the overall memory limit. When the limit is reached the kernel freezes all the containers and notifies the node manager. The node manager chooses a container and preempts it. It continues this step until the node is resumed from the OOM condition.

The Limit for Elastic Memory Control
---------

The limit is the amount of memory allocated to all the containers on the node. The limit is specified by `yarn.nodemanager.resource.memory-mb` and `yarn.nodemanager.vmem-pmem-ratio`. If these are not set, the limit is set based on the available resources. See `yarn.nodemanager.resource.detect-hardware-capabilities` for details.

The pluggable preemption logic
------------------------------

The preemption logic specifies which container to preempt in a node wide out-of-memory situation. The default logic is the `DefaultOOMHandler`. It picks the latest container that exceeded its memory limit. In the unlikely case that no such container is found, it preempts the container that was launched most recently. This continues until the OOM condition is resolved. This logic supports bursting, when containers use more memory than they reserved as long as we have memory available. This helps to improve the overall cluster utilization. The logic ensures that as long as a container is within its limit, it won't get preempted. If the container bursts it can be preempted. There is a case that all containers are within their limits but we are out of memory. This can also happen in case of oversubscription. We prefer preemting the latest containers to minimize the cost and value lost. Once preempted, the data in the container is lost.

The default out-of-memory handler can be updated using `yarn.nodemanager.elastic-memory-control.oom-handler`. The class named in this configuration entry has to implement java.lang.Runnable. The `run()` function will be called in a node level out-of-memory situation. The constructor should accept an `NmContext` object.

Physical and virtual memory control
----------------------------------

In case of Elastic Memory Control, the limit applies to the physical or virtual (rss+swap in cgroups) memory depending on whether `yarn.nodemanager.pmem-check-enabled` or `yarn.nodemanager.vmem-check-enabled` is set.

There is no reason to set them both. If the system runs with swap disabled, both will have the same number. If swap is enabled the virtual memory counter will account for pages in physical memory and on the disk. This is what the application allocated and it has control over. The limit should be applied to the virtual memory in this case. When swapping is enabled, the physical memory is no more than the virtual memory and it is adjusted by the kernel not just by the container. There is no point preempting a container when it exceeds a physical memory limit with swapping. The system will just swap out some memory, when needed.

Virtual memory measurement and swapping
--------------------------------------------

There is a difference between the virtual memory reported by the container monitor and the virtual memory limit specified in the elastic memory control feature. The container monitor uses `ProcfsBasedProcessTree` by default for measurements that returns values from the `proc` file system. The virtual memory returned is the size of the address space of all the processes in each container. This includes anonymous pages, pages swapped out to disk, mapped files and reserved pages among others. Reserved pages are not backed by either physical or swapped memory. They can be a large part of the virtual memory usage. The reservabe address space was limited on 32 bit processors but it is very large on 64-bit ones making this metric less useful. Some Java Virtual Machines reserve large amounts of pages but they do not actually use it. This will result in gigabytes of virtual memory usage shown. However, this does not mean that anything is wrong with the container.

Because of this you can now use `CGroupsResourceCalculator`. This shows only the sum of the physical memory usage and swapped pages as virtual memory usage excluding the reserved address space. This reflects much better what the application and the container allocated.

In order to enable cgroups based resource calculation set `yarn.nodemanager.resource-calculator.class` to `org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsResourceCalculator`.

Configuration quickstart
------------------------

The following levels of memory enforcement are available and supported:

Level | Configuration type | Options
---|---|---
0 | No memory control | All settings below are false
1 | Strict Container Memory enforcement through polling | P or V
2 | Strict Container Memory enforcement through cgroups | CG, C and (P or V)
3 | Elastic Memory Control through cgroups | CG, E and (P or V)

The symbols above mean that the respective configuration entries are `true`:

P: `yarn.nodemanager.pmem-check-enabled`

V: `yarn.nodemanager.vmem-check-enabled`

C: `yarn.nodemanager.resource.memory.enforced`

E: `yarn.nodemanager.elastic-memory-control.enabled`

cgroups prerequisites
---------------------

CG: C and E require the following prerequisites:
1. `yarn.nodemanager.container-executor.class` should be `org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor`.
2. `yarn.nodemanager.runtime.linux.allowed-runtimes` should at least be `default`.
3. `yarn.nodemanager.resource.memory.enabled` should be `true`

Configuring no memory control
-----------------------------

`yarn.nodemanager.pmem-check-enabled` and `yarn.nodemanager.vmem-check-enabled` should be `false`.

`yarn.nodemanager.resource.memory.enforced` should be `false`.

`yarn.nodemanager.elastic-memory-control.enabled` should be `false`.

Configuring strict container memory enforcement with polling without cgroups
----------------------------------------------------------------

`yarn.nodemanager.pmem-check-enabled` or `yarn.nodemanager.vmem-check-enabled` should be `true`.

`yarn.nodemanager.resource.memory.enforced` should be `false`.

`yarn.nodemanager.elastic-memory-control.enabled` should be `false`.

Configuring strict container memory enforcement with cgroups
------------------------------------------------------------

Strict memory control preempts containers right away using the OOM killer feature of the kernel, when they reach their physical or virtual memory limits. You need to set the following options on top of the prerequisites above to use strict memory control.

Configure the cgroups prerequisites mentioned above.

`yarn.nodemanager.pmem-check-enabled` or `yarn.nodemanager.vmem-check-enabled` should be `true`. You can set them both. **Currently this is ignored by the code, only physical limits can be selected.**

`yarn.nodemanager.resource.memory.enforced` should be true

Configuring elastic memory resource control
------------------------------------------

The cgroups based elastic memory control preempts containers only if the overall system memory usage reaches its limit allowing bursting. This feature requires setting the following options on top of the prerequisites.

Configure the cgroups prerequisites mentioned above.

`yarn.nodemanager.elastic-memory-control.enabled` should be `true`.

`yarn.nodemanager.resource.memory.enforced` should be `false`

`yarn.nodemanager.pmem-check-enabled` or `yarn.nodemanager.vmem-check-enabled` should be `true`. If swapping is turned off the former should be set, the latter should be set otherwise.
